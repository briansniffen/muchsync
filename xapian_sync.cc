#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

const char xapian_triggers[] =
R"(CREATE TABLE IF NOT EXISTS modified_docids (
  docid INTEGER PRIMARY KEY);
DELETE FROM modified_docids;
CREATE TEMP TRIGGER tag_delete AFTER DELETE ON main.tags
  WHEN old.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (old.docid); END;
CREATE TEMP TRIGGER tag_insert AFTER INSERT ON main.tags
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;
CREATE TEMP TRIGGER message_id_insert AFTER INSERT ON main.message_ids
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;

CREATE TEMP TABLE deleted_xapian_dirs (
  dir_docid INTEGER PRIMARY KEY,
  dir_path TEXT UNIQUE); 
)";

template<typename T> void
sync_table (sqlstmt_t &s, T &t, T &te,
	    function<int(sqlstmt_t &s, T &t)> cmpfn,
	    function<void(sqlstmt_t *s, T *t)> update)
{
  s.step();
  while (s.row()) {
    int cmp {t == te ? -1 : cmpfn (s, t)};
    if (cmp == 0) {
      update (&s, &t);
      s.step();
      ++t;
    }
    else if (cmp < 0) {
      update (&s, nullptr);
      s.step();
    }
    else {
      update (nullptr, &t);
      ++t;
    }
  }
  while (t != te) {
    update (NULL, &t);
    ++t;
  }
}

string
percent_encode (const string &raw)
{
  ostringstream outbuf;
  outbuf.fill('0');
  outbuf.setf(ios::hex, ios::basefield);

  for (char c : raw) {
    if (isalnum (c) || (c >= '+' && c <= '.')
	|| c == '_' || c == '@' || c == '=')
      outbuf << c;
    else
      outbuf << '%' << setw(2) << int (uint8_t (c));
  }
  return outbuf.str ();
}

inline int
hexdigit (char c)
{
  if (c >= '0' && c <= '9')
    return c - '0';
  else if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  else
    throw runtime_error ("precent_decode: illegal hexdigit " + string (1, c));
}

string
percent_decode (const string &encoded)
{
  ostringstream outbuf;
  int escape_pos = 0, escape_val = 0;
  for (char c : encoded) {
    switch (escape_pos) {
    case 0:
      if (c == '%')
	escape_pos = 1;
      else
	outbuf << c;
      break;
    case 1:
      escape_val = hexdigit(c) << 4;
      escape_pos = 2;
      break;
    case 2:
      escape_pos = 0;
      outbuf << char (escape_val | hexdigit(c));
      break;
    }
  }
  if (escape_pos)
    throw runtime_error ("percent_decode: incomplete escape");
  return outbuf.str();
}

string
tag_from_term (const string &term)
{
  assert (!strncmp (term.c_str(), notmuch_tag_prefix.c_str(),
		    notmuch_tag_prefix.length()));
  return percent_encode (term.substr (notmuch_tag_prefix.length()));
}

string
term_from_tag (const string &tag)
{
  return notmuch_tag_prefix + percent_decode (tag);
}

static void
xapian_scan_tags (sqlite3 *sqldb, Xapian::Database &xdb)
{
  sqlstmt_t
    scan (sqldb, "SELECT docid FROM tags WHERE tag = ? ORDER BY docid ASC;"),
    add_tag (sqldb, "INSERT INTO tags (docid, tag) VALUES (?, ?);"),
    del_tag (sqldb, "DELETE FROM tags WHERE docid = ? & tag = ?;");

  for (Xapian::TermIterator ti = xdb.allterms_begin(notmuch_tag_prefix),
	 te = xdb.allterms_end(notmuch_tag_prefix); ti != te; ti++) {
    string tag = tag_from_term (*ti);
    if (opt_verbose > 1)
      cout << "  " << tag << "\n";
    scan.reset().bind_text(1, tag);
    add_tag.reset().bind_text(2, tag);
    del_tag.reset().bind_text(2, tag);

    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);

    sync_table<Xapian::PostingIterator>
      (scan, pi, pe,
       [] (sqlstmt_t &s, Xapian::PostingIterator &p) -> int {
	 return s.integer(0) - *p;
       },
       [&] (sqlstmt_t *sp, Xapian::PostingIterator *pp) {
	 if (!sp)
	   add_tag.reset().bind_int(1, **pp).step();
	 else if (!pp)
	   del_tag.reset().bind_value(1, sp->value(0)).step();
       });
  }
}

static void
xapian_scan_message_ids (sqlite3 *sqldb, Xapian::Database xdb)
{
  sqlstmt_t
    scan (sqldb, "\
SELECT message_id, docid FROM message_ids ORDER BY docid ASC;"),
    add_message (sqldb, "\
INSERT INTO message_ids (message_id, docid) VALUES (?, ?);"),
    del_message (sqldb, "DELETE FROM message_ids WHERE docid = ?;");

  Xapian::ValueIterator
    vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
    ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);

  sync_table<Xapian::ValueIterator>
    (scan, vi, ve,
     [] (sqlstmt_t &s, Xapian::ValueIterator &vi) -> int {
       return s.integer(1) - vi.get_docid();
     },
     [&add_message,&del_message] (sqlstmt_t *sp, Xapian::ValueIterator *vip) {
       if (!sp)
	 add_message.reset().param(**vip, i64(vip->get_docid())).step();
       else if (!vip)
	 del_message.reset().param(sp->value(1)).step();
       else if (sp->str(0) != **vip) {
	 // This should be really unusual
	 cerr << "warning: message id changed from <"
	      << sp->str(0) << "> to <" << **vip << ">\n";
	 del_message.reset().param(sp->value(1)).step();
	 add_message.reset().param(**vip, i64(vip->get_docid())).step();
       }
     });
}

static Xapian::docid
xapian_get_unique_posting (const Xapian::Database &xdb, const string &term)
{
  Xapian::PostingIterator pi = xdb.postlist_begin (term),
    pe = xdb.postlist_end (term);
  if (pi == pe)
    throw range_error (string() + "xapian term " + term + " has no postings");
  i64 ret = *pi;
  if (++pi != pe)
    cerr << "warning: xapian term " << term << " has multiple postings\n";
  return ret;
}

static void
xapian_scan_directories (sqlite3 *sqldb, Xapian::Database xdb)
{
  sqlexec (sqldb, "DROP TABLE IF EXISTS old_xapian_dirs; "
	   "ALTER TABLE xapian_dirs RENAME TO old_xapian_dirs; "
	   "%s", xapian_dirs_def);
  sqlstmt_t insert (sqldb,
      "INSERT INTO xapian_dirs (dir_path, dir_docid) VALUES (?, ?);");

  for (Xapian::TermIterator
         ti = xdb.allterms_begin(notmuch_directory_prefix),
         te = xdb.allterms_end(notmuch_directory_prefix);
       ti != te; ti++) {
    string dir = (*ti).substr(notmuch_directory_prefix.length());
    if (!dir_contains_messages(dir))
      continue;
    insert.reset().param(dir, i64(xapian_get_unique_posting(xdb, *ti))).step();
  }

  sqlexec (sqldb, "INSERT INTO deleted_xapian_dirs (dir_docid, dir_path)"
	   " SELECT dir_docid, dir_path from old_xapian_dirs"
	   " EXCEPT SELECT dir_docid, dir_path from xapian_dirs;");
  sqlexec (sqldb, "DELETE FROM xapian_files"
	   " WHERE dir_docid IN (SELECT dir_docid FROM deleted_xapian_dirs);");
}

static void
xapian_scan_filenames (sqlite3 *sqldb, Xapian::Database xdb)
{
  sqlstmt_t
    dirscan (sqldb, "SELECT dir_path, dir_docid FROM xapian_dirs;"),
    filescan (sqldb, "SELECT file_id, name, docid"
	      " FROM xapian_files WHERE dir_docid = ? ORDER BY name;"),
    add_file (sqldb, "INSERT INTO xapian_files"
	      " (name, docid, dir_docid)"
	      " VALUES (?, ?, ?);"),
    del_file (sqldb, "DELETE FROM xapian_files WHERE file_id = ?;");

  while (dirscan.step().row()) {
    string dir = dirscan.str(0);

    i64 dir_docid = dirscan.integer(1);
    string dirtermprefix = (notmuch_file_direntry_prefix
			    + to_string (dir_docid) + ":");
    filescan.reset().bind_int(1, dir_docid);
    add_file.reset().bind_int(3, dir_docid);

    auto cmp = [&dirtermprefix]
      (sqlstmt_t &s, Xapian::TermIterator &ti) -> int {
      return s.str(1).compare((*ti).substr(dirtermprefix.length()));
    };
    auto merge = [&dirtermprefix,&add_file,&del_file,&xdb]
      (sqlstmt_t *sp, Xapian::TermIterator *tip) {
      if (!tip) {
	del_file.reset().param(sp->value(0)).step();
	return;
      }

      if (sp && !opt_fullscan)
	return;

      i64 docid = xapian_get_unique_posting (xdb, **tip);
      if (sp) {
	if (sp->integer(2) == docid)
	  return;
	del_file.reset().param(sp->value(0)).step();
      }
      string dirent { (**tip).substr(dirtermprefix.length()) };
      add_file.reset().param(dirent, docid).step();
    };

    Xapian::TermIterator ti = xdb.allterms_begin(dirtermprefix),
      te = xdb.allterms_end(dirtermprefix);
    sync_table<Xapian::TermIterator> (filescan, ti, te, cmp, merge);
  }
}

void
xapian_scan (sqlite3 *sqldb, writestamp ws, const string &path)
{
  print_time ("opening Xapian");
  Xapian::Database xdb (path + "/.notmuch/xapian");
  sqlexec (sqldb, xapian_triggers);
  print_time ("scanning message IDs");
  xapian_scan_message_ids (sqldb, xdb);
  print_time ("scanning tags");
  xapian_scan_tags (sqldb, xdb);
  print_time ("updating version stamps");
  sqlexec (sqldb, "UPDATE message_ids SET replica = %lld, version = %lld"
	   " WHERE docid IN (SELECT docid FROM modified_docids);",
	   ws.first, ws.second);
  print_time ("scanning directories in xapian");
  xapian_scan_directories (sqldb, xdb);
  print_time ("scanning filenames in xapian");
  xapian_scan_filenames (sqldb, xdb);
  print_time ("closing Xapian");
}
