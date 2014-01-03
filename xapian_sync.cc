#include <cstring>
#include <functional>
#include <iostream>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

const char xapian_schema[] =
R"(CREATE TABLE IF NOT EXISTS tags (
  tag TEXT NOT NULL,
  docid INTEGER NOT NULL,
  UNIQUE (docid, tag),
  UNIQUE (tag, docid));
CREATE TABLE IF NOT EXISTS message_ids (
  message_id TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS xapian_files (
  file_id INTEGER PRIMARY KEY,
  dir_docid INTEGER,
  name TEXT NOT NULL,
  docid INTEGER,
  UNIQUE (dir_docid, name));)";

const char xapian_triggers[] =
R"(CREATE TABLE IF NOT EXISTS modified_docids (
  docid INTEGER PRIMARY KEY);
CREATE TEMP TRIGGER tag_delete AFTER DELETE ON main.tags
  WHEN old.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (old.docid); END;
CREATE TEMP TRIGGER tag_insert AFTER INSERT ON main.tags
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;
CREATE TEMP TRIGGER message_id_insert AFTER INSERT ON main.message_ids
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;

CREATE TABLE IF NOT EXISTS deleted_docids (
  docid INTEGER PRIMARY KEY,
  message_id TEXT UNIQUE);
CREATE TEMP TRIGGER message_id_delete AFTER DELETE ON main.message_ids
  BEGIN INSERT INTO deleted_docids (docid, message_id)
        VALUES (old.docid, old.message_id);
  END;

CREATE TABLE IF NOT EXISTS deleted_dirs (
  docid INTEGER PRIMARY KEY,
  path TEXT UNIQUE); 

CREATE TABLE IF NOT EXISTS new_files (
  file_id INTEGER PRIMARY KEY);
CREATE TEMP TRIGGER file_insert AFTER INSERT ON main.xapian_files
  BEGIN INSERT INTO new_files (file_id) VALUES (new.file_id); END;

CREATE TABLE IF NOT EXISTS deleted_files (
  dir_docid INTEGER,
  name TEXT NOT NULL,
  docid INTEGER,
  UNIQUE (dir_docid, name));
CREATE TEMP TRIGGER file_delete AFTER DELETE ON main.xapian_files
  BEGIN INSERT INTO deleted_files (name, dir_docid, docid)
        VALUES (old.name, old.dir_docid, old.docid); END;
)";

const char xapian_dirs_schema[] =
  R"(CREATE TABLE IF NOT EXISTS xapian_dirs (
  path TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY,
  mctime REAL);)";

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
     [&] (sqlstmt_t *sp, Xapian::ValueIterator *vip) {
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

static void
xapian_scan_directories (sqlite3 *sqldb, Xapian::Database xdb, int rootfd)
{
  save_old_table (sqldb, "xapian_dirs", xapian_dirs_schema);
  sqlstmt_t insert
    (sqldb, "INSERT INTO xapian_dirs (path, docid, mctime) VALUES (?, ?,?);");

  for (Xapian::TermIterator
         ti = xdb.allterms_begin(notmuch_directory_prefix),
         te = xdb.allterms_end(notmuch_directory_prefix);
       ti != te; ti++) {
    string dir = (*ti).substr(notmuch_directory_prefix.length());
    if (dir.length() >= 4) {
      string end (dir.substr (dir.length() - 4));
      if (end != "/cur" && end != "/new")
        continue;
    }
    else if (dir != "cur" && dir != "new")
      continue;
    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);
    if (pi == pe)
      cerr << "warning: directory term " << *ti << " has no postings\n";
    else {
      insert.reset();
      struct stat sb;
      if (fstatat (rootfd, dir.c_str(), &sb, 0)) {
	cerr << dir << ": " << strerror (errno) << '\n';
	insert.bind_null(3);
      }
      else
	insert.bind_real(3, max (ts_to_double(sb.st_mtim),
				 ts_to_double(sb.st_ctim)));
      insert.param(dir, i64(*pi)).step();
      if (++pi != pe)
        cerr << "warning: directory term " << *ti << " has multiple postings\n";
    }
  }

  fmtexec (sqldb, "INSERT INTO deleted_dirs (docid, path)"
	   " SELECT docid, path from old_xapian_dirs"
	   " EXCEPT SELECT docid, path from xapian_dirs;");
  fmtexec (sqldb, "DELETE FROM xapian_files"
	   " WHERE dir_docid IN (SELECT docid FROM deleted_dirs);");
}

static void
xapian_scan_filenames (sqlite3 *sqldb, Xapian::Database xdb)
{
  sqlstmt_t
    dirscan (sqldb, "\
SELECT path, docid FROM xapian_dirs n LEFT OUTER JOIN old_xapian_dirs o\
 USING (path, docid) WHERE ifnull (n.mctime != o.mctime, 1);"),
    filescan (sqldb, "SELECT file_id, name, docid FROM xapian_files"
	      " WHERE dir_docid = ? ORDER BY name;"),
    add_file (sqldb, "INSERT INTO xapian_files (name, docid, dir_docid)"
	      " VALUES (?, ?, ?);"),
    del_file (sqldb, "DELETE FROM xapian_files WHERE file_id = ?;");

  while (dirscan.step().row()) {
    string dir = dirscan.str(0);
    cout << "  " << dir << '\n';
    i64 dir_docid = dirscan.integer(1);
    string dirtermprefix = (notmuch_file_direntry_prefix
			    + to_string (dir_docid) + ":");
    filescan.reset().bind_int(1, dir_docid);
    add_file.reset().bind_int(3, dir_docid);

    Xapian::TermIterator
      ti = xdb.allterms_begin(dirtermprefix),
      te = xdb.allterms_end(dirtermprefix);

    sync_table<Xapian::TermIterator>
      (filescan, ti, te,
       [&dirtermprefix] (sqlstmt_t &s, Xapian::TermIterator &ti) -> int {
	return s.str(1).compare((*ti).substr(dirtermprefix.length()));
       }, [&] (sqlstmt_t *sp, Xapian::TermIterator *tip) {
	if (!tip)
	  del_file.reset().param(sp->value(0)).step();
	else {
	  string dirent { (*ti).substr(dirtermprefix.length()) };
	  Xapian::PostingIterator pi = xdb.postlist_begin (**tip),
	    pe = xdb.postlist_end (**tip);
	  if (pi == pe)
	    cerr << "warning: file direntry term " << **tip
		 << " has no postings\n";
	  else {
	    if (!sp)
	      add_file.reset().bind_text(1, dirent).bind_int(2, *pi).step(); 
	    else if (sp->integer(2) != *pi) {
	      // This should be really unusual
	      cerr << "warning: docid changed for "
		   << dir << '/' << dirent << '\n';
	      del_file.reset().param(sp->value(0)).step();
	      add_file.reset().bind_text(1, dirent).bind_int(2, *pi).step(); 
	    }
	    if (++pi != pe)
	      cerr << "warning: file direntry term " << *ti
		   << " has multiple postings\n";
	  }
	}
      });
  }
}

void
xapian_scan (sqlite3 *sqldb, writestamp ws, const string &path)
{
  int rootfd = open (path.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (path + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  Xapian::Database xdb (path + "/.notmuch/xapian");

  print_time ("configuring database");
  fmtexec (sqldb, xapian_schema);
  fmtexec (sqldb, xapian_triggers);
  fmtexec (sqldb, "DELETE FROM modified_docids; "
	   "DELETE FROM deleted_docids; "
	   "DELETE FROM deleted_dirs; "
	   "DELETE FROM new_files; "
	   "DELETE FROM deleted_files; ");

  print_time ("scanning message IDs");
  xapian_scan_message_ids (sqldb, xdb);
  print_time ("scanning tags");
  xapian_scan_tags (sqldb, xdb);
  print_time ("scanning directories");
  xapian_scan_directories (sqldb, xdb, rootfd);
  print_time ("scanning filenames in modified directories");
  xapian_scan_filenames (sqldb, xdb);
  print_time ("done tags");
}
