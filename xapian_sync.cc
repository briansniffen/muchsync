#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/sha.h>
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
  dir_docid INTEGER PRIMARY KEY,
  path TEXT UNIQUE); 

CREATE TABLE IF NOT EXISTS new_files (
  file_id INTEGER PRIMARY KEY);
CREATE TEMP TRIGGER file_insert AFTER INSERT ON main.xapian_files
  BEGIN INSERT INTO new_files (file_id) VALUES (new.file_id); END;

CREATE TABLE IF NOT EXISTS deleted_files (
  file_id INTEGER PRIMARY KEY,
  dir_docid INTEGER,
  name TEXT NOT NULL,
  docid INTEGER,
  UNIQUE (dir_docid, name));
CREATE TEMP TRIGGER file_delete AFTER DELETE ON main.xapian_files
  BEGIN INSERT INTO deleted_files (file_id, name, dir_docid, docid)
        VALUES (old.file_id, old.name, old.dir_docid, old.docid); END;
)";

const char xapian_dirs_schema[] =
  R"(CREATE TABLE IF NOT EXISTS xapian_dirs (
  path TEXT UNIQUE NOT NULL,
  dir_docid INTEGER PRIMARY KEY,
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

string
tag_from_term (const string &term)
{
  assert (!strncmp (term.c_str(), notmuch_tag_prefix.c_str(),
		    notmuch_tag_prefix.length()));

  ostringstream tagbuf;
  tagbuf.fill('0');
  tagbuf.setf(ios::hex, ios::basefield);

  char c;
  for (const char *p = term.c_str() + 1; (c = *p); p++)
    if (isalnum (c) || (c >= '+' && c <= '.')
	|| c == '_' || c == '@' || c == '=')
      tagbuf << c;
    else
      tagbuf << '%' << setw(2) << int (uint8_t (c));

  return tagbuf.str ();
}

inline int
hexdigit (char c)
{
  if (c >= '0' && c <= '9')
    return c - '0';
  else if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  else
    throw runtime_error ("illegal hexdigit " + string (1, c));
}

string
term_from_tag (const string &tag)
{
  ostringstream tagbuf;
  tagbuf << notmuch_tag_prefix;
  int escape_pos = 0, escape_val;
  char c;
  for (const char *p = tag.c_str() + 1; (c = *p); p++) {
    switch (escape_pos) {
    case 0:
      if (c == '%')
	escape_pos = 1;
      else
	tagbuf << c;
      break;
    case 1:
      escape_val = hexdigit(c) << 4;
      escape_pos = 2;
      break;
    case 2:
      escape_pos = 0;
      tagbuf << char (escape_val | hexdigit(c));
      break;
    }
  }
  if (escape_pos)
    throw runtime_error ("term_from_tag: incomplete escape");
  return tagbuf.str();
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
xapian_scan_directories (sqlite3 *sqldb, Xapian::Database xdb, int rootfd)
{
  save_old_table (sqldb, "xapian_dirs", xapian_dirs_schema);
  sqlstmt_t insert (sqldb,
      "INSERT INTO xapian_dirs (path, dir_docid, mctime) VALUES (?, ?,?);");

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
    insert.reset()
      .bind_text(1, dir)
      .bind_int(2, xapian_get_unique_posting(xdb, *ti));
    struct stat sb;
    if (fstatat (rootfd, dir.c_str(), &sb, 0)) {
      cerr << dir << ": " << strerror (errno) << '\n';
      insert.bind_null(3);
    }
    else {
      insert.bind_real(3, max (ts_to_double(sb.st_mtim),
			       ts_to_double(sb.st_ctim)));
    }
    insert.step();
  }

  fmtexec (sqldb, "INSERT INTO deleted_dirs (dir_docid, path)"
	   " SELECT dir_docid, path from old_xapian_dirs"
	   " EXCEPT SELECT dir_docid, path from xapian_dirs;");
  fmtexec (sqldb, "DELETE FROM xapian_files"
	   " WHERE dir_docid IN (SELECT dir_docid FROM deleted_dirs); "
	   "UPDATE deleted_files SET dir_docid = NULL"
	   " WHERE dir_docid IN (SELECT dir_docid FROM deleted_dirs);");
}

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setw (2) << setfill ('0');
  for (auto c : s)
    os << (int (c) & 0xff);
  return os.str ();
}

static string
get_sha (int dfd, const char *direntry, struct stat *sbp)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);
  if (sbp && fstat (fd, sbp))
    throw runtime_error (string() + direntry + ": " + strerror (errno));

  SHA_CTX ctx;
  SHA1_Init (&ctx);

  char buf[16384];
  int n;
  while ((n = read (fd, buf, sizeof (buf))) > 0)
    SHA1_Update (&ctx, buf, n);
  if (n < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  unsigned char resbuf[SHA_DIGEST_LENGTH];
  SHA1_Final (resbuf, &ctx);
  return hexdump ({ reinterpret_cast<const char *> (resbuf), sizeof (resbuf) });
}

static void
xapian_scan_filenames (sqlite3 *sqldb, Xapian::Database xdb, int rootfd)
{
  sqlstmt_t
    dirscan (sqldb, "SELECT path, dir_docid FROM xapian_dirs;"),
    filescan (sqldb, "SELECT file_id, name, docid, mtime, size, hash"
	      " FROM xapian_files WHERE dir_docid = ? ORDER BY name;"),
    add_file (sqldb, "INSERT INTO xapian_files"
	      " (name, docid, mtime, size, inode, hash, dir_docid)"
	      " VALUES (?, ?, ?, ?, ?, ?, ?);"),
    del_file (sqldb, "DELETE FROM xapian_files WHERE file_id = ?;"),
    upd_file (sqldb, "UPDATE xapian_files SET mtime = ?, inode = ?"
		 " WHERE file_id = ?;");

  while (dirscan.step().row()) {
    string dir = dirscan.str(0);

    int dfd = openat (rootfd, dir.c_str(), O_RDONLY);
    if (dfd < 0) {
      perror (dir.c_str());
      continue;
    }
    cleanup _c (close, dfd);

    cout << "  " << dir << '\n';
    i64 dir_docid = dirscan.integer(1);
    string dirtermprefix = (notmuch_file_direntry_prefix
			    + to_string (dir_docid) + ":");
    filescan.reset().bind_int(1, dir_docid);
    add_file.reset().bind_int(7, dir_docid);

    auto cmp = [&dirtermprefix]
      (sqlstmt_t &s, Xapian::TermIterator &ti) -> int {
      return s.str(1).compare((*ti).substr(dirtermprefix.length()));
    };
    auto merge = [&dirtermprefix,&add_file,&del_file,&xdb,dfd,&dir,&upd_file]
      (sqlstmt_t *sp, Xapian::TermIterator *tip) {
      if (!tip) {
	del_file.reset().param(sp->value(0)).step();
	return;
      }

      if (sp && !opt_fullscan)
	return;

      i64 docid = xapian_get_unique_posting (xdb, **tip);
      if (sp && sp->integer(2) != docid) {
	del_file.reset().param(sp->value(0)).step();
	sp = nullptr;
      }

      string dirent { (**tip).substr(dirtermprefix.length()) };
      struct stat sb;
      string hashval;
      if (sp) {
	if (fstatat (dfd, dirent.c_str(), &sb, 0)) {
	  perror ((dir + dirent).c_str());
	  return;
	}
	if (ts_to_double(sb.st_mtim) == sp->real(3)
	    && sb.st_size == sp->integer(4))
	  return;
	string hashval = get_sha (dfd, dirent.c_str(), &sb);
	if (hashval == sp->str(5)) {
	  upd_file.reset().param(ts_to_double(sb.st_mtim), i64(sb.st_ino),
				 sp->value(0)).step ();
	  return;
	}
	del_file.reset().param(sp->value(0)).step();
	sp = nullptr;
      }
      if (!hashval.size())
	hashval = get_sha (dfd, dirent.c_str(), &sb);
      add_file.reset();
      add_file.param(dirent, docid, ts_to_double(sb.st_mtim),
		     i64(sb.st_size), i64(sb.st_ino), hashval);
      add_file.step();
    };

    Xapian::TermIterator ti = xdb.allterms_begin(dirtermprefix),
      te = xdb.allterms_end(dirtermprefix);
    sync_table<Xapian::TermIterator> (filescan, ti, te, cmp, merge);
  }
  fmtexec (sqldb,
	   "UPDATE deleted_files SET docid = NULL"
	   " WHERE docid IN (SELECT docid FROM deleted_docids);");
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
  xapian_scan_filenames (sqldb, xdb, rootfd);
  print_time ("done tags");
}
