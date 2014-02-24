#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

static void
drop_triggers(sqlite3 *db)
{
  for (const char *trigger
	 : { "tag_delete", "tag_insert", "link_delete", "link_insert" })
    sqlexec (db, "DROP TRIGGER IF EXISTS %s;", trigger);
  for (const char *table
	 : { "modified_docids", "deleted_xapian_dirs", "modified_hashes" })
    sqlexec(db, "DROP TABLE IF EXISTS %s;", table);
}

static void
set_triggers(sqlite3 *db)
{
  drop_triggers (db);
  sqlexec(db, R"(
CREATE TEMP TABLE IF NOT EXISTS modified_docids (
  docid INTEGER PRIMARY KEY,
  new INTEGER);
DELETE FROM modified_docids;
CREATE TEMP TRIGGER tag_delete AFTER DELETE ON main.tags
  WHEN old.docid NOT IN (SELECT docid FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid, new) VALUES (old.docid, 0); END;
CREATE TEMP TRIGGER tag_insert AFTER INSERT ON main.tags
  WHEN new.docid NOT IN (SELECT docid FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid, new) VALUES (new.docid, 0); END;

CREATE TEMP TABLE IF NOT EXISTS deleted_xapian_dirs (
  dir_docid INTEGER PRIMARY KEY,
  dir_path TEXT UNIQUE); 
DELETE FROM deleted_xapian_dirs;

CREATE TEMP TABLE IF NOT EXISTS modified_hashes (hash_id INTEGER PRIMARY KEY);
DELETE FROM modified_hashes;
CREATE TEMP TRIGGER link_delete AFTER DELETE ON xapian_files
  WHEN old.hash_id NOT IN (SELECT hash_id FROM modified_hashes)
  BEGIN INSERT INTO modified_hashes (hash_id) VALUES (old.hash_id); END;
CREATE TEMP TRIGGER link_insert AFTER INSERT ON xapian_files
  WHEN new.hash_id NOT IN (SELECT hash_id FROM modified_hashes)
  BEGIN INSERT INTO modified_hashes (hash_id) VALUES (new.hash_id); END;
)");
}

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
xapian_scan_tags (sqlite3 *sqldb, Xapian::Database &xdb, const writestamp &ws)
{
  sqlstmt_t
    scan (sqldb, "SELECT docid FROM tags WHERE tag = ? ORDER BY docid ASC;"),
    add_tag (sqldb, "INSERT INTO tags (docid, tag) VALUES (?, ?);"),
    del_tag (sqldb, "DELETE FROM tags WHERE (docid = ?) & (tag = ?);");

  for (Xapian::TermIterator ti = xdb.allterms_begin(notmuch_tag_prefix),
	 te = xdb.allterms_end(notmuch_tag_prefix); ti != te; ti++) {
    string tag = tag_from_term (*ti);
    if (opt_verbose > 1)
      cerr << "  " << tag << "\n";
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

  sqlexec(sqldb, "UPDATE message_ids SET replica = %lld, version = %lld"
	  " WHERE docid IN (SELECT docid FROM modified_docids WHERE new = 0);",
	  ws.first, ws.second);
}

static void
xapian_scan_message_ids (sqlite3 *sqldb, const writestamp &ws,
			 Xapian::Database xdb)
{
  sqlstmt_t
    scan(sqldb,
	  "SELECT message_id, docid FROM message_ids ORDER BY docid ASC;"),
    add_message(sqldb,
		"INSERT INTO message_ids (message_id, docid, replica, version)"
		" VALUES (?, ?, %lld, %lld);", ws.first, ws.second),
    flag_new_message(sqldb, "INSERT INTO modified_docids (docid, new)"
		     " VALUES (?, 1);"),
    del_message(sqldb, "DELETE FROM message_ids WHERE docid = ?;");

  Xapian::ValueIterator
    vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
    ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);

  sync_table<Xapian::ValueIterator>
    (scan, vi, ve,
     [] (sqlstmt_t &s, Xapian::ValueIterator &vi) -> int {
       return s.integer(1) - vi.get_docid();
     },
     [&add_message,&del_message,&flag_new_message]
     (sqlstmt_t *sp, Xapian::ValueIterator *vip) {
       if (!sp) {
	 i64 docid = vip->get_docid();
	 add_message.reset().param(**vip, docid).step();
	 flag_new_message.reset().param(docid).step();
       }
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

class fileops {
public:
  sqlstmt_t scan_dir_;
private:
  sqlstmt_t get_msgid_;
  sqlstmt_t del_file_;
  sqlstmt_t add_file_;
  sqlstmt_t upd_file_;
  sqlstmt_t get_hashid_;
  sqlstmt_t get_hash_;
  sqlstmt_t add_hash_;
  sqlstmt_t upd_hash_;
  string get_msgid(i64 docid);
  i64 get_file_hash_id(int dfd, const string &file, i64 docid);
public:
  fileops(sqlite3 *db, const writestamp &ws);
  void del_file(i64 rowid) { del_file_.reset().param(rowid).step(); }
  void add_file(const string &dir, int dfd, i64 dir_docid,
		string name, i64 docid);
  void check_file(const string &dir, int dfd, i64 dir_docid);
};

fileops::fileops(sqlite3 *db, const writestamp &ws)
  : scan_dir_(db, "SELECT rowid, name, docid%s"
	      " FROM xapian_files WHERE dir_docid = ? ORDER BY name;",
	      opt_fullscan ? ", mtime, inode, hash_id" : ""),
    get_msgid_(db, "SELECT message_id FROM message_ids WHERE docid = ?;"),
    del_file_(db, "DELETE FROM xapian_files WHERE rowid = ?;"),
    add_file_(db, "INSERT INTO xapian_files"
	      " (dir_docid, name, docid, mtime, inode, hash_id)"
	      " VALUES (?, ?, ?, ?, ?, ?);"),
    upd_file_(db, "UPDATE xapian_files SET mtime = ?, inode = ?"
	      " WHERE rowid = ?;"),
    get_hashid_(db, opt_fullscan
	      ? "SELECT hash_id, size, message_id FROM maildir_hashes"
	        " WHERE hash = ?;"
	      : "SELECT hash_id FROM maildir_hashes WHERE hash = ?;"),
    get_hash_(db, "SELECT hash, size FROM maildir_hashes WHERE hash_id = ?;"),
    add_hash_(db, "INSERT OR REPLACE INTO maildir_hashes "
	      " (hash, size, message_id, replica, version)"
	      " VALUES (?, ?, ?, %lld, %lld);", ws.first, ws.second),
    upd_hash_(db, "UPDATE maildir_hashes SET size = ?, message_id = ?"
	      " WHERE hash_id = ?;",
	      ws.first, ws.second)
{
}

string
fileops::get_msgid(i64 docid)
{
  get_msgid_.reset().param(docid).step();
  if (!get_msgid_.row())
    throw runtime_error ("xapian_fileops: unknown docid " + to_string(docid));
  return get_msgid_.str(0);
}

i64
fileops::get_file_hash_id(int dfd, const string &name, i64 docid)
{
  i64 sz;
  if (opt_verbose > 2)
    cerr << "    " << name << '\n';
  string hash = get_sha(dfd, name.c_str(), &sz);

  if (get_hashid_.reset().param(hash).step().row()) {
    i64 hash_id = get_hashid_.integer(0);
    if (!opt_fullscan)
      return hash_id;
    string msgid = get_msgid(docid);
    if (sz == get_hashid_.integer(1) && msgid == get_hashid_.str(2))
      return hash_id;
    // This should almost never happen
    cerr << "size or message-id changed for hash " << hash << '\n';
    upd_hash_.reset().param(sz, msgid, hash_id).step();
    return hash_id;
  }

  add_hash_.reset().param(hash, sz, get_msgid(docid)).step();
  return sqlite3_last_insert_rowid(add_hash_.getdb());
}

void
fileops::add_file(const string &dir, int dfd, i64 dir_docid,
		  string name, i64 docid)
{
  struct stat sb;
  if (fstatat(dfd, name.c_str(), &sb, 0)) {
    if (errno == ENOENT)
      return;
    throw runtime_error (dir + ": " + strerror(errno));
  }
  if (!S_ISREG(sb.st_mode))
    return;

  i64 hash_id = get_file_hash_id(dfd, name, docid);
  add_file_.reset()
    .param(dir_docid, name, docid, ts_to_double(sb.st_mtim),
	   i64(sb.st_ino), hash_id).step();
}

void
fileops::check_file(const string &dir, int dfd, i64 dir_docid)
{
  if (!opt_fullscan)
    return;
  string name = scan_dir_.str(1);
  struct stat sb;
  if (fstatat(dfd, name.c_str(), &sb, 0)) {
    if (errno == ENOENT)
      return;
    throw runtime_error (dir + ": " + strerror(errno));
  }
  if (!S_ISREG(sb.st_mode))
    return;

  double fs_mtim = ts_to_double(sb.st_mtim);
  i64 fs_inode = sb.st_ino, fs_size = sb.st_size;
  double db_mtim = scan_dir_.real(3);
  i64 db_inode = scan_dir_.integer(4);

  i64 db_hashid = scan_dir_.integer(5);
  if (!get_hash_.reset().param(db_hashid).step().row())
    throw runtime_error ("invalid hash_id: " + to_string(db_hashid));
  i64 db_size = get_hash_.integer(1);

  if (fs_mtim == db_mtim && fs_inode == db_inode && fs_size == db_size)
    return;

  i64 rowid = scan_dir_.integer(0), docid = scan_dir_.integer(2);
  i64 fs_hashid = get_file_hash_id(dfd, name, docid);
  if (db_hashid == fs_hashid)
    upd_file_.reset().param(fs_mtim, fs_inode, rowid).step();
  else {
    del_file_.reset().param(rowid).step();
    add_file_.reset().param(dir_docid, name, docid, fs_mtim, fs_inode,
			    fs_hashid);
  }
}

static void
xapian_scan_filenames (sqlite3 *db, const string &maildir,
		       const writestamp &ws, Xapian::Database xdb)
{
  sqlstmt_t dirscan (db, "SELECT dir_path, dir_docid FROM xapian_dirs;");
  fileops f (db, ws);

  while (dirscan.step().row()) {
    string dir = dirscan.str(0);
    if (opt_verbose > 1)
      cerr << "  " << dir << '\n';
    string dirpath = maildir + "/" + dir;
    int dfd = open(dirpath.c_str(), O_RDONLY);
    if (dfd == -1) {
      cerr << dirpath << ": " << strerror (errno) << '\n';
      continue;
    }
    cleanup _close (close, dfd);

    i64 dir_docid = dirscan.integer(1);
    f.scan_dir_.reset().param(dir_docid).step();

    string dirtermprefix = (notmuch_file_direntry_prefix
			    + to_string (dir_docid) + ":");
    Xapian::TermIterator ti = xdb.allterms_begin(dirtermprefix),
      te = xdb.allterms_end(dirtermprefix);
    size_t dirtermprefixlen = dirtermprefix.size();

    unordered_map<string,Xapian::docid> to_add;

    while (f.scan_dir_.row() && ti != te) {
      //string dbname = f.scan_dir_.str(1);
      //string xname = (*ti).substr(dirtermprefixlen);
      const char *dbname = f.scan_dir_.c_str(1);
      string term = *ti;
      const char *xname = &term[dirtermprefixlen];
      int cmp = strcmp(dbname,xname);
      if (!cmp) {
	if (opt_fullscan)
	  f.check_file(dir, dfd, dir_docid);
	f.scan_dir_.step();
	++ti;
      }
      else if (cmp < 0) {
	f.del_file(f.scan_dir_.integer(0));
	f.scan_dir_.step();
      }
      else {
	to_add.emplace(term.substr(dirtermprefixlen),
		       xapian_get_unique_posting(xdb, term));
	++ti;
      }
    }
    while (f.scan_dir_.row()) {
      f.del_file(f.scan_dir_.integer(0));
      f.scan_dir_.step();
    }
    while (ti != te) {
      string term = *ti;
      to_add.emplace(term.substr(dirtermprefixlen),
		     xapian_get_unique_posting(xdb, term));
      ++ti;
    }

    // With a cold buffer cache, reading files to compute hashes goes
    // shockingly faster in the order of directory entries.
    if (!to_add.empty()) {
      _close.disable();
      DIR *d = fdopendir(dfd);
      cleanup _closedir (closedir, d);
      struct dirent *e;
      auto notfound = to_add.end();
      while ((e = readdir(d)) && !to_add.empty()) {
	string name (e->d_name);
	auto action = to_add.find(name);
	if (action != notfound) {
	  f.add_file(dir, dfd, dir_docid, action->first, action->second);
	  to_add.erase(action);
	}
      }
    }
  }
}

static void
xapian_adjust_nlinks(sqlite3 *db, writestamp ws)
{
  sqlstmt_t
    newcount(db, "SELECT hash_id, dir_docid, count(*)"
	     " FROM xapian_files NATURAL JOIN modified_hashes"
	     " GROUP BY hash_id, dir_docid ORDER BY hash_id, dir_docid;"),
    oldcount(db, "SELECT hash_id, dir_docid, link_count, xapian_nlinks.rowid"
	     " FROM xapian_nlinks NATURAL JOIN modified_hashes"
	     " ORDER BY hash_id, dir_docid;"),
    updcount(db, "UPDATE xapian_nlinks SET link_count = ? WHERE rowid = ?;"),
    delcount(db, "DELETE FROM xapian_nlinks WHERE rowid = ?;"),
    addcount(db, "INSERT INTO xapian_nlinks (hash_id, dir_docid, link_count)"
	     " VALUES (?, ?, ?);"),
    updhash(db, "UPDATE maildir_hashes SET replica = %lld, version = %lld"
	    " WHERE hash_id = ?;", ws.first, ws.second);
    
  newcount.step();
  oldcount.step();
  while (newcount.row() || oldcount.row()) {
    i64 d;  // < 0 only oldcount valid, > 0 only newcount valid
    if (!newcount.row())
      d = -1;
    else if (!oldcount.row())
      d = 1;
    else if (!(d = oldcount.integer(0) - newcount.integer(0)))
      d = oldcount.integer(1) - newcount.integer(1);
    if (d == 0) {
      i64 cnt = newcount.integer(2);
      if (cnt != oldcount.integer(2)) {
	updhash.reset().param(newcount.value(0)).step();
	updcount.reset().param(cnt, oldcount.value(3)).step();
      }
      oldcount.step();
      newcount.step();
    }
    else if (d < 0) {
      // file deleted and (hash_id, dir_id) not present newcount
      updhash.reset().param(oldcount.value(0)).step();
      delcount.reset().param(oldcount.value(3)).step();
      oldcount.step();
    }
    else {
      // file added and (hash_id, dir_id) not present in oldcount
      updhash.reset().param(newcount.value(0)).step();
      addcount.reset().param(newcount.value(0), newcount.value(1),
			     newcount.value(2)).step();
      newcount.step();
    }
  }
}

void
xapian_scan(sqlite3 *sqldb, writestamp ws, string maildir)
{
  while (maildir.size() > 1 && maildir.back() == '/')
    maildir.resize (maildir.size() - 1);
  if (maildir.empty())
    maildir = ".";
  print_time ("starting scan of Xapian database");
  Xapian::Database xdb (maildir + "/.notmuch/xapian");
  set_triggers(sqldb);
  print_time ("opened Xapian");
  xapian_scan_message_ids (sqldb, ws, xdb);
  print_time ("scanned message IDs");
  xapian_scan_tags (sqldb, xdb, ws);
  print_time ("scanned tags");
  xapian_scan_directories (sqldb, xdb);
  print_time ("scanned directories in xapian");
  xapian_scan_filenames (sqldb, maildir, ws, xdb);
  print_time ("scanned filenames in xapian");
  xapian_adjust_nlinks(sqldb, ws);
  print_time ("adjusted link counts");
}
