
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <unistd.h>
#include <sys/stat.h>

#include "muchsync.h"
#include "work_queue.h"

using namespace std;

class file_dbops {
  mutex m_;
  sqlstmt_t del_file_;
  sqlstmt_t add_file_;
  sqlstmt_t upd_file_;
  sqlstmt_t mod_hash_;
  sqlstmt_t get_hash_;
  sqlstmt_t add_hash_;

  static sqlstmt_t make_mod_hash(sqlite3 *db) {
    sqlexec(db, "CREATE TEMP TABLE IF NOT EXISTS modified_maildir_hashes ("
	    "hash_id INTEGER PRIMARY KEY);");
    return sqlstmt_t(db,
		     "INSERT OR IGNORE INTO modified_maildir_hashes (hash_id)"
		     " VALUES(?);");
  }

public:
  file_dbops(sqlite3 *db, writestamp ws)
    : del_file_(db, "DELETE FROM maildir_files WHERE rowid = ?;"),
      add_file_(db, "INSERT INTO "
		"maildir_files (name, mtime, inode, hash_id, dir_id)"
		" VALUES (?, ?, ?, ?, ?);"),
      upd_file_(db, "UPDATE maildir_files"
		" SET mtime = ?, inode = ? WHERE rowid = ?;"),
      mod_hash_(make_mod_hash(db)),
      get_hash_(db, "SELECT hash_id, replica, version, message_id"
		" FROM maildir_hashes WHERE hash = ?;"),
      add_hash_(db, "INSERT INTO maildir_hashes (hash, size, replica, version)"
		" VALUES (?, ?, %lld, %lld);", ws.first, ws.second)
  {
  }

  i64 get_hash_id(const string &hash, i64 sz) {
    lock_guard<mutex> _lk (m_);
    if (get_hash_.reset().param(hash).step().row())
      return get_hash_.integer(0);
    add_hash_.reset().param(hash, sz).step();
    return sqlite3_last_insert_rowid (sqlite3_db_handle(add_hash_.get()));
  }
  void del_file(i64 rowid, i64 hash_id) {
    lock_guard<mutex> _lk (m_);
    del_file_.reset().param(rowid).step();
    mod_hash_.reset().param(hash_id).step();
  }
  void add_file(const char *name, const timespec &mtime, i64 inode,
		i64 hash_id, i64 dir_id) {
    lock_guard<mutex> _lk (m_);
    add_file_.reset().param(name, ts_to_double(mtime), inode,
			    hash_id, dir_id).step();
    mod_hash_.reset().param(hash_id).step();
  }
  void upd_file(i64 rowid, const timespec &mtime, i64 inode) {
    lock_guard<mutex> _lk (m_);
    upd_file_.reset().param(ts_to_double(mtime), inode, rowid).step();
  }
};

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setfill('0');
  for (auto c : s)
    os << setw(2) << (int (c) & 0xff);
  string ret = os.str();
  if (ret.size() != 2 * s.size()) {
    cerr << ret.size() << " != 2 * " << s.size () << "\n";
    cerr << "s[0] == " << hex << unsigned (s[0]) << ", s.back() = "
	 << unsigned (s.back()) << "\n";
    terminate();
  }
  return ret;
}

string
hash_ctx::final()
{
  unsigned char resbuf[output_bytes];
  SHA1_Final (resbuf, &ctx_);
  return hexdump ({ reinterpret_cast<const char *> (resbuf), sizeof (resbuf) });
}

static string
get_sha (int dfd, const char *direntry, i64 *sizep)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);

  hash_ctx ctx;
  char buf[16384];
  int n;
  i64 sz = 0;
  while ((n = read (fd, buf, sizeof (buf))) > 0) {
    ctx.update (buf, n);
    sz += n;
  }
  if (n < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  if (sizep)
    *sizep = sz;
  return ctx.final();
}

void
find_new_directories (sqlite3 *sqldb, const string &maildir, int rootfd)
{
  int dirlen = maildir.length() + 1; // remove maildir and slash
  char *paths[] {const_cast<char *> (maildir.c_str()), nullptr};

  sqlstmt_t
    exists (sqldb, "SELECT 1 FROM maildir_dirs WHERE dir_path = ?;"),
    create (sqldb, "INSERT INTO maildir_dirs (dir_path) VALUES (?);");

  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, nullptr), fts_close};
  if (!ftsp)
    throw runtime_error (maildir + ": " + strerror (errno));
  while (FTSENT *f = fts_read (ftsp.get())) {
    if (f->fts_info == FTS_D) {
      string dirpath (f->fts_pathlen > dirlen ? f->fts_path + dirlen : "");
      if (dirpath == ".notmuch") {
	fts_set (ftsp.get(), f, FTS_SKIP);
	continue;
      }
      if (dir_contains_messages (f->fts_name)) {
	exists.reset().param(dirpath).step();
	if (!exists.row())
	  create.reset().param(dirpath).step();
      }
      if (f->fts_statp->st_nlink <= 2)
	fts_set (ftsp.get(), f, FTS_SKIP);
    }
  }
}

void
find_modified_directories (sqlite3 *sqldb, const string &maildir, int rootfd)
{
  sqlexec (sqldb, "CREATE TEMP TABLE modified_maildir_dirs ("
	   "dir_id INTEGER PRIMARY KEY);");
  sqlstmt_t
    scan (sqldb, "SELECT dir_id, dir_path, ctime, mtime, inode"
	  " FROM maildir_dirs;"),
    deldir (sqldb, "DELETE FROM maildir_dirs WHERE dir_id = ?;"),
    upddir (sqldb, "UPDATE maildir_dirs "
	    "SET ctime = ?, mtime = ?, inode = ? WHERE dir_id = ?;"),
    moddir (sqldb, "INSERT INTO modified_maildir_dirs (dir_id) VALUES (?)");

  while (scan.step().row()) {
    struct stat sb;
    int res = fstatat (rootfd, scan.c_str(1), &sb, 0);
    if ((res && errno == ENOENT) || (!res && !S_ISDIR(sb.st_mode))) {
      deldir.reset().param(scan.value(0)).step();
      continue;
    }
    if (res)
      throw runtime_error (scan.str(1) + ": " + strerror (errno));
    double ctim = ts_to_double(sb.st_ctim), mtim = ts_to_double(sb.st_mtim);
    if (scan.null(4) || i64(sb.st_ino) != scan.integer(4)
	|| ctim != scan.real(2) || mtim != scan.real(3)) {
      upddir.reset().param(ctim, mtim, i64(sb.st_ino), scan.value(0)).step();
      moddir.reset().param(scan.value(0)).step();
    }
  }
}

static int
compare_fts_name (const FTSENT **a, const FTSENT **b)
{
  return strcmp ((*a)->fts_name, (*b)->fts_name);
}

static const struct stat *
ftsent_stat (int dfd, const FTSENT *f, struct stat *sbp)
{
  if (f->fts_info != FTS_NSOK)
    return f->fts_statp;
  if (!fstatat (dfd, f->fts_name, sbp, 0))
    return sbp;
  throw runtime_error (string (f->fts_path) + ": " + strerror (errno));
}

/* true if we need to re-hash */
static bool
must_check_file (sqlstmt_t &scan, const struct stat *sbp)
{
  return !S_ISREG(sbp->st_mode)
    || ts_to_double(sbp->st_mtim) != scan.real(1)
    || i64(sbp->st_ino) != scan.integer(2)
    || i64(sbp->st_size) != scan.integer(3);
}

static void
scan_directory (const string &path, int dfd, i64 dir_id,
		sqlstmt_t &scan, file_dbops &fdb, work_queue &wq)
{
  struct stat sbuf;		// Just storage in case !opt_fullscan
  char *paths[] {const_cast<char *> (path.c_str()), nullptr};
  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, &compare_fts_name), fts_close};
  if (!ftsp)
    throw runtime_error (path + ": " + strerror (errno));
  errno = 0;
  FTSENT *f = fts_read (ftsp.get());
  if (f)
    f = fts_children (ftsp.get(), opt_fullscan ? 0 : FTS_NAMEONLY);
  if (errno)
    throw runtime_error (path + ": " + strerror (f->fts_errno));

  if (opt_verbose > 1)
    cerr << "  " << path << '\n';

  scan.step();
  while (scan.row() && f) {
    if (f->fts_info != FTS_F && f->fts_info != FTS_NSOK) {
      f = f->fts_link;
      continue;
    }
    const i64 rowid = scan.integer(5), hash_id = scan.integer(4);
    int cmp = strcmp (scan.c_str(0), f->fts_name);
    if (cmp < 0) {
      fdb.del_file(rowid, hash_id);
      scan.step();
      continue;
    }
    else if (cmp == 0
	     && (!opt_fullscan || !must_check_file(scan, f->fts_statp))) {
      scan.step();
      f = f->fts_link;
      continue;
    }

    /* At this point we either have a new file or the metadata has
     * changed suspiciously.  Either way, assuming it's still a
     * regular file, we must compute a full hash of its contents. */
    const struct stat *sbp = ftsent_stat(dfd, f, &sbuf);
    if (!S_ISREG (sbp->st_mode)) {
      fdb.del_file(rowid, hash_id);
      scan.step();
      f = f->fts_link;
      continue;
    }
    timespec mtim = sbp->st_mtim;
    i64 inode = sbp->st_ino;
    string name = f->fts_name;

    if (opt_verbose > 2)
      cerr << "    " << name << "\n";

    //wq.enqueue([path,cmp,mtim,inode,rowid,hash_id,name,dir_id,&fdb]() {
	i64 sz;
	string hash = get_sha(AT_FDCWD, (path + "/" + name).c_str(), &sz);
	i64 new_hash_id = fdb.get_hash_id(hash, sz);
	if (cmp == 0) {
	  /* metadata changed suspiciously */
	  if (new_hash_id == hash_id) {
	    /* file unchanged; update metadata so we don't re-hash next time */
	    fdb.upd_file(rowid, mtim, inode);
    scan.step();
    f = f->fts_link;
    continue;
	    return;
	  }
	  cerr << "warning: " << path + "/" + name << " was modified\n";
	  /* file changed; delete old entry and consider it a new file */
	  fdb.del_file(rowid, hash_id);
	}
	fdb.add_file(name.c_str(), mtim, inode, new_hash_id, dir_id);
	//});
    scan.step();
    f = f->fts_link;
  }
  for (; scan.row(); scan.step())
    fdb.del_file(scan.integer(5), scan.integer(4));
  for (; f; f = f->fts_link) {
    const struct stat *sbp = ftsent_stat (dfd, f, &sbuf);
    if (!S_ISREG (sbp->st_mode))
      continue;
    if (opt_verbose > 2)
      cerr << "    " << f->fts_name << "\n";
    timespec mtim = sbp->st_mtim;
    i64 inode = sbp->st_ino;
    string name = f->fts_name;
    wq.enqueue([path,name,mtim,inode,dir_id,&fdb](){
	i64 sz;
	string hash = get_sha(AT_FDCWD, (path + "/" + name).c_str(), &sz);
	i64 new_hash_id = fdb.get_hash_id(hash, sz);
	fdb.add_file(name.c_str(), mtim, inode, new_hash_id, dir_id);
      });
  }
}

static void
scan_directories (sqlite3 *sqldb, const string &maildir,
		  int rootfd, writestamp ws)
{
  sqlstmt_t
    scandirs (sqldb, "SELECT dir_id, dir_path FROM %s ORDER BY dir_path;",
	      opt_fullscan ? "maildir_dirs"
	      : "maildir_dirs NATURAL JOIN modified_maildir_dirs"),
    scanfiles (sqldb, "SELECT name, mtime, inode, size, hash_id,"
	       " maildir_files.rowid"
	       " FROM maildir_files JOIN maildir_hashes USING (hash_id)"
	       " WHERE dir_id = ? ORDER BY name;");
  file_dbops fdb (sqldb, ws);
  int dfd;
  work_queue wq;

  while (scandirs.step().row()) {
    i64 dir_id {scandirs.integer(0)};
    string subdir {scandirs.str(1)};
    dfd = openat(rootfd, subdir.c_str(), O_RDONLY);
    if (dfd < 0)
      throw runtime_error (subdir + ": " + strerror (errno));
    cleanup _c (close, dfd);
    scanfiles.reset().bind_value(1, scandirs.value(0));

    string dir = maildir
      + (subdir.size() && maildir.size() && maildir.back() != '/' ? "/" : "")
      + subdir;
    if (dir.empty())
      dir = ".";
    scan_directory (dir, dfd, dir_id, scanfiles, fdb, wq);
  }
}

static void
sync_maildir_ws (sqlite3 *sqldb, writestamp ws)
{
  // Sadly, sqlite has no full outer join.  Hence we manually scan
  // both oldcount and newcount to bring newcount up to date.
  sqlstmt_t
    newcount (sqldb, "SELECT hash_id, dir_id, count(*)"
	      " FROM maildir_files NATURAL JOIN modified_maildir_hashes"
	      " GROUP BY hash_id, dir_id ORDER BY hash_id, dir_id;"),
    oldcount (sqldb, "SELECT hash_id, dir_id, link_count, maildir_links.rowid"
	      " FROM maildir_links NATURAL JOIN modified_maildir_hashes"
	      " ORDER BY hash_id, dir_id;"),
    updcount (sqldb, "UPDATE maildir_links SET link_count = ?"
	      " WHERE rowid = ?;"),
    updhash (sqldb, "UPDATE maildir_hashes SET replica = %lld, version = %lld"
	     " WHERE hash_id = ?;",
	     ws.first, ws.second, ws.first, ws.second),
    addlink (sqldb, "INSERT INTO maildir_links (hash_id, dir_id, link_count)"
	     " VALUES (?, ?, ?);"),
    dellink (sqldb, "DELETE FROM maildir_links WHERE rowid = ?;");

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
      dellink.reset().param(oldcount.value(3)).step();
      oldcount.step();
    }
    else {
      // file added and (hash_id, dir_id) not present in oldcount
      updhash.reset().param(newcount.value(0)).step();
      addlink.reset().param(newcount.value(0), newcount.value(1),
			    newcount.value(2)).step();
      newcount.step();
    }
  }
}

void
scan_maildir (sqlite3 *sqldb, writestamp ws, string maildir)
{
  while (maildir.size() > 1 && maildir.back() == '/')
    maildir.resize (maildir.size() - 1);

  print_time ("starting scan of mail directory");
  int rootfd = open (maildir.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (maildir + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  find_new_directories (sqldb, maildir, rootfd);
  print_time ("searched for new subdirectories of maildir");
  find_modified_directories (sqldb, maildir, rootfd);
  sqlexec (sqldb, "UPDATE maildir_dirs SET dir_docid = "
	   "(SELECT xapian_dirs.dir_docid FROM xapian_dirs WHERE"
	   " xapian_dirs.dir_path = maildir_dirs.dir_path);");
  print_time ("search for modified directories in maildir");
  scan_directories (sqldb, maildir, rootfd, ws);
  print_time (opt_fullscan ? "scanned files in all directories"
	      : "scanned files in modified directories");
  if (opt_fullscan)
    sqlexec (sqldb, R"(
UPDATE maildir_hashes SET message_id =
ifnull ((SELECT message_id FROM maildir_files JOIN maildir_dirs USING (dir_id)
   JOIN xapian_files USING (dir_docid, name) JOIN message_ids USING (docid)
   WHERE maildir_files.hash_id = maildir_hashes.hash_id),
  maildir_hashes.message_id);
)");
  else
    sqlexec (sqldb, R"(
UPDATE maildir_hashes SET message_id =
  (SELECT message_id FROM maildir_files JOIN maildir_dirs USING (dir_id)
   JOIN xapian_files USING (dir_docid, name) JOIN message_ids USING (docid)
   WHERE maildir_files.hash_id = maildir_hashes.hash_id)
  WHERE message_id IS NULL;
)");
  print_time ("updated message ids");
  sync_maildir_ws (sqldb, ws);
  print_time ("updated version stamps");
}
