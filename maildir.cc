
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

#include <openssl/sha.h>

#include "muchsync.h"

using namespace std;

const char maildir_triggers[] = R"(
CREATE TEMP TABLE modified_maildir_dirs (
  dir_id INTEGER PRIMARY KEY,
  dir_path TEXT UNIQUE NOT NULL);
CREATE TEMP TRIGGER dir_update_trigger AFTER UPDATE ON main.maildir_dirs
  BEGIN INSERT INTO modified_maildir_dirs (dir_id, dir_path)
        VALUES (new.dir_id, new.dir_path);
  END;
CREATE TEMP TABLE modified_maildir_hashes (
  hash_id INTEGER PRIMARY KEY);
CREATE TEMP TRIGGER maildir_files_add AFTER UPDATE ON main.maildir_files
  BEGIN INSERT INTO modified_maildir_hashes (hash_id) VALUES (new.hash_id);
  END;
CREATE TEMP TRIGGER maildir_files_del AFTER DELETE ON main.maildir_files
  BEGIN INSERT INTO modified_maildir_hashes (hash_id) VALUES (old.hash_id);
  END;
)";

const char maildir_cleanup[] = R"(
DROP TRIGGER dir_update_trigger;
DROP TABLE modified_maildir_dirs;
)";

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setfill ('0');
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
get_sha (int dfd, const char *direntry)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);

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

#if 0
bool
get_header (istream &in, string &name, string &value)
{
  string line;
  getline (in, line);
  string::size_type idx = line.find (':');
  if (idx == string::npos)
    return false;
  name.resize (idx);
  for (string::size_type i = 0; i < idx; i++)
    name[i] = tolower (line[i]);
  value = line.substr (idx+1, string::npos);
  while (in.peek() == ' ' || in.peek() == '\t') {
    getline (in, line);
    value += line;
  }
  return true;
}

bool
get_msgid (ifstream &msg, string &msgid)
{
  msg.seekg (0, ios_base::beg);
  string name, val;
  while (get_header (msg, name, val))
    if (name == "message-id") {
      string::size_type b{0}, e{val.size()};
      while (b < e && isspace(val[b]))
	++b;
      if (b < e && val[b] == '<')
	++b;
      while (b < e && isspace(val[e-1]))
	--e;
      if (b < e && val[e-1] == '>')
	--e;
      msgid = val.substr (b, e-b);
      return true;
    }
  return false;
}

bool
get_msgid (const string &file, string &msgid)
{
  ifstream msg (file);
  return get_msgid (msg, msgid);
}
#endif

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
      string dirpath (f->fts_path + dirlen);
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
  unordered_map<string, i64> modified;

  sqlstmt_t
    scan (sqldb, "SELECT dir_id, dir_path, ctime, mtime, inode"
	  " FROM maildir_dirs;"),
    deldir (sqldb, "DELETE FROM maildir_dirs WHERE dir_id = ?;"),
    upddir (sqldb, "UPDATE maildir_dirs "
	    "SET ctime = ?, mtime = ?, inode = ? WHERE dir_id = ?;");

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
	|| ctim != scan.real(2) || mtim != scan.real(3))
      upddir.reset().param(ctim, mtim, i64(sb.st_ino), scan.value(0)).step();
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
check_file (sqlstmt_t &scan, const struct stat *sbp)
{
  return !S_ISREG(sbp->st_mode)
    || ts_to_double(sbp->st_mtim) != scan.real(1)
    || i64(sbp->st_ino) != scan.integer(2)
    || i64(sbp->st_size) != scan.integer(3);
}

static void
scan_directory (sqlite3 *sqldb, const string &maildir, const string &subdir,
		sqlstmt_t &scan, sqlstmt_t &del_file, sqlstmt_t &add_file,
		sqlstmt_t &upd_file, int dfd,
		function<i64(const string &)> gethash)
{
  struct stat sbuf;		// Just storage in case !opt_fullscan
  string path = maildir.size() ? maildir + "/" + subdir : subdir;
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
    cout << "  " << subdir << '\n';

  scan.step();
  while (scan.row() && f) {
    if (f->fts_info != FTS_F && f->fts_info != FTS_NSOK) {
      f = f->fts_link;
      continue;
    }
    int cmp = strcmp (scan.c_str(0), f->fts_name);
    if (cmp < 0) {
      del_file.reset().param(scan.value(5)).step();
      scan.step();
      continue;
    }
    else if (cmp == 0 && (!opt_fullscan || !check_file (scan, f->fts_statp))) {
      scan.step();
      f = f->fts_link;
      continue;
    }
    /* At this point we either have a new file or the metadata has
     * changed suspiciously.  Either way, assuming it's still a
     * regular file, we must compute a full hash of its contents. */
    const struct stat *sbp = ftsent_stat (dfd, f, &sbuf);
    if (!S_ISREG (sbp->st_mode)) {
      del_file.reset().param(scan.value(5)).step();
      scan.step();
      f = f->fts_link;
    }
    if (opt_verbose > 2)
      cout << "    " << f->fts_name << "\n";
    i64 hashid = gethash (get_sha (dfd, f->fts_name));
    if (cmp == 0) {
      /* metadata changed suspiciously */
      if (hashid == scan.integer(4)) {
	/* file unchanged; update metadata so we don't re-hash next time */
	upd_file.reset().param(ts_to_double(sbp->st_mtim),i64(sbp->st_ino),
			       i64(sbp->st_size),scan.value(5)).step();
	scan.step();
	f = f->fts_link;
	continue;
      }
      cerr << "warning: " << subdir + "/" + f->fts_name << " was modified\n";
      /* file changed; delete old entry and consider it a new file */
      del_file.reset().param(scan.value(5)).step();
    }
    add_file.reset().param(f->fts_name, ts_to_double(sbp->st_mtim),
			   i64(sbp->st_ino), i64(sbp->st_size), hashid).step();
    scan.step();
    f = f->fts_link;
  }
  for (; scan.row(); scan.step())
    del_file.reset().param(scan.value(5)).step();
  for (; f; f = f->fts_link) {
    const struct stat *sbp = ftsent_stat (dfd, f, &sbuf);
    if (!S_ISREG (sbp->st_mode))
      continue;
    if (opt_verbose > 2)
      cout << "    " << f->fts_name << "\n";
    i64 hashid = gethash (get_sha (dfd, f->fts_name));
    add_file.reset().param(f->fts_name, ts_to_double(sbp->st_mtim),
			   i64(sbp->st_ino), i64(sbp->st_size), hashid).step();
  }
}

static void
scan_files (sqlite3 *sqldb, const string &maildir, int rootfd, writestamp ws)
{
  sqlstmt_t
    scandirs (sqldb, "SELECT dir_id, dir_path FROM %s ORDER BY dir_path;",
	      opt_fullscan ? "maildir_dirs" : "modified_maildir_dirs"),
    scanfiles (sqldb, "SELECT name, mtime, inode, size, hash_id, rowid"
	       " FROM maildir_files WHERE dir_id = ? ORDER BY name;"),
    del_file (sqldb, "DELETE FROM maildir_files WHERE rowid = ?;"),
    add_file (sqldb, "INSERT INTO maildir_files"
	      " (name, mtime, inode, size, hash_id, dir_id)"
	      " VALUES (?, ?, ?, ?, ?, ?);"),
    upd_file (sqldb, "UPDATE maildir_files"
	      " SET mtime = ?, inode = ?, size = ? WHERE rowid = ?;"),
    get_hash (sqldb, "SELECT hash_id, replica, version"
	      " FROM maildir_hashes WHERE hash = ?;"),
    add_hash (sqldb, "INSERT INTO maildir_hashes (hash, replica, version)"
	      " VALUES (?, %lld, %lld);", ws.first, ws.second);

  auto gethash = [sqldb,ws,&get_hash,&add_hash] (const string &h) -> i64 {
    if (get_hash.reset().param(h).step().row())
      return get_hash.integer(0);
    add_hash.reset().param(h).step().row();
    return sqlite3_last_insert_rowid (sqldb);
  };

  while (scandirs.step().row()) {
    string dir {scandirs.str(1)};
    int dfd = openat(rootfd, dir.c_str(), O_RDONLY);
    if (dfd < 0)
      throw runtime_error (dir + ": " + strerror (errno));
    cleanup _c (close, dfd);
    scanfiles.reset().bind_value(1, scandirs.value(0));
    add_file.reset().bind_value(6, scandirs.value(0));
    scan_directory (sqldb, maildir, dir,
		    scanfiles, del_file, add_file, upd_file, dfd, gethash);
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
  while (maildir.size() && maildir.back() == '/')
    maildir.resize (maildir.size() - 1);

  int rootfd = open (maildir.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (maildir + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  sqlexec (sqldb, maildir_triggers);
  print_time ("finding new subdirectories of maildir");
  find_new_directories (sqldb, maildir, rootfd);
  print_time ("finding modified directories in maildir");
  find_modified_directories (sqldb, maildir, rootfd);
  sqlexec (sqldb, "UPDATE maildir_dirs SET dir_docid = "
	   "(SELECT xapian_dirs.dir_docid FROM xapian_dirs WHERE"
	   " xapian_dirs.dir_path = maildir_dirs.dir_path);");
  print_time (opt_fullscan ? "scanning files in all directories"
	      : "scanning files in modified directories");
  scan_files (sqldb, maildir, rootfd, ws);
  print_time ("updating version stamps");
  sync_maildir_ws (sqldb, ws);
  print_time ("cleaning up temporary tables");
  sqlexec (sqldb, maildir_cleanup);

  //scan_directory (sqldb, maildir, "");

  // traverse_maildir (maildir, oldtime, lookup, insert);
}
