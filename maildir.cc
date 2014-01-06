
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

const char maildir_schema[] = R"(
CREATE TABLE IF NOT EXISTS maildir_dirs (
  dir_path TEXT PRIMARY KEY,
  ctime REAL,
  mtime REAL,
  inode INTEGER
);
CREATE TABLE IF NOT EXISTS maildir_files (
  dir_path TEXT NOT NULL,
  name TEXT NOT NULL COLLATE BINARY,
  mtime REAL,
  inode INTEGER,
  size INTEGER,
  hash TEXT,
  replica INTEGER,
  version INTEGER,
  PRIMARY KEY (dir_path, name));
CREATE TABLE IF NOT EXISTS modified_directories (
  dir_path TEXT PRIMARY KEY);
)";

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setw (2) << setfill ('0');
  for (auto c : s)
    os << (int (c) & 0xff);
  return os.str ();
}

string
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
    create (sqldb, "INSERT INTO maildir_dirs (dir_path, inode) VALUES (?, 0);");

  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, nullptr), fts_close};
  if (!ftsp)
    throw runtime_error (maildir + ": " + strerror (errno));
  while (FTSENT *f = fts_read (ftsp.get())) {
    if (f->fts_info == FTS_D) {
      if (dir_contains_messages (f->fts_name)) {
	exists.reset().param(f->fts_path + dirlen).step();
	if (!exists.row())
	  create.reset().param(f->fts_path + dirlen).step();
      }
      if (f->fts_statp->st_nlink <= 2)
	fts_set (ftsp.get(), f, FTS_SKIP);
    }
  }
}

void
find_modified_directories (sqlite3 *sqldb, const string &maildir, int rootfd)
{
  sqlstmt_t
    scan (sqldb, "SELECT dir_path, ctime, mtime, inode FROM maildir_dirs;"),
    deldir (sqldb, "DELETE FROM maildir_dirs WHERE dir_path = ?;"),
    delfiles (sqldb, "DELETE FROM maildir_files WHERE dir_path = ?;"),
    upddir (sqldb, "UPDATE maildir_dirs "
	    "SET ctime = ?, mtime = ?, inode = ? WHERE dir_path = ?;");

  fmtexec (sqldb, "CREATE TEMP TRIGGER dir_update_trigger "
	   "AFTER UPDATE ON main.maildir_dirs BEGIN "
	   "INSERT INTO modified_directories (dir_path) VALUES (new.dir_path);"
	   "END;");

  while (scan.step().row()) {
    struct stat sb;
    if (fstatat (rootfd, scan.c_str(0), &sb, 0)) {
      if (errno != ENOENT)
	throw runtime_error (scan.str(0) + ": " + strerror (errno));
      deldir.reset().param(scan.value(0)).step();
      delfiles.reset().param(scan.value(0)).step();
      continue;
    }
    double ctim = ts_to_double(sb.st_ctim), mtim = ts_to_double(sb.st_mtim);
    if (i64(sb.st_ino) != scan.integer(3)
	|| ctim != scan.real(1) || mtim != scan.real(2))
      upddir.reset().param(ctim, mtim, i64(sb.st_ino), scan.value(0)).step();
  }
}

static int
compare_fts_name (const FTSENT **a, const FTSENT **b)
{
  return strcmp ((*a)->fts_name, (*b)->fts_name);
}

static void
scan_directory (sqlite3 *sqldb, const string &maildir, const string &subdir,
		sqlstmt_t &scan, sqlstmt_t &del_file, sqlstmt_t &add_file,
		sqlstmt_t &upd_file, int dfd)
{
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
    if (!opt_fullscan && cmp == 0) {
      scan.step();
      f = f->fts_link;
      continue;
    }
    struct stat sbuf;
    if (f->fts_info == FTS_NSOK && fstatat (dfd, f->fts_name, &sbuf, 0)) {
      if (errno == ENOENT) {
	f = f->fts_link;
	continue;
      }
      throw runtime_error (subdir + '/' + f->fts_name + ": "
			   + strerror (errno));
    }
    struct stat &sb = *(f->fts_info == FTS_NSOK ? &sbuf : f->fts_statp);
    if (!S_ISREG (sb.st_mode)) {
      f = f->fts_link;
      continue;
    }
    double mtim = ts_to_double(sb.st_mtim);
    if (cmp > 0) {
      cout << "hashing new file " << subdir << '/' << f->fts_name << "\n";
      string hashval = get_sha (dfd, f->fts_name, nullptr);
      add_file.reset()
	.param(f->fts_name, mtim, i64(sb.st_ino), i64(sb.st_size), hashval)
	.step();
      f = f->fts_link;
      continue;
    }
    if (mtim != scan.real(1) || sb.st_size != scan.integer(3)
	|| i64(sb.st_ino) != scan.integer(2)) {
      cout << "hashing old file " << subdir << '/' << f->fts_name << "\n";
      string hashval = get_sha (dfd, f->fts_name, nullptr);
      if (sb.st_size != scan.integer(4) || hashval != scan.str(4)) {
	del_file.reset().param(scan.value(5)).step();
	add_file.reset()
	  .param(f->fts_name, mtim, i64(sb.st_ino), i64(sb.st_size), hashval)
	  .step();
      }
      else {
	cout << "hash did not change\n";
	upd_file.reset().param(mtim, i64(sb.st_ino), scan.value(5)).step();
      }
    }
    scan.step();
    f = f->fts_link;
  }
  for (; scan.row(); scan.step())
    del_file.reset().param(scan.value(5)).step();
  for (; f; f = f->fts_link) {
    if (f->fts_info != FTS_F && f->fts_info != FTS_NSOK)
      continue;
    struct stat sbuf;
    if (f->fts_info == FTS_NSOK && fstatat (dfd, f->fts_name, &sbuf, 0)) {
      if (errno == ENOENT)
	continue;
      throw runtime_error (subdir + '/' + f->fts_name + ": "
			   + strerror (errno));
    }
    struct stat &sb = *(f->fts_info == FTS_NSOK ? &sbuf : f->fts_statp);
    if (!S_ISREG (sb.st_mode))
      continue;
    double mtim = ts_to_double(sb.st_mtim);
    string hashval = get_sha (dfd, f->fts_name, nullptr);
    add_file.reset()
      .param(f->fts_name, mtim, i64(sb.st_ino), i64(sb.st_size), hashval)
      .step();
  }
}

static void
scan_files (sqlite3 *sqldb, const string &maildir, int rootfd)
{
  sqlstmt_t
    scandirs (sqldb, "SELECT dir_path FROM %s ORDER BY dir_path;",
	      opt_fullscan ? "maildir_dirs" : "modified_directories"),
    scanfiles (sqldb, "SELECT name, mtime, inode, size, hash, rowid"
	       " FROM maildir_files WHERE dir_path = ? ORDER BY name;"),
    del_file (sqldb, "DELETE FROM maildir_files WHERE rowid = ?;"),
    add_file (sqldb, "INSERT INTO maildir_files"
	      " (name, mtime, inode, size, hash, dir_path)"
	      " VALUES (?, ?, ?, ?, ?, ?);"),
    upd_file (sqldb, "UPDATE maildir_files"
	      " SET mtime = ?, inode = ? WHERE rowid = ?;");

  while (scandirs.step().row()) {
    string dir {scandirs.str(0)};
    int dfd = openat(rootfd, dir.c_str(), O_RDONLY);
    if (dfd < 0)
      throw runtime_error (dir + ": " + strerror (errno));
    cleanup _c (close, dfd);
    scanfiles.reset().bind_text(1, dir);
    add_file.reset().bind_text(6, dir);
    scan_directory (sqldb, maildir, dir,
		    scanfiles, del_file, add_file, upd_file, dfd);
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

  fmtexec (sqldb, maildir_schema);
  fmtexec (sqldb, "DELETE FROM modified_directories;");
  print_time ("finding new subdirectories of maildir");
  find_new_directories (sqldb, maildir, rootfd);
  print_time ("finding modified directories in maildir");
  find_modified_directories (sqldb, maildir, rootfd);
  print_time (opt_fullscan ? "scanning files in all directories"
	      : "scanning files in modified directories");
  scan_files (sqldb, maildir, rootfd);

  //scan_directory (sqldb, maildir, "");

  // traverse_maildir (maildir, oldtime, lookup, insert);
}
