
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

const char maildir_def[] = R"(
CREATE TABLE IF NOT EXISTS maildir_dirs (
  dir_path TEXT PRIMARY KEY,
  ctime REAL,
  mtime REAL,
  inode INTEGER
);
CREATE TABLE IF NOT EXISTS maildir_files (
  dir_path TEXT NOT NULL,
  name TEXT NOT NULL,
  ctime REAL,
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

#if 0
inline void
check_message (sqlstmt_t &lookup, sqlstmt_t &insert, int skip, FTSENT *f)
{
  string filename (f->fts_accpath + skip);

  double ctime (ts_to_double(f->fts_statp->st_ctim)),
    mtime (ts_to_double(f->fts_statp->st_mtim));
  i64 size = f->fts_statp->st_size;
  string hash;

  lookup.reset().param(filename).step();

  if (lookup.done() || lookup.null(3) || size != lookup.integer(2)
      || mtime != lookup.real(1)) {
    ifstream msg (f->fts_accpath);
    hash = get_sha (msg);
    if (msg.fail()) {
      cerr << "Warning: Cannot read " << filename << '\n';
      return;
    }
  }
  else if (ctime == lookup.real(0))
    return;
  else
    hash = lookup.str(3);

  insert.reset().param(filename, ctime, mtime, size, hash).step();
  // cout << filename << "..." << hash << '\n';
}

static void
traverse_maildir (const string &dir, double oldest,
		  sqlstmt_t &lookup, sqlstmt_t &insert)
{
  int dirlen = dir.length();
  char *paths[] {const_cast<char *> (dir.c_str()), nullptr};
  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, nullptr), fts_close};
  if (!ftsp)
    throw runtime_error (dir + ": " + strerror (errno));
  bool in_msg_dir = false;
  while (FTSENT *f = fts_read (ftsp.get())) {
    if (in_msg_dir) {
      if (f->fts_info == FTS_D)
	fts_set (ftsp.get(), f, FTS_SKIP);
      else if (f->fts_info == FTS_DP) {
	assert (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new"));
	in_msg_dir = false;
      }
      else if (f->fts_name[0] != '.'
	       && changed_since (f->fts_statp, oldest))
	check_message (lookup, insert, dirlen, f);
    }
    else if (f->fts_info == FTS_D) {
      if (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new")) {
	if (changed_since (f->fts_statp, oldest))
	  in_msg_dir = true;
	else
	  fts_set (ftsp.get(), f, FTS_SKIP);
      }
      else if (f->fts_statp->st_nlink <= 2)
	fts_set (ftsp.get(), f, FTS_SKIP);
    }
  }
}
#endif

void
find_modified_directories (sqlite3 *sqldb, const string &maildir, int rootfd)
{
  int dirlen = maildir.length();
  char *paths[] {const_cast<char *> (maildir.c_str()), nullptr};

  /*
  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, nullptr), fts_close};
  if (!ftsp)
    throw runtime_error (dir + ": " + strerror (errno));
  bool in_msg_dir = false;
  while (FTSENT *f = fts_read (ftsp.get())) {
    if (in_msg_dir) {
      if (f->fts_info == FTS_D)
	fts_set (ftsp.get(), f, FTS_SKIP);
      else if (f->fts_info == FTS_DP) {
	assert (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new"));
	in_msg_dir = false;
      }
      else if (f->fts_name[0] != '.'
	       && changed_since (f->fts_statp, oldest))
	check_message (lookup, insert, dirlen, f);
    }
    else if (f->fts_info == FTS_D) {
      if (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new")) {
	if (changed_since (f->fts_statp, oldest))
	  in_msg_dir = true;
	else
	  fts_set (ftsp.get(), f, FTS_SKIP);
      }
      else if (f->fts_statp->st_nlink <= 2)
	fts_set (ftsp.get(), f, FTS_SKIP);
    }
  }
  */
}

void
scan_maildir (sqlite3 *sqldb, writestamp ws, const string &maildir)
{
  int rootfd = open (maildir.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (maildir + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  fmtexec (sqldb, "DELETE FROM modified_directories;");
  find_modified_directories (sqldb, maildir, rootfd);

  fmtexec (sqldb, maildir_def);


  // traverse_maildir (maildir, oldtime, lookup, insert);
}
