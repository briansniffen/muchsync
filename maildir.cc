
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fts.h>
#include <sys/stat.h>
#include <openssl/sha.h>

#include "muchsync.h"

using namespace std;

const char maildir_def[] = R"(CREATE TABLE IF NOT EXISTS maildir (
  filename TEXT PRIMARY KEY,
  ctime REAL,
  mtime REAL,
  size INTEGER,
  hash TEXT,
  replica INTEGER,
  version INTEGER
);)";

constexpr bool
changed_since (const struct stat *sb, double time)
{
  return ts_to_double (sb->st_mtim) >= time
    || ts_to_double (sb->st_ctim) >= time;
}

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

string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setw (2) << setfill ('0');
  for (auto c : s)
    os << (int (c) & 0xff);
  return os.str ();
}

string
get_sha (ifstream &msg)
{
  msg.seekg (0, ios_base::beg);
  SHA_CTX ctx;
  SHA1_Init (&ctx);
  while (msg) {
    char buf[8192];
    streamsize n = msg.readsome (buf, sizeof (buf));
    if (n <= 0)
      break;
    SHA1_Update (&ctx, buf, n);
  }
  unsigned char resbuf[SHA256_DIGEST_LENGTH];
  SHA1_Final (resbuf, &ctx);
  string res { reinterpret_cast<const char *> (resbuf), sizeof (resbuf) };
  return msg.fail() ? "" : hexdump (res);
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

void
scan_maildir (sqlite3 *sqldb, writestamp ws, const string &maildir)
{
  fmtexec (sqldb, maildir_def);

  double newtime, oldtime;
  {
    timespec ts;
    if (clock_gettime (CLOCK_REALTIME, &ts)) {
      perror ("clock_gettime");
      throw runtime_error (string ("clock_gettime: ") + strerror (errno));
    }
    newtime = ts_to_double (ts);

    try { oldtime = getconfig<double> (sqldb, "timestamp"); }
    catch (sqldone_t) { oldtime = 0; }
  }
  if (oldtime > newtime) {
    cerr << "Warning: ignoring database timestamp in the future\n";
    oldtime = 0;
  }

  sqlstmt_t
    lookup(sqldb, "SELECT ctime, mtime, size, hash FROM maildir"
	   " WHERE filename = ?;"),
    insert(sqldb, "INSERT OR REPLACE INTO"
	   " maildir (filename, ctime, mtime, size, hash,"
	   " replica, version)"
	   " VALUES (?, ?, ?, ?, ?, %lld, %lld);", ws.first, ws.second);

  traverse_maildir (maildir, oldtime, lookup, insert);

  setconfig (sqldb, "timestamp", newtime);
}
