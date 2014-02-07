
#include <cstring>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/stat.h>
#include <openssl/rand.h>
#include <notmuch.h>
#include "muchsync.h"
#include "infinibuf.h"

using namespace std;

const char dbvers[] = "muchsync 0";
#define MUCHSYNC_DEFDIR "/.notmuch/muchsync"
const char muchsync_defdir[] = MUCHSYNC_DEFDIR;
const char muchsync_dbpath[] = MUCHSYNC_DEFDIR "/state.db";
const char muchsync_trashdir[] = MUCHSYNC_DEFDIR "/trash";
const char muchsync_tmpdir[] = MUCHSYNC_DEFDIR "/tmp";

bool opt_fullscan;
bool opt_noscan;
bool opt_init;
bool opt_server;
int opt_verbose;
string opt_ssh = "ssh -CTaxq";
string opt_remote_muchsync_path = "muchsync";
string opt_notmuch_config;
string opt_init_dest;

unordered_set<string> new_tags;

const char schema_def[] = R"(
-- General table
CREATE TABLE configuration (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT);
CREATE TABLE sync_vector (
  replica INTEGER PRIMARY KEY,
  version INTEGER);

-- Shadow copy of the Xapian database to detect changes
CREATE TABLE tags (
  tag TEXT NOT NULL,
  docid INTEGER NOT NULL,
  UNIQUE (docid, tag),
  UNIQUE (tag, docid));
CREATE TABLE message_ids (
  message_id TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY,
  replica INTEGER,
  version INTEGER);
CREATE INDEX message_ids_writestamp ON message_ids (replica, version);
CREATE TABLE xapian_files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir_docid INTEGER,
  name TEXT NOT NULL,
  docid INTEGER,
  UNIQUE (dir_docid, name));

-- State from last scan of maildir, to detect changes
CREATE TABLE maildir_dirs (
  dir_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir_path TEXT UNIQUE NOT NULL,
  ctime REAL,
  mtime REAL,
  inode INTEGER,
  dir_docid INTEGER);
CREATE TABLE maildir_files (
  dir_id INTEGER NOT NULL,
  name TEXT NOT NULL COLLATE BINARY,
  mtime REAL,
  inode INTEGER,
  hash_id INTEGER NOT NULL,
  PRIMARY KEY (dir_id, name));
CREATE INDEX maildir_dir_hash_index ON maildir_files (dir_id, hash_id);
CREATE INDEX maildir_hash_dir_index ON maildir_files (hash_id, dir_id);
-- poor man's foreign key
CREATE TRIGGER dir_delete_trigger AFTER DELETE ON maildir_dirs
  BEGIN DELETE FROM maildir_files WHERE dir_id = old.dir_id;
  END;
CREATE TABLE maildir_hashes (
  hash_id INTEGER PRIMARY KEY,
  hash TEXT UNIQUE NOT NULL,
  size INTEGER,
  message_id TEXT,
  replica INTEGER,
  version INTEGER);
CREATE INDEX maildir_hashes_message_id ON maildir_hashes (message_id);
CREATE INDEX maildir_hashes_writestamp ON maildir_hashes (replica, version);
CREATE TABLE maildir_links (
  hash_id INTEGER NOT NULL,
  dir_id INTEGER NOT NULL,
  link_count INTEGER,
  PRIMARY KEY (hash_id, dir_id));
)";

const char xapian_dirs_def[] =
R"(CREATE TABLE xapian_dirs (
  dir_path TEXT UNIQUE NOT NULL,
  dir_docid INTEGER PRIMARY KEY);)";

static double
time_stamp ()
{
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  return ts_to_double (ts);
}

static double start_time_stamp {time_stamp()};
static double last_time_stamp {start_time_stamp};

void
print_time (string msg)
{
  double now = time_stamp();
  if (opt_verbose > 0) {
    auto oldFlags = cerr.flags();
    cerr.setf (ios::fixed, ios::floatfield);
    if (opt_server)
      msg = "[SERVER] " + msg;
    cerr << msg << "... " << now - start_time_stamp
	 << " (+" << now - last_time_stamp << ")\n";
    cerr.flags (oldFlags);
  }
  last_time_stamp = now;
}

sqlite3 *
dbcreate (const char *path)
{
  i64 self = 0;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1
      || self == 0) {
    cerr << "RAND_pseudo_bytes failed\n";
    return nullptr;
  }
  self &= ~(i64 (1) << 63);

  sqlite3 *db = nullptr;
  int err = sqlite3_open_v2 (path, &db,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, nullptr);
  if (err) {
    cerr << path << ": " << sqlite3_errstr (err);
    return nullptr;
  }
  sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");

  try {
    sqlexec (db, "BEGIN;");
    sqlexec (db, schema_def);
    sqlexec (db, xapian_dirs_def);
    setconfig (db, "dbvers", dbvers);
    setconfig (db, "self", self);
    setconfig (db, "last_scan", 0.0);
    sqlexec (db, "INSERT INTO sync_vector (replica, version)"
	     " VALUES (%lld, 0);", self);
    sqlexec (db, "COMMIT;");
  } catch (sqlerr_t exc) {
    sqlite3_close_v2 (db);
    cerr << exc.what () << '\n';
    return nullptr;
  }
  return db;
}

bool
muchsync_init (const string &maildir, bool create = false)
{
  string trashbase = maildir + muchsync_trashdir + "/";
  if (!access ((maildir + muchsync_tmpdir).c_str(), 0)
      && !access ((trashbase + "ff").c_str(), 0))
    return true;

  if (create && mkdir (maildir.c_str(), 0777) && errno != EEXIST) {
    perror (maildir.c_str());
    return false;
  }

  string notmuchdir = maildir + "/.notmuch";
  if (create && access (notmuchdir.c_str(), 0) && errno == ENOENT) {
    notmuch_database_t *notmuch;
    if (!notmuch_database_create (maildir.c_str(), &notmuch))
      notmuch_database_destroy (notmuch);
  }

  string msdir = maildir + muchsync_defdir;
  for (string d : {msdir, maildir + muchsync_trashdir,
	maildir + muchsync_tmpdir}) {
    if (mkdir (d.c_str(), 0777) && errno != EEXIST) {
      perror (d.c_str());
      return false;
    }
  }

  for (int i = 0; i < 0x100; i++) {
    ostringstream os;
    os << trashbase << hex << setfill('0') << setw(2) << i;
    if (mkdir (os.str().c_str(), 0777) && errno != EEXIST) {
      perror (os.str().c_str());
      return false;
    }
  }
  return true;
}

sqlite3 *
dbopen (const char *path)
{
  sqlite3 *db = nullptr;
  if (access (path, 0) && errno == ENOENT)
    db = dbcreate (path);
  else {
    sqlite3_open_v2 (path, &db, SQLITE_OPEN_READWRITE, nullptr);
    sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");
  }
  if (!db)
    return nullptr;

  sqlexec (db, "PRAGMA secure_delete = 0;");

  try {
    if (getconfig<string> (db, "dbvers") != dbvers) {
      cerr << path << ": invalid database version\n";
      sqlite3_close_v2 (db);
      return nullptr;
    }
    getconfig<i64> (db, "self");
  }
  catch (sqldone_t) {
    cerr << path << ": invalid configuration\n";
    sqlite3_close_v2 (db);
    return nullptr;
  }
  catch (sqlerr_t &e) {
    cerr << path << ": " << e.what() << '\n';
    sqlite3_close_v2 (db);
    return nullptr;
  }

  return db;
}

versvector
get_sync_vector (sqlite3 *db)
{
  versvector vv;
  sqlstmt_t s (db, "SELECT replica, version FROM sync_vector;");
  while (s.step().row())
    vv.emplace (s.integer(0), s.integer(1));
  return vv;
}

string
show_sync_vector (const versvector &vv)
{
  ostringstream sb;
  sb << '<';
  bool first = true;
  for (auto ws : vv) {
    if (first)
      first = false;
    else
      sb << ",";
    sb << 'R' << ws.first << '=' << ws.second;
  }
  sb << '>';
  return sb.str();
}

istream &
read_writestamp (istream &in, writestamp &ws)
{
  input_match (in, 'R');
  in >> ws.first;
  input_match (in, '=');
  in >> ws.second;
  return in;
}

istream &
read_sync_vector (istream &in, versvector &vv)
{
  input_match (in, '<');
  vv.clear();
  for (;;) {
    char c;
    if ((in >> c) && c == '>')
      return in;
    in.unget();
    writestamp ws;
    if (!read_writestamp (in, ws))
      break;
    vv.insert (ws);
    if (!(in >> c) || c == '>')
      break;
    if (c != ',') {
      in.setstate (ios_base::failbit);
      break;
    }
  }
  return in;
}

void
sync_local_data (sqlite3 *sqldb, const string &maildir)
{
  print_time ("synchronizing muchsync database with Xapian");
  sqlexec (sqldb, "SAVEPOINT localsync;");

  try {
    i64 self = getconfig<i64>(sqldb, "self");
    sqlexec (sqldb, "UPDATE sync_vector"
	     " SET version = version + 1 WHERE replica = %lld;", self);
    if (sqlite3_changes (sqldb) != 1)
      throw runtime_error ("My replica id (" + to_string (self)
			   + ") not in sync vector");
    versvector vv = get_sync_vector (sqldb);
    i64 vers = vv.at(self);
    writestamp ws { self, vers };

    xapian_scan (sqldb, ws, maildir);
    scan_maildir (sqldb, ws, maildir);
  }
  catch (...) {
    sqlexec (sqldb, "ROLLBACK TO localsync;");
    throw;
  }
  sqlexec (sqldb, "RELEASE localsync;");
  print_time ("finished synchronizing muchsync database with Xapian");
}

string
notmuch_maildir_location()
{
  string loc = cmd_output ("notmuch config get database.path");
  while (loc.size() > 0 && (loc.back() == '\n' || loc.back() == '\r'))
    loc.resize(loc.size()-1);
  struct stat sb;
  if (!loc.size() || stat(loc.c_str(), &sb) || !S_ISDIR(sb.st_mode))
    throw runtime_error("cannot find location of default maildir");
  return loc;
}

unordered_set<string>
notmuch_new_tags()
{
  istringstream is (cmd_output ("notmuch config get new.tags"));
  string line;
  unordered_set<string> ret;
  while (getline (is, line))
    ret.insert(line);
  return ret;
}

static string
get_notmuch_config()
{
  char *p = getenv("NOTMUCH_CONFIG");
  if (p && *p)
    return p;
  p = getenv("HOME");
  if (p && *p)
    return string(p) + "/.notmuch-config";
  cerr << "Cannot find HOME directory\n";
  exit(1);
}

[[noreturn]] void
usage ()
{
  cerr
    << "usage: muchsync [-FMXv] [-m maildir] [-s ssh] [-n tag [-n tag ...]]\n"
    << "                        [-r /path/to/muchsync] [server[:maildir]]\n"
    << "       muchsync --server [--nosync] [maildir]\n";
  exit (1);
}

static void
server ()
{
  ifdinfinistream ibin(0);
  cleanup _fixbuf ([](streambuf *sb){ cin.rdbuf(sb); },
		   cin.rdbuf(ibin.rdbuf()));

  string maildir;
  try { maildir = notmuch_maildir_location(); }
  catch (exception e) { cerr << e.what() << '\n'; exit (1); }
  string dbpath = maildir + muchsync_dbpath;

  if (!muchsync_init (maildir))
    exit (1);
  new_tags = notmuch_new_tags();
  sqlite3 *db = dbopen(dbpath.c_str());
  if (!db)
    exit(1);
  cleanup _c (sqlite3_close_v2, db);

  try {
    if (!opt_noscan)
      sync_local_data(db, maildir);
    muchsync_server(db, maildir);
  }
  catch (const exception &e) {
    cerr << e.what() << '\n';
    exit(1);
  }
}

static void
client(int ac, char **av)
{
  new_tags = notmuch_new_tags();

  struct stat sb;
  int err = stat(opt_notmuch_config.c_str(), &sb);
  if (opt_init) {
    if (!err) {
      cerr << opt_notmuch_config << " should not exist with --init option\n";
      exit (1);
    }
    else if (errno != ENOENT) {
      cerr << opt_notmuch_config << ": " << strerror(errno) << '\n';
      exit (1);
    }
  }
  else if (err) {
    cerr << opt_notmuch_config << ": " << strerror(errno) << '\n';
    exit (1);
  }

  string maildir;
  try { maildir = notmuch_maildir_location(); }
  catch (exception e) { cerr << e.what() << '\n'; exit (1); }
  string dbpath = maildir + muchsync_dbpath;

  if (ac == 0) {
    if (opt_init)
      usage();
    sqlite3 *db = dbopen(dbpath.c_str());
    if (!db)
      exit (1);
    cleanup _c (sqlite3_close_v2, db);
    sync_local_data (db, notmuch_maildir_location());
    exit(0);
  }

  if (!muchsync_init(maildir, true))
    exit (1);
  sqlite3 *db = dbopen(dbpath.c_str());
  if (!db)
    exit (1);
  cleanup _c (sqlite3_close_v2, db);

#if 0
  try {
#endif
    if (ac > 0)
      muchsync_client (db, maildir, ac, av);
    else
      sync_local_data (db, maildir);
#if 0
  }
  catch (const exception &e) {
    cerr << e.what() << '\n';
    exit (1);
  }
#endif
}

enum opttag {
  OPT_VERSION = 0x100,
  OPT_SERVER,
  OPT_NOSCAN,
  OPT_INIT
};

static const struct option muchsync_options[] = {
  { "version", no_argument, nullptr, OPT_VERSION },
  { "server", no_argument, nullptr, OPT_SERVER },
  { "noscan", no_argument, nullptr, OPT_NOSCAN },
  { "init", required_argument, nullptr, OPT_INIT },
  { "config", required_argument, nullptr, 'C' },
  { nullptr, 0, nullptr, 0 }
};

int
main (int argc, char **argv)
{
  umask (077);

  opt_notmuch_config = get_notmuch_config();

  int opt;
  while ((opt = getopt_long(argc, argv, "+C:Fr:s:v",
			    muchsync_options, nullptr)) != -1)
    switch (opt) {
    case 0:
      break;
    case 'C':
      opt_notmuch_config = optarg;
      setenv("NOTMUCH_CONFIG", optarg, 1);
      break;
    case 'F':
      opt_fullscan = true;
      break;
    case 'r':
      opt_remote_muchsync_path = optarg;
      break;
    case 's':
      opt_ssh = optarg;
      break;
    case 'v':
      opt_verbose++;
      break;
    case OPT_VERSION:
      cout << PACKAGE_STRING << '\n';
      exit (0);
    case OPT_SERVER:
      opt_server = true;
      break;
    case OPT_NOSCAN:
      opt_noscan = true;
      break;
    case OPT_INIT:
      opt_init = true;
      opt_init_dest = optarg;
      break;
    default:
      usage ();
    }

  if (opt_server) {
    if (opt_init || optind != argc)
      usage();
    server();
  }
  else
    client(argc - optind, argv + optind);
  return 0;
}

