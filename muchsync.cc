
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <openssl/rand.h>
#include "muchsync.h"

using namespace std;

bool opt_fullscan;
int opt_verbose;

#define DBVERS "muchsync 0"

const char schema_def[] =
R"(CREATE TABLE configuration (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT);
CREATE TABLE sync_vector (
  replica INTEGER PRIMARY KEY,
  version INTEGER);

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
CREATE TABLE xapian_files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir_docid INTEGER,
  name TEXT NOT NULL,
  docid INTEGER,
  UNIQUE (dir_docid, name));

CREATE TABLE maildir_dirs (
  dir_id INTEGER PRIMARY KEY AUTOINCREMENT,
  dir_path TEXT UNIQUE NOT NULL,
  ctime REAL,
  mtime REAL,
  inode INTEGER);
CREATE TABLE maildir_files (
  dir_id INTEGER NOT NULL,
  name TEXT NOT NULL COLLATE BINARY,
  mtime REAL,
  inode INTEGER,
  size INTEGER,
  hash_id INTEGER NOT NULL,
  PRIMARY KEY (dir_id, name));
CREATE INDEX dir_hash_index ON maildir_files (dir_id, hash_id);
-- poor man's foreign key
CREATE TRIGGER dir_delete_trigger AFTER DELETE ON maildir_dirs
  BEGIN DELETE FROM maildir_files WHERE dir_id = old.dir_id;
  END;
CREATE TABLE maildir_hashes (
  hash_id INTEGER PRIMARY KEY,
  hash TEXT UNIQUE NOT NULL,
  replica INTEGER,
  version INTEGER);
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
  cout << msg << "... " << now - start_time_stamp
       << " (+" << now - last_time_stamp << ")\n";
  last_time_stamp = now;
}

sqlite3 *
dbcreate (const char *path)
{
  i64 self;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1) {
    fprintf (stderr, "RAND_pseudo_bytes failed\n");
    return NULL;
  }
  self &= ~(i64 (1) << 63);

  sqlite3 *pDb = NULL;
  int err = sqlite3_open_v2 (path, &pDb,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
  if (err) {
    fprintf (stderr, "%s: %s\n", path, sqlite3_errstr (err));
    return NULL;
  }

  try {
    fmtexec (pDb, "BEGIN;");
    fmtexec (pDb, schema_def);
    fmtexec (pDb, xapian_dirs_def);
    setconfig (pDb, "DBVERS", DBVERS);
    setconfig (pDb, "self", self);
    setconfig (pDb, "last_scan", 0.0);
    fmtexec (pDb, "INSERT INTO sync_vector (replica, version)"
	     " VALUES (%lld, 0);", self);
    fmtexec (pDb, "COMMIT;");
  } catch (sqlerr_t exc) {
    sqlite3_close_v2 (pDb);
    cerr << exc.what () << '\n';
    return NULL;
  }
  return pDb;
}

sqlite3 *
dbopen (const char *path)
{
  sqlite3 *pDb = NULL;
  if (access (path, 0) && errno == ENOENT)
    pDb = dbcreate (path);
  else
    sqlite3_open_v2 (path, &pDb, SQLITE_OPEN_READWRITE, NULL);
  if (!pDb)
    return NULL;

  fmtexec (pDb, "PRAGMA secure_delete = 0;");

  try {
    if (getconfig<string> (pDb, "DBVERS") != DBVERS) {
      cerr << path << ": invalid database version (" << DBVERS << ")\n";
      sqlite3_close_v2 (pDb);
      return NULL;
    }
    getconfig<i64> (pDb, "self");
  }
  catch (sqldone_t) {
    cerr << path << ": invalid configuration\n";
    sqlite3_close_v2 (pDb);
    return NULL;
  }
  catch (sqlerr_t &e) {
    cerr << path << ": " << e.what() << '\n';
    sqlite3_close_v2 (pDb);
    return NULL;
  }

  return pDb;
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
      sb << ", ";
    sb << 'R' << ws.first << '=' << ws.second;
  }
  sb << '>';
  return sb.str();
}

[[noreturn]] void
usage ()
{
  fprintf (stderr, "usage: muchsync [-F] db maildir\n");
  exit (1);
}

int
main (int argc, char **argv)
{
  bool opt_maildir_only = false, opt_xapian_only = false;
  int opt;
  while ((opt = getopt(argc, argv, "Fmvx")) != -1)
    switch (opt) {
    case 'F':
      opt_fullscan = true;
      break;
    case 'm':
      opt_maildir_only = true;
      break;
    case 'v':
      opt_verbose++;
      break;
    case 'x':
      opt_xapian_only = true;
      break;
    default:
      usage ();
    }
  
  if (optind + 2 != argc)
    usage ();

  sqlite3 *db = dbopen (argv[optind]);
  if (!db)
    exit (1);
  cleanup _c (sqlite3_close_v2, db);
  string maildir{argv[optind+1]};

  i64 self = getconfig<i64>(db, "self");
  fmtexec (db, "BEGIN IMMEDIATE;");
  fmtexec (db, "UPDATE sync_vector"
	   " SET version = version + 1 WHERE replica = %lld;",
	   self);
  if (sqlite3_changes (db) != 1) {
    cerr << "My replica id (" << self << ") not in sync vector\n";
    return 1;
  }
  versvector vv = get_sync_vector (db);
  i64 vers = vv.at(self);
  writestamp ws { self, vers };

  printf ("self = %lld\n", self);
  printf ("version = %lld\n", vers);
  printf ("sync_vector = %s\n", show_sync_vector(vv).c_str());

  try {
    double start_scan_time { time_stamp() };

    if (!opt_xapian_only)
      scan_maildir (db, ws, maildir);
    if (!opt_maildir_only)
      xapian_scan (db, ws, maildir);

    setconfig (db, "last_scan", start_scan_time);
    fmtexec(db, "COMMIT;");
  }
  catch (std::runtime_error e) {
    fprintf (stderr, "%s\n", e.what ());
    fmtexec(db, "COMMIT;"); // XXX - see what happened
    exit (1);
  }

  return 0;
}
