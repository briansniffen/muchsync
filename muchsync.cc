
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <openssl/rand.h>
#include "muchsync.h"

using namespace std;

#define DBVERS "muchsync 0"

const char schema_def[] =
R"(CREATE TABLE configuration (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT);
CREATE TABLE sync_vector (
  replica INTEGER PRIMARY KEY,
  version INTEGER);
CREATE TABLE messages (
  docid INTEGER UNIQUE,
  message_id TEXT UNIQUE NOT NULL,
  tags TEXT,
  replica INTEGER,
  version INTEGER);
CREATE TABLE hashes (
  hash TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  docid INTEGER,
  replica INTEGER,
  version INTEGER,
  create_replica INEGER,
  create_version INTEGER);
CREATE TABLE files (
  docid INTEGER,
  dir_docid INTEGER,
  path TEXT PRIMARY KEY,
  inode INT,
  mtime REAL,
  size INT,
  hash TEXT,
  replica INTEGER,
  version INTEGER);)";

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
    setconfig (pDb, "DBVERS", DBVERS);
    setconfig (pDb, "self", self);
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

int
main (int argc, char **argv)
{
  if (argc != 3) {
    fprintf (stderr, "usage error\n");
    exit (1);
  }

  sqlite3 *db = dbopen (argv[1]);
  if (!db)
    exit (1);
  cleanup _c (sqlite3_close_v2, db);


  i64 self = getconfig<i64>(db, "self");
  fmtexec (db, "BEGIN;");
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
    xapian_scan (db, ws, argv[2]);
    //scan_xapian (db, ws, argv[2]);

    //scan_notmuch (db, argv[2]);
    //scan_maildir (db, ws, argv[2]);
    fmtexec(db, "COMMIT;");
  }
  catch (std::runtime_error e) {
    fprintf (stderr, "%s\n", e.what ());
    fmtexec(db, "COMMIT;"); // XXX - see what happened
    exit (1);
  }

  return 0;
}
