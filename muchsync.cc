
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <openssl/rand.h>
#include "muchsync.h"

using namespace std;

#define DBVERS "muchsync 0"

sqlite3 *
dbcreate (const char *path)
{
  static const char extra_defs[] = "\
CREATE TABLE sync_vector (replica INTEGER PRIMARY KEY, version INTEGER);\n\
CREATE TABLE configuration (key TEXT PRIMARY KEY NOT NULL, value TEXT);";

  i64 self;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1) {
    fprintf (stderr, "RAND_pseudo_bytes failed\n");
    return NULL;
  }
  self &= ~((i64) 1 << 63);

  sqlite3 *pDb = NULL;
  int err = sqlite3_open_v2 (path, &pDb,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
  if (err) {
    fprintf (stderr, "%s: %s\n", path, sqlite3_errstr (err));
    return NULL;
  }

  try {
    fmtexec (pDb, "BEGIN;");
    fmtexec (pDb, extra_defs);
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

  printf ("self = %lld\n", getconfig<i64>(db, "self"));

  fmtexec (db, "BEGIN;");
  try {
    // scan_xapian (db, argv[2]);
    //scan_notmuch (db, argv[2]);
    scan_maildir (db, argv[2]);
    fmtexec(db, "COMMIT;"); // see what happened
  }
  catch (std::runtime_error e) {
    fprintf (stderr, "%s\n", e.what ());
    fmtexec(db, "COMMIT;"); // XXX - see what happened
    exit (1);
  }
  sqlite3_close_v2 (db);

  return 0;
}
