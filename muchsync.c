
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <notmuch.h>
#include <sqlite3.h>
#include <talloc.h>

sqlite3 *
dbcreate (const char *path)
{
  sqlite3 *pDb = NULL;
  sqlite3_open_v2 (path, &pDb, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
  if (!pDb)
    return NULL;
  char *errmsg = NULL;
  int err = sqlite3_exec (pDb, "\
CREATE TABLE messages (message_id TEXT UNIQUE NOT NULL, \
                       tags TEXT, \
                       writer INTEGER, \
                       writer_versioin INTEGER, \
                       creator INTEGER, \
                       creator_version INTEGER); \
CREATE TABLE files (message_id TEXT, \
                    path TEXT UNIQUE NOT NULL, \
                    size INTEGER, \
                    sha1 BLOB, \
                    mtime REAL, \
                    ctime REAL); \
CREATE TABLE sync_vector (replica INTEGER PRIMARY, \
                          version INTEGER); \
CREATE TABLE configuration (key TEXT UNIQUE NOT NULL, value);",
			  NULL, NULL, &errmsg);
  if (err != SQLITE_OK) {
    fprintf (stderr, "%s: %s\n", path, errmsg);
    sqlite3_close_v2 (pDb);
    return NULL;
  }
  return pDb;
}

sqlite3 *
dbopen (const char *path)
{
  if (access (path, 0) && errno == ENOENT)
    return dbcreate (path);
  sqlite3 *pDb = NULL;
  sqlite3_open_v2 (path, &pDb, SQLITE_OPEN_READWRITE, NULL);
  return pDb;
}

int
main (int argc, char **argv)
{
  if (argc != 2) {
    fprintf (stderr, "usage error\n");
    exit (1);
  }

  sqlite3 *db = dbopen (argv[1]);
  if (!db)
    exit (1);

  return 0;
}
