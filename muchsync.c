
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include <notmuch.h>
#include <openssl/rand.h>
#include <sqlite3.h>
#include <talloc.h>

#define DBVERS "muchsync 0"

char *
getconfig (void *ctx, sqlite3 *db, const char *key)
{
  sqlite3_stmt *pStmt;
  if(sqlite3_prepare_v2 (db,
			 "SELECT value FROM configuration WHERE key = ?;",
			 -1, &pStmt, NULL) != SQLITE_OK)
    return NULL;

  char *ret = NULL;
  if (sqlite3_bind_text (pStmt, 1, key, -1, SQLITE_STATIC) != SQLITE_OK
      || sqlite3_step (pStmt) != SQLITE_ROW)
    goto leave;
  const char *value = (const char *) sqlite3_column_text (pStmt, 0);
  if (value)
    ret = talloc_strdup (ctx, value);
 leave:
  sqlite3_finalize (pStmt);
  return ret;
}

int
getconfig_int64 (sqlite3 *db, const char *key, int64_t *valp)
{
  sqlite3_stmt *pStmt;
  int err;
  err = sqlite3_prepare_v2 (db,
			    "SELECT value FROM configuration WHERE key = ?;",
			    -1, &pStmt, NULL);
  if (err)
    return err;

  err = sqlite3_bind_text (pStmt, 1, key, -1, SQLITE_STATIC);
  if (!err)
    err = sqlite3_step (pStmt);
  if (err == SQLITE_ROW)
    err = SQLITE_OK;
  if (!err)
    *valp = sqlite3_column_int64 (pStmt, 0);
  sqlite3_finalize (pStmt);
  return err;
}

int
setconfig (sqlite3 *db, const char *key, const char *value)
{
  static const char query[] =
    "INSERT OR REPLACE INTO configuration (key, value) VALUES (?, ?);";
  sqlite3_stmt *pStmt;
  int err = sqlite3_prepare_v2 (db, query, -1, &pStmt, NULL);
  if (err)
    return err;

  err = sqlite3_bind_text (pStmt, 1, key, -1, SQLITE_STATIC);
  if (!err)
    err = sqlite3_bind_text (pStmt, 2, value, -1, SQLITE_STATIC);
  if (!err) {
    err = sqlite3_step (pStmt);
    if (err == SQLITE_DONE)
      err = SQLITE_OK;
  }
  sqlite3_finalize (pStmt);
  return err;
}

int
setconfig_int64 (sqlite3 *db, const char *key, int64_t value)
{
  static const char query[] =
    "INSERT OR REPLACE INTO configuration (key, value) VALUES (?, ?);";
  sqlite3_stmt *pStmt;
  int err = sqlite3_prepare_v2 (db, query, -1, &pStmt, NULL);
  if (err) {
    printf ("couldn't prepare %d\n", err);
    return err;
  }

  err = sqlite3_bind_text (pStmt, 1, key, -1, SQLITE_STATIC);
  if (!err)
    err = sqlite3_bind_int64 (pStmt, 2, value);
  if (!err) {
    err = sqlite3_step (pStmt);
    if (err == SQLITE_DONE)
      err = SQLITE_OK;
  }
  sqlite3_finalize (pStmt);
  return err;
}


sqlite3 *
dbcreate (const char *path)
{
  static const char table_defs[] = "\
CREATE TABLE messages (message_id TEXT UNIQUE NOT NULL,\
 tags TEXT,\
 writer INTEGER, \
 writer_versioin INTEGER,\
 creator INTEGER, \
 creator_version INTEGER);\n\
CREATE TABLE files (message_id TEXT,\
 path TEXT UNIQUE NOT NULL,\
 size INTEGER,\
 sha1 BLOB,\
 mtime REAL,\
 ctime REAL);\n\
CREATE TABLE sync_vector (replica INTEGER PRIMARY KEY, version INTEGER);\n\
CREATE TABLE configuration (key TEXT PRIMARY KEY NOT NULL, value TEXT);";

  sqlite3 *pDb = NULL;
  sqlite3_open_v2 (path, &pDb, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
  if (!pDb)
    return NULL;
  int err = 0;
  err = sqlite3_exec (pDb, "BEGIN;", NULL, NULL, NULL);
  if (!err)
    err = sqlite3_exec (pDb, table_defs, NULL, NULL, NULL);

  if (!err)
    err = setconfig (pDb, "DBVERS", DBVERS);

  int64_t self;
  if (!err) {
    if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1) {
      fprintf (stderr, "RAND_pseudo_bytes failed\n");
      sqlite3_close_v2 (pDb);
      return NULL;
    }
    err = setconfig_int64 (pDb, "self", self);
  }
  if (!err) {
    char *cmd = talloc_asprintf (NULL, "\
INSERT INTO sync_vector (replica, version) VALUES (%lld, 0);", 
				 (long long) self);
    err = sqlite3_exec (pDb, cmd, NULL, NULL, NULL);
    talloc_free (cmd);
  }

  if (!err)
    err = sqlite3_exec (pDb, "COMMIT;", NULL, NULL, NULL);
  if (err) {
    fprintf (stderr, "%s: %s\n", path, sqlite3_errmsg (pDb));
    sqlite3_close_v2 (pDb);
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
