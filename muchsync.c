
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include <notmuch.h>
#include <openssl/rand.h>
#include <sqlite3.h>
#include <talloc.h>

#define DBVERS "muchsync 0"

typedef sqlite3_int64 i64;

void
dbperror (sqlite3 *db, const char *query)
{
  const char *dbpath = sqlite3_db_filename (db, "main");
  if (!dbpath)
    dbpath = "sqlite3 database";
  if (query)
    fprintf (stderr, "%s:\n  Query: %s\n  Error: %s\n",
	     dbpath, query, sqlite3_errmsg (db));
  else
    fprintf (stderr, "%s: %s\n", dbpath, sqlite3_errmsg (db));
}

int
fmtexec (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  va_list ap;
  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    return SQLITE_NOMEM;
  int err = sqlite3_exec (db, query, NULL, NULL, NULL);
  if (err)
    dbperror (db, query);
  sqlite3_free (query);
  return err;
}

int
fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...)
{
  char *query;
  const char *tail;
  sqlite3_stmt *stmtp = NULL;
  int err;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query) {
    if (stmtpp)
      *stmtpp = NULL;
    fprintf (stderr, "sqlite3_vmprintf: out of memory\n");
    return SQLITE_NOMEM;
  }

  err = sqlite3_prepare_v2 (db, query, -1, &stmtp, &tail);
  if (!err && tail && *tail) {
    fprintf (stderr, "fmtstep: illegal compound query\n  Query: %s\n", query);
    abort ();
  }
  if (!err)
    err = sqlite3_step (stmtp);

  if (err != SQLITE_ROW) {
    sqlite3_finalize (stmtp);
    stmtp = NULL;
  }
  if (stmtpp)
    *stmtpp = stmtp;

  if (err != SQLITE_OK && err != SQLITE_ROW && err != SQLITE_DONE)
    dbperror (db, query);
  sqlite3_free (query);
  return err;
}

char *
getconfig_text (void *ctx, sqlite3 *db, const char *key)
{
  sqlite3_stmt *pStmt;
  if (fmtstep (db, &pStmt,
	       "SELECT value FROM configuration WHERE key = %Q;", key)
      != SQLITE_ROW)
    return NULL;
  const char *value = (const char *) sqlite3_column_text (pStmt, 0);
  char *ret = value ? talloc_strdup (ctx, value) : NULL;
  sqlite3_finalize (pStmt);
  return ret;
}

int
getconfig_int64 (sqlite3 *db, const char *key, i64 *valp)
{
  sqlite3_stmt *pStmt;
  int err = fmtstep (db, &pStmt,
		     "SELECT value FROM configuration WHERE key = %Q;", key);
  if (err == SQLITE_ROW) {
    *valp = sqlite3_column_int64 (pStmt, 0);
    sqlite3_finalize (pStmt);
    return SQLITE_OK;
  }
  assert (err != SQLITE_OK); /* should be ROW or DONE if no error */
  return err;
}

/* type should be Q for strings, lld for i64s, f for double */
#define setconfig_type(db, key, type, val)				\
  fmtexec(db,							\
	  "INSERT OR REPLACE INTO configuration (key, value)"	\
          "VALUES (%Q, %" #type ");", key, val)
#define setconfig_int64(db, key, val) setconfig_type(db, key, lld, val)
#define setconfig_text(db, key, val) setconfig_type(db, key, Q, val)

sqlite3 *
dbcreate (const char *path)
{
  static const char table_defs[] = "\
CREATE TABLE messages (message_id TEXT UNIQUE NOT NULL,\
 tags TEXT,\
 writer INTEGER,\
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

  if (fmtexec (pDb, "BEGIN;")
      || fmtexec (pDb, table_defs)
      || setconfig_text (pDb, "DBVERS", DBVERS)
      || setconfig_int64 (pDb, "self", self)
      || fmtexec (pDb, "INSERT INTO sync_vector (replica, version)"
		  " VALUES (%lld, 0);", self)
      || fmtexec (pDb, "COMMIT;")) {
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
scan_notmuch (const char *mailpath, sqlite3 *db)
{
  notmuch_database_t *notmuch;
  notmuch_status_t err;

  if (sqlite3_exec (db,
		    "CREATE TEMP TABLE old_files AS SELECT path FROM files;",
		    NULL, NULL, NULL))
    return -1;

  err = notmuch_database_open (mailpath, NOTMUCH_DATABASE_MODE_READ_ONLY,
			       &notmuch);
  if (err)
    return -1;

  int pathprefixlen = strlen (mailpath) + 1;
  notmuch_query_t *query = notmuch_query_create (notmuch, "");
  notmuch_query_set_omit_excluded (query, NOTMUCH_EXCLUDE_FALSE);
  notmuch_query_set_sort (query, NOTMUCH_SORT_UNSORTED);
  notmuch_messages_t *messages = notmuch_query_search_messages (query);
  while (notmuch_messages_valid (messages)) {
    notmuch_message_t *message = notmuch_messages_get (messages);


    const char *message_id = notmuch_message_get_message_id (message);
    notmuch_filenames_t *pathiter = notmuch_message_get_filenames (message);
    while (notmuch_filenames_valid (pathiter)) {
      const char *path = notmuch_filenames_get (pathiter);

      printf ("%s %s\n", message_id, path + pathprefixlen);

      notmuch_filenames_move_to_next (pathiter);
    }



    notmuch_message_destroy (message);
    notmuch_messages_move_to_next (messages);
  }

  notmuch_database_destroy (notmuch);
  return 0;
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
  sqlite3_exec (db, "BEGIN;", NULL, NULL, NULL);
  scan_notmuch (argv[2], db);
  sqlite3_exec (db, "COMMIT;", NULL, NULL, NULL);

  return 0;
}
