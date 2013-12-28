#include <sqlite3.h>

/* sqlite.cc */
void dbperror (sqlite3 *db, const char *query);
int fmtexec (sqlite3 *db, const char *fmt, ...);
int fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...);
sqlite3 *dbopen (const char *path);
int scan_notmuch (const char *mailpath, sqlite3 *db);
