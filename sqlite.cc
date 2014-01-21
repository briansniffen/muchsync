
#include <iostream>
#include <sstream>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include <notmuch.h>
#include <sqlite3.h>

#include "muchsync.h"

using namespace std;

void
dbthrow (sqlite3 *db, const char *query)
{
  const char *dbpath = sqlite3_db_filename (db, "main");
  if (!dbpath)
    dbpath = "sqlite3 database";
  ostringstream errbuf;
  if (query)
    errbuf << dbpath << ":\n  Query: " << query
	   << "\n  Error: " << sqlite3_errmsg (db);
  else
    errbuf << dbpath << ": " << sqlite3_errmsg (db);
  throw sqlerr_t (errbuf.str ());
}

sqlstmt_t &
sqlstmt_t::set_status (int status)
{
  status_ = status;
  if (status != SQLITE_OK && status != SQLITE_ROW && status != SQLITE_DONE)
    dbthrow (sqlite3_db_handle (stmt_), nullptr);
  return *this;
}

void
sqlstmt_t::fail ()
{
  assert (status_ != SQLITE_OK);
  if (status_ == SQLITE_DONE)
    throw sqldone_t(string ("No rows left in query: ") + sqlite3_sql (stmt_));
  else
    throw sqlerr_t(string ("sqlstmt_t::operator[]: used after error\n"
			   "  Query: ") + sqlite3_sql (stmt_)
		   + "\n  Error: " + sqlite3_errstr(status_));
}

sqlstmt_t::sqlstmt_t (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  const char *tail;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory");
  cleanup _c (sqlite3_free, query);

  if (sqlite3_prepare_v2 (db, query, -1, &stmt_, &tail))
    dbthrow (db, query);
  if (tail && *tail) {
    cerr << "sqlstmt_t: illegal compound query\n  Query: " << query << '\n';
    abort ();
  }
}

void
sqlexec (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  va_list ap;
  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory in sqlexec");
  int err = sqlite3_exec (db, query, NULL, NULL, NULL);
  if (err != SQLITE_OK && err != SQLITE_DONE && err != SQLITE_ROW)
    dbthrow (db, query);
  sqlite3_free (query);
}

sqlstmt_t
fmtstmt (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  const char *tail;
  sqlite3_stmt *stmtp;
  va_list ap;

  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);
  if (!query) {
    cerr << "sqlite3_vmprintf(" << fmt << "): out of memory\n";
    return sqlstmt_t (nullptr);
  }

  if (sqlite3_prepare_v2 (db, query, -1, &stmtp, &tail)) {
    dbthrow (db, query);
    return sqlstmt_t (nullptr);
  }
  if (tail && *tail) {
    cerr << "fmtstmt: illegal compound query\n  Query: " << query << '\n';
    abort ();
  }
  return sqlstmt_t (stmtp);
}

void
save_old_table (sqlite3 *sqldb, const string &table, const char *create)
{
  sqlexec (sqldb, "%s", create);
  sqlexec (sqldb, "DROP TABLE IF EXISTS \"old_%s\";"
		  "ALTER TABLE \"%s\" RENAME TO \"old_%s\";",
	   table.c_str(), table.c_str(), table.c_str());
  sqlexec (sqldb, "%s", create);
}
