
#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include <notmuch.h>
#include <sqlite3.h>

#include "sqlstmt.h"

using namespace std;

static void
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
  va_list ap;
  va_start (ap, fmt);
  char *query = sqlite3_vmprintf (fmt, ap);
  va_end (ap);

  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory");
  unique_ptr<char, decltype(&sqlite3_free)> _c (query, sqlite3_free);

  const char *tail;
  if (sqlite3_prepare_v2 (db, query, -1, &stmt_, &tail))
    dbthrow (db, query);
  if (tail && *tail)
    throw sqlerr_t (string("illegal compound query\n  Query:  ") + query);
}

void
sqlexec (sqlite3 *db, const char *fmt, ...)
{
  char *query;
  va_list ap;
  va_start (ap, fmt);
  query = sqlite3_vmprintf (fmt, ap);
  unique_ptr<char, decltype(&sqlite3_free)> _c (query, sqlite3_free);
  va_end (ap);
  if (!query)
    throw sqlerr_t ("sqlite3_vmprintf: out of memory in sqlexec");
  int err = sqlite3_exec (db, query, NULL, NULL, NULL);
  if (err != SQLITE_OK && err != SQLITE_DONE && err != SQLITE_ROW)
    dbthrow (db, query);
}
