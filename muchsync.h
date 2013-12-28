#include <cassert>
#include <memory>
#include <string>
#include <sqlite3.h>
#include <notmuch.h>

/* sqlite.cc */
using i64 = sqlite3_int64;

class _sqlcolval_t {
  sqlite3_stmt *stmt_;
  int iCol_;
 public:
  _sqlcolval_t (sqlite3_stmt *stmt, int iCol) : stmt_ (stmt), iCol_ (iCol) {}
  _sqlcolval_t (_sqlcolval_t &&) = default;
  explicit operator bool () {
    return sqlite3_column_type (stmt_, iCol_) != SQLITE_NULL;
  }
  operator i64 () { return sqlite3_column_int64 (stmt_, iCol_); }
  operator double () { return sqlite3_column_double (stmt_, iCol_); }
  operator std::string () {
    return { static_cast<const char *> (sqlite3_column_blob (stmt_, iCol_)),
	size_t (sqlite3_column_bytes (stmt_, iCol_)) };
  }
};

class sqlstmt_t {
  sqlite3_stmt *stmt_;
  int status_ = SQLITE_OK;
  bool set_status (int status);
 public:
  explicit sqlstmt_t(sqlite3_stmt *stmt) : stmt_ (stmt) {}
  sqlstmt_t(sqlstmt_t &&r) : stmt_ (r.stmt_) { r.stmt_ = nullptr; }
  ~sqlstmt_t() { sqlite3_finalize (stmt_); }

  sqlite3_stmt *get() { return stmt_; }
  explicit operator bool() { return stmt_; }
  int status() const { return status_; }
  bool step() { return set_status(sqlite3_step (stmt_)); }
  bool reset() { return set_status(sqlite3_reset (stmt_)); }
  _sqlcolval_t operator[] (int iCol) {
    assert (status_ == SQLITE_ROW);
    return { stmt_, iCol };
  }

  bool bind(int i, std::nullptr_t) {
    return set_status (sqlite3_bind_null(stmt_, i));
  }
  bool bind(int i, i64 v) {
    return set_status (sqlite3_bind_int64(stmt_, i, v));
  }
  bool bind(int i, double v) {
    return set_status (sqlite3_bind_double(stmt_, i, v));
  }
  bool bind(int i, std::string v) {
    return set_status (sqlite3_bind_blob(stmt_, i,
					 v.c_str(), v.size(), SQLITE_STATIC));
  }
};

void dbperror (sqlite3 *db, const char *query);
int fmtexec (sqlite3 *db, const char *fmt, ...);
sqlstmt_t fmtstmt (sqlite3 *db, const char *fmt, ...);
int fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...);

sqlite3 *dbopen (const char *path);
int scan_notmuch (const char *mailpath, sqlite3 *db);

/* notmuch.cc */
std::string message_tags (notmuch_message_t *message);
bool scan_message_ids (sqlite3 *sqldb, const std::string &path);
