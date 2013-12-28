#include <cassert>
#include <memory>
#include <string>
#include <sqlite3.h>
#include <notmuch.h>

using std::string;

/* sqlite.cc */
using sqldb_t = std::unique_ptr<sqlite3, decltype (&sqlite3_close)>;
using i64 = sqlite3_int64;

struct sqlerr_t : public std::runtime_error {
  sqlerr_t (const string &msg) : std::runtime_error (msg) {}
};

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
  operator string () {
    return { static_cast<const char *> (sqlite3_column_blob (stmt_, iCol_)),
	size_t (sqlite3_column_bytes (stmt_, iCol_)) };
  }
};

class sqlstmt_t {
  sqlite3_stmt *stmt_;
  int status_ = SQLITE_OK;
  void set_status (int status);
 public:
  explicit sqlstmt_t(sqlite3_stmt *stmt) : stmt_ (stmt) {}
  explicit sqlstmt_t(sqlite3 *db, const char *fmt, ...);
  explicit sqlstmt_t(const sqldb_t &db, const char *fmt, ...);
  sqlstmt_t(sqlstmt_t &&r) : stmt_ (r.stmt_) { r.stmt_ = nullptr; }
  ~sqlstmt_t() { sqlite3_finalize (stmt_); }

  sqlite3_stmt *get() { return stmt_; }
  int status() const { return status_; }
  bool row() { return status_ == SQLITE_ROW; }
  void step() { set_status(sqlite3_step (stmt_)); }
  void reset() { set_status(sqlite3_reset (stmt_)); }
  _sqlcolval_t operator[] (int iCol) {
    assert (status_ == SQLITE_ROW);
    return { stmt_, iCol };
  }

  void bind(int i, std::nullptr_t) { set_status (sqlite3_bind_null(stmt_, i)); }
  void bind(int i, i64 v) { set_status (sqlite3_bind_int64(stmt_, i, v)); }
  void bind(int i, unsigned v) { set_status (sqlite3_bind_int64(stmt_, i, v)); }
  void bind(int i, double v) { set_status (sqlite3_bind_double(stmt_, i, v)); }
  void bind(int i, string v) {
    set_status (sqlite3_bind_blob(stmt_, i,
				  v.c_str(), v.size(), SQLITE_STATIC));
  }
};

void dbthrow (sqlite3 *db, const char *query);
int fmtexec (sqlite3 *db, const char *fmt, ...);
sqlstmt_t fmtstmt (sqlite3 *db, const char *fmt, ...);
int fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...);

sqlite3 *dbopen (const char *path);

/* notmuch.cc */
string message_tags (notmuch_message_t *message);
void scan_xapian (sqlite3 *sqldb, const string &path);
void scan_notmuch (sqlite3 *db, const string &path);
