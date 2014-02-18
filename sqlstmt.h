// -*- C++ -*-

#include <cassert>
#include <string>
#include <sqlite3.h>
#include <tuple>

using i64 = sqlite3_int64;

struct sqlerr_t : public std::runtime_error {
  sqlerr_t (const std::string &msg) : std::runtime_error (msg) {}
};
/* A sqldone_t is thrown if you ask for data when no rows are left */
struct sqldone_t : public std::runtime_error {
  sqldone_t (const std::string &msg) : std::runtime_error (msg) {}
};

class sqlstmt_t {
  sqlite3_stmt *stmt_;
  int status_ = SQLITE_OK;
  sqlstmt_t &set_status (int status);
  void fail ();
  void ensure_row () { if (status_ != SQLITE_ROW) fail(); }

 public:
  explicit sqlstmt_t(sqlite3_stmt *stmt) : stmt_(stmt) {}
  explicit sqlstmt_t(sqlite3 *db, const char *fmt, ...);
  sqlstmt_t(const sqlstmt_t &r);
  sqlstmt_t(sqlstmt_t &&r) : stmt_ (r.stmt_) { r.stmt_ = nullptr; }
  ~sqlstmt_t() { sqlite3_finalize (stmt_); }

  sqlite3_stmt *get() { return stmt_; }
  sqlite3 *getdb() { return sqlite3_db_handle(stmt_); }
  int status() const { return status_; }
  bool row() {
    if (status_ == SQLITE_ROW)
      return true;
    // Something like SQLITE_OK indicates row() not used after step()
    assert (status_ == SQLITE_DONE);
    return false;
  }
  bool done() { return !row(); }
  sqlstmt_t &step() { return set_status(sqlite3_step (stmt_)); }
  sqlstmt_t &reset() { return set_status(sqlite3_reset (stmt_)); }

  /* Access columns */
  template<typename T> T column(int);
  bool null(int i) {
    ensure_row();
    return sqlite3_column_type (stmt_, i) == SQLITE_NULL;
  }
  sqlite3_int64 integer(int i) {
    ensure_row();
    return sqlite3_column_int64 (stmt_, i);
  }
  double real(int i) {
    ensure_row();
    return sqlite3_column_double (stmt_, i);
  }
  std::string str(int i) {
    ensure_row();
    return { static_cast<const char *> (sqlite3_column_blob (stmt_, i)),
	size_t (sqlite3_column_bytes (stmt_, i)) };
  }
  const char *c_str(int i) {
    ensure_row();
    return reinterpret_cast<const char *> (sqlite3_column_text (stmt_, i));
  }
  sqlite3_value *value(int i) {
    ensure_row();
    return sqlite3_column_value(stmt_, i);
  }

  /* Bind parameters */
  sqlstmt_t &bind_null(int i) {
    return set_status (sqlite3_bind_null(stmt_, i));
  }
  sqlstmt_t &bind_int(int i, sqlite3_int64 v) {
    return set_status (sqlite3_bind_int64(stmt_, i, v));
  }
  sqlstmt_t &bind_real(int i, double v) {
    return set_status (sqlite3_bind_double(stmt_, i, v));
  }
  sqlstmt_t &bind_text(int i, const std::string &v) {
    return set_status (sqlite3_bind_text(stmt_, i, v.data(), v.size(),
					 SQLITE_STATIC));
  }
  sqlstmt_t &bind_text(int i, std::string &&v) {
    return set_status (sqlite3_bind_text(stmt_, i, v.data(), v.size(),
					 SQLITE_TRANSIENT));
  }
  sqlstmt_t &bind_text(int i, const char *p, int len = -1) {
    return set_status (sqlite3_bind_text(stmt_, i, p, len, SQLITE_STATIC));
  }
  sqlstmt_t &bind_blob(int i, const void *p, int len) {
    return set_status (sqlite3_bind_blob(stmt_, i, p, len, SQLITE_STATIC));
  }
  sqlstmt_t &bind_value(int i, const sqlite3_value *v) {
    return set_status (sqlite3_bind_value (stmt_, i, v));
  }

  /* Overloaded bind */
  sqlstmt_t &bind(int i, std::nullptr_t) { return bind_null(i); }
  sqlstmt_t &bind(int i, sqlite3_int64 v) { return bind_int(i, v); }
  sqlstmt_t &bind(int i, int v) { return bind_int(i, v); }
  sqlstmt_t &bind(int i, unsigned v) { return bind_int(i, v); }
  sqlstmt_t &bind(int i, const double &v) { return bind_real(i, v); }
  sqlstmt_t &bind(int i, const std::string &v) { return bind_text(i, v); }
  sqlstmt_t &bind(int i, std::string &&v) { return bind_text(i, std::move(v)); }
  sqlstmt_t &bind(int i, const char *v) { return bind_text(i, v); }
  sqlstmt_t &bind(int i, const sqlite3_value *v) { return bind_value(i, v); }

  /* Bind multiple parameters at once */
  sqlstmt_t &_param(int) { return *this; }
  template<typename H, typename... T>
  sqlstmt_t &_param(int i, H&& h, T&&... t) {
    return this->bind(i, std::forward<H>(h))._param(i+1, std::forward<T>(t)...);
  }
  template<typename... Args> sqlstmt_t &param(Args&&... args) {
    return _param (1, std::forward<Args> (args)...);
  }

  /* Bind tuple */
  template<size_t N> struct _tparm_helper {
    template<typename... Args>
    static sqlstmt_t &go(sqlstmt_t &s, const std::tuple<Args...> &t) {
      return _tparm_helper<N-1>::go(s.bind(N, std::get<N-1>(t)), t);
    }
  };
  template<typename... Args>
  sqlstmt_t &tparam(const std::tuple<Args...> &t) {
    return _tparm_helper<sizeof...(Args)>::go(*this, t);
  }
};

template<> struct sqlstmt_t::_tparm_helper<0> {
  template<typename... Args>
  static sqlstmt_t &go(sqlstmt_t &s, const std::tuple<Args...> &t) { return s; }
};

template<> inline bool
sqlstmt_t::column(int i)
{
  return null(i);
}
template<> inline i64
sqlstmt_t::column(int i)
{
  return integer(i);
}
template<> inline double
sqlstmt_t::column(int i)
{
  return real(i);
}
template<> inline std::string
sqlstmt_t::column(int i)
{
  return str(i);
}
template<> inline const char *
sqlstmt_t::column(int i)
{
  return c_str(i);
}

void sqlexec (sqlite3 *db, const char *fmt, ...);

