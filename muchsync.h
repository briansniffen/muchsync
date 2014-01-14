// -*- C++ -*-

#include <cassert>
#include <memory>
#include <string>
#include <unordered_map>
#include <sqlite3.h>
#include <notmuch.h>

using std::string;

extern const char dbvers[];

constexpr double
ts_to_double (const timespec &ts)
{
  return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

/* Kludgy way to fit C cleanup functions into the C++ RAII scheme. */
class cleanup {
  std::function<void()> action_;
public:
  cleanup(std::function<void()> &&action) : action_(action) {}
  template<typename... Args> cleanup (Args... args)
    : action_(std::bind(args...)) {}
  ~cleanup() { action_(); }
};

/* sqlite.cc */
using i64 = sqlite3_int64;

struct sqlerr_t : public std::runtime_error {
  sqlerr_t (const string &msg) : std::runtime_error (msg) {}
};
/* A sqldone_t is thrown if you ask for data when no rows are left */
struct sqldone_t : public std::runtime_error {
  sqldone_t (const string &msg) : std::runtime_error (msg) {}
};

class sqlstmt_t {
  sqlite3_stmt *stmt_;
  int status_ = SQLITE_OK;
  sqlstmt_t &set_status (int status);
  void fail ();
  void ensure_row () { if (status_ != SQLITE_ROW) fail(); }

 public:
  explicit sqlstmt_t(sqlite3_stmt *stmt) : stmt_ (stmt) {}
  explicit sqlstmt_t(sqlite3 *db, const char *fmt, ...);
  sqlstmt_t(const sqlstmt_t &r) = delete;
  sqlstmt_t(sqlstmt_t &&r) : stmt_ (r.stmt_) { r.stmt_ = nullptr; }
  ~sqlstmt_t() { sqlite3_finalize (stmt_); }

  sqlite3_stmt *get() { return stmt_; }
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
  template<typename T> T column (int);
  bool null(int iCol) {
    ensure_row();
    return sqlite3_column_type (stmt_, iCol) == SQLITE_NULL;
  }
  sqlite3_int64 integer(int iCol) {
    ensure_row();
    return sqlite3_column_int64 (stmt_, iCol);
  }
  double real(int iCol) {
    ensure_row();
    return sqlite3_column_double (stmt_, iCol);
  }
  string str(int iCol) {
    ensure_row();
    return { static_cast<const char *> (sqlite3_column_blob (stmt_, iCol)),
	size_t (sqlite3_column_bytes (stmt_, iCol)) };
  }
  const char *c_str(int iCol) {
    ensure_row();
    return reinterpret_cast<const char *> (sqlite3_column_text (stmt_, iCol));
  }
  sqlite3_value *value(int iCol) {
    ensure_row();
    return sqlite3_column_value(stmt_, iCol);
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
  sqlstmt_t &bind_text(int i, const string &v) {
    return set_status (sqlite3_bind_text(stmt_, i, v.data(), v.size(),
					 SQLITE_STATIC));
  }
  sqlstmt_t &bind_text(int i, string &&v) {
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

  sqlstmt_t &_param(int) { return *this; }
  template<typename... Rest>
    sqlstmt_t &_param(int i, sqlite3_int64 v, Rest... rest) {
    return this->bind_int(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, unsigned v, Rest... rest) {
    return this->bind_int(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, int v, Rest... rest) {
    return this->bind_int(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, const string &v, Rest... rest) {
    return this->bind_text(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, string &&v, Rest... rest) {
    return this->bind_text(i, move (v))._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, const char *v, Rest... rest) {
    return this->bind_text(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, double v, Rest... rest) {
    return this->bind_real(i, v)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, std::nullptr_t, Rest... rest) {
    return this->bind_null(i)._param(i+1, rest...);
  }
  template<typename... Rest>
    sqlstmt_t &_param(int i, const sqlite3_value *v, Rest... rest) {
    return this->bind_value(i, v)._param(i+1, rest...);
  }
  template<typename... Args> sqlstmt_t &param(Args&&... args) {
    return _param (1, std::forward<Args> (args)...);
  }
};

template<> inline bool
sqlstmt_t::column (int iCol)
{
  return null (iCol);
}
template<> inline i64
sqlstmt_t::column (int iCol)
{
  return integer (iCol);
}
template<> inline double
sqlstmt_t::column (int iCol)
{
  return real (iCol);
}
template<> inline string
sqlstmt_t::column (int iCol)
{
  return str (iCol);
}
template<> inline const char *
sqlstmt_t::column (int iCol)
{
  return c_str (iCol);
}

void dbthrow (sqlite3 *db, const char *query);
void sqlexec (sqlite3 *db, const char *fmt, ...);
sqlstmt_t fmtstmt (sqlite3 *db, const char *fmt, ...);
int fmtstep (sqlite3 *db, sqlite3_stmt **stmtpp, const char *fmt, ...);
void save_old_table (sqlite3 *sqldb, const string &table, const char *create);

/* protocol.cc */
void muchsync_server (sqlite3 *db, const string &maildir);

/* muchsync.cc */
extern const char xapian_dirs_def[];
extern bool opt_fullscan;
extern bool opt_maildir_only, opt_xapian_only;
extern int opt_verbose;
extern string opt_ssh;
// Writestamp is the pair (replica-id, version-number)
using writestamp = std::pair<i64,i64>;
// Version vector is a set of writestamps with distinct replica-ids
using versvector = std::unordered_map<i64,i64>;
void print_time (string msg);
versvector get_sync_vector (sqlite3 *db);
string show_sync_vector (const versvector &vv);
bool read_sync_vector (const string &s, versvector &vv);


/*
 * Example: getconfig(db, "key", &sqlstmt_t::integer)
 */
template<typename T> T
getconfig (sqlite3 *db, const string &key)
{
  static const char query[] = "SELECT value FROM configuration WHERE key = ?;";
  return sqlstmt_t(db, query).param(key).step().template column<T>(0);
}
template<typename T> void
setconfig (sqlite3 *db, const string &key, const T &value)
{
  static const char query[] =
    "INSERT OR REPLACE INTO configuration VALUES (?, ?);";
  sqlstmt_t(db, query).param(key, value).step();
}

/* xapian_sync.cc */
void xapian_scan (sqlite3 *sqldb, writestamp ws, const string &path);
string term_from_tag (const string &tag);
string tag_from_term (const string &term);

/* fdstream.cc */
string cmd_output (const string &cmd);
void infinite_buffer (int infd, int outfd);

/* Maildirs place messages in directories called "new" and "dir" */
inline bool
dir_contains_messages (const string &dir)
{
  if (dir.length() >= 4) {
    string end (dir.substr (dir.length() - 4));
    return end == "/cur" || end == "/new";
  }
  return dir == "cur" || dir == "new";
}
void scan_maildir (sqlite3 *sqldb, writestamp ws, string maildir);
