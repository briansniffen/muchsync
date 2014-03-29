// -*- C++ -*-

#include <istream>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "cleanup.h"
#include "sqlstmt.h"
#include "sql_db.h"

using std::string;

template<typename C> inline typename C::mapped_type
find_default (typename C::mapped_type def, const C &c, typename C::key_type k)
{
  auto i = c.find(k);
  return i == c.end() ? def : i->second;
}

extern const char dbvers[];

constexpr double
ts_to_double (const timespec &ts)
{
  return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

/* protocol.cc */
struct notmuch_db;
string permissive_percent_encode (const string &raw);
void muchsync_server (sqlite3 *db, notmuch_db &nm);
void muchsync_client (sqlite3 *db, notmuch_db &nm,
		      std::istream &in, std::ostream &out);
std::istream &get_response (std::istream &in, string &line);


/* muchsync.cc */
extern bool opt_fullscan;
extern bool opt_noscan;
extern bool opt_upbg;
extern int opt_upbg_fd;
extern bool opt_noup;
extern int opt_verbose;
extern string opt_ssh;
extern string opt_remote_muchsync_path;
extern string opt_notmuch_config;
extern const char muchsync_trashdir[];
extern const char muchsync_tmpdir[];
// Version vector is a set of writestamps with distinct replica-ids
using versvector = std::unordered_map<i64,i64>;
void print_time (string msg);
versvector get_sync_vector (sqlite3 *db);
string show_sync_vector (const versvector &vv);
std::istream &read_sync_vector (std::istream &sb, versvector &vv);
void sync_local_data (sqlite3 *sqldb, const string &maildir);

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
void xapian_scan (sqlite3 *sqldb, writestamp ws, string maildir);

/* spawn.cc */
string cmd_output (const string &cmd);
void cmd_iofds (int fds[2], const string &cmd);

