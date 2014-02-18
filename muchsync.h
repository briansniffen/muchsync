// -*- C++ -*-

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <openssl/sha.h>

#include "sqlstmt.h"

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

/* Kludgy way to fit C cleanup functions into the C++ RAII scheme. */
class cleanup {
  std::function<void()> action_;
public:
  cleanup(std::function<void()> &&action) : action_(action) {}
  template<typename... Args> cleanup (Args... args)
    : action_(std::bind(args...)) {}
  void disable() { action_ = [] () {}; }
  ~cleanup() { action_(); }
};

/* protocol.cc */
string permissive_percent_encode (const string &raw);
void muchsync_server (sqlite3 *db, const string &maildir);
void muchsync_client (sqlite3 *db, const string &maildir,
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
extern const char xapian_dirs_def[];
extern const char muchsync_trashdir[];
extern const char muchsync_tmpdir[];
extern std::unordered_set<string> new_tags;
// Writestamp is the pair (replica-id, version-number)
using writestamp = std::pair<i64,i64>;
// Version vector is a set of writestamps with distinct replica-ids
using versvector = std::unordered_map<i64,i64>;
void print_time (string msg);
versvector get_sync_vector (sqlite3 *db);
std::unordered_set<string> notmuch_new_tags ();
string show_sync_vector (const versvector &vv);
std::istream &read_writestamp (std::istream &in, writestamp &ws);
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
string percent_encode (const string &raw);
string percent_decode (const string &escaped);

/* spawn.cc */
string cmd_output (const string &cmd);
void cmd_iofds (int fds[2], const string &cmd);

/* maildir.cc */
class hash_ctx {
  SHA_CTX ctx_;
public:
  static constexpr size_t output_bytes = SHA_DIGEST_LENGTH;
  hash_ctx() { init(); }
  void init() { SHA1_Init(&ctx_); }
  void update(const void *buf, size_t n) { SHA1_Update (&ctx_, buf, n); }
  string final();
};
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
string get_sha (int dfd, const char *direntry, i64 *sizep);

inline std::istream &
input_match (std::istream &in, char want)
{
  char got;
  if ((in >> got) && got != want)
    in.setstate (std::ios_base::failbit);
  return in;
}
