// -*- C++ -*-

#include <istream>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "cleanup.h"
#include "sql_db.h"
#include "notmuch_db.h"

using std::string;

/* protocol.cc */
void muchsync_server(sqlite3 *db, notmuch_db &nm);
void muchsync_client(sqlite3 *db, notmuch_db &nm,
		     std::istream &in, std::ostream &out);
std::istream &get_response(std::istream &in, string &line, bool err_ok = true);

/* muchsync.cc */
extern bool opt_fullscan;
extern bool opt_noscan;
extern bool opt_upbg;
extern int opt_upbg_fd;
extern bool opt_noup;
extern string opt_ssh;
extern string opt_remote_muchsync_path;
extern string opt_notmuch_config;
extern const char muchsync_trashdir[];
extern const char muchsync_tmpdir[];

/* xapian_sync.cc */
void sync_local_data(sqlite3 *sqldb, const string &maildir);
