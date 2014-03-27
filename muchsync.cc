
#include <cstring>
#include <istream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <vector>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <openssl/rand.h>
#include <notmuch.h>
#include "muchsync.h"
#include "infinibuf.h"
#include "notmuch_db.h"

using namespace std;

const char dbvers[] = "muchsync 0";
#define MUCHSYNC_DEFDIR "/.notmuch/muchsync"
const char muchsync_defdir[] = MUCHSYNC_DEFDIR;
const char muchsync_dbpath[] = MUCHSYNC_DEFDIR "/state.db";
const char muchsync_trashdir[] = MUCHSYNC_DEFDIR "/trash";
const char muchsync_tmpdir[] = MUCHSYNC_DEFDIR "/tmp";

bool opt_fullscan;
bool opt_noscan;
bool opt_init;
bool opt_server;
bool opt_upbg;
bool opt_noup;
bool opt_new;
int opt_verbose;
int opt_upbg_fd = -1;
string opt_ssh = "ssh -CTaxq";
string opt_remote_muchsync_path = "muchsync";
string opt_notmuch_config;
string opt_init_dest;

#if 1
struct whattocatch_t {
  const char *what() noexcept { return "no such exception"; }
};
#else
using whattocatch_t = const exception;
#endif

const char schema_def[] = R"(
-- General table
CREATE TABLE configuration (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT);
CREATE TABLE sync_vector (
  replica INTEGER PRIMARY KEY,
  version INTEGER);

-- Shadow copy of the Xapian database to detect changes
CREATE TABLE xapian_dirs (
  dir_path TEXT UNIQUE NOT NULL,
  dir_docid INTEGER PRIMARY KEY,
  dir_mtime INTEGER);
CREATE TABLE tags (
  tag TEXT NOT NULL,
  docid INTEGER NOT NULL,
  UNIQUE (docid, tag),
  UNIQUE (tag, docid));
CREATE TABLE message_ids (
  message_id TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY,
  replica INTEGER,
  version INTEGER);
CREATE INDEX message_ids_writestamp ON message_ids (replica, version);
CREATE TABLE xapian_files (
  dir_docid INTEGER NOT NULL,
  name TEXT NOT NULL,
  docid INTEGER,
  mtime REAL,
  inode INTEGER,
  hash_id INGEGER,
  PRIMARY KEY (dir_docid, name));
CREATE INDEX xapian_files_hash_id ON xapian_files (hash_id, dir_docid);
CREATE TABLE maildir_hashes (
  hash_id INTEGER PRIMARY KEY,
  hash TEXT UNIQUE NOT NULL,
  size INTEGER,
  message_id TEXT,
  replica INTEGER,
  version INTEGER);
CREATE INDEX maildir_hashes_message_id ON maildir_hashes (message_id);
CREATE INDEX maildir_hashes_writestamp ON maildir_hashes (replica, version);
CREATE TABLE xapian_nlinks (
  hash_id INTEGER NOT NULL,
  dir_docid INTEGER NOT NULL,
  link_count INTEGER,
  PRIMARY KEY (hash_id, dir_docid));
)";

static double
time_stamp ()
{
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  return ts_to_double (ts);
}

static double start_time_stamp {time_stamp()};
static double last_time_stamp {start_time_stamp};

void
print_time (string msg)
{
  double now = time_stamp();
  if (opt_verbose > 0) {
    auto oldFlags = cerr.flags();
    cerr.setf (ios::fixed, ios::floatfield);
    cerr << msg << "... " << now - start_time_stamp
	 << " (+" << now - last_time_stamp << ")\n";
    cerr.flags (oldFlags);
  }
  last_time_stamp = now;
}

sqlite3 *
dbcreate (const char *path)
{
  i64 self = 0;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1
      || self == 0) {
    cerr << "RAND_pseudo_bytes failed\n";
    return nullptr;
  }
  self &= ~(i64 (1) << 63);

  sqlite3 *db = nullptr;
  int err = sqlite3_open_v2 (path, &db,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, nullptr);
  if (err) {
    cerr << path << ": " << sqlite3_errstr (err) << '\n';
    return nullptr;
  }
  sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");

  try {
    sqlexec (db, "BEGIN;");
    sqlexec (db, schema_def);
    setconfig (db, "dbvers", dbvers);
    setconfig (db, "self", self);
    setconfig (db, "last_scan", 0.0);
    sqlexec (db, "INSERT INTO sync_vector (replica, version)"
	     " VALUES (%lld, 1);", self);
    sqlexec (db, "COMMIT;");
  } catch (sqlerr_t exc) {
    sqlite3_close_v2 (db);
    cerr << exc.what () << '\n';
    return nullptr;
  }
  return db;
}

bool
muchsync_init (const string &maildir, bool create = false)
{
  string trashbase = maildir + muchsync_trashdir + "/";
  if (!access ((maildir + muchsync_tmpdir).c_str(), 0)
      && !access ((trashbase + "ff").c_str(), 0))
    return true;

  if (create && mkdir (maildir.c_str(), 0777) && errno != EEXIST) {
    perror (maildir.c_str());
    return false;
  }

  string notmuchdir = maildir + "/.notmuch";
  if (create && access (notmuchdir.c_str(), 0) && errno == ENOENT) {
    notmuch_database_t *notmuch;
    if (!notmuch_database_create (maildir.c_str(), &notmuch))
      notmuch_database_destroy (notmuch);
  }

  string msdir = maildir + muchsync_defdir;
  for (string d : {msdir, maildir + muchsync_trashdir,
	maildir + muchsync_tmpdir}) {
    if (mkdir (d.c_str(), 0777) && errno != EEXIST) {
      perror (d.c_str());
      return false;
    }
  }

  for (int i = 0; i < 0x100; i++) {
    ostringstream os;
    os << trashbase << hex << setfill('0') << setw(2) << i;
    if (mkdir (os.str().c_str(), 0777) && errno != EEXIST) {
      perror (os.str().c_str());
      return false;
    }
  }
  return true;
}

sqlite3 *
dbopen (const char *path, bool exclusive = false)
{
  sqlite3 *db = nullptr;
  if (access (path, 0) && errno == ENOENT)
    db = dbcreate (path);
  else {
    sqlite3_open_v2 (path, &db, SQLITE_OPEN_READWRITE, nullptr);
    if (exclusive)
      sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");
  }
  if (!db)
    return nullptr;

  sqlexec (db, "PRAGMA secure_delete = 0;");

  try {
    if (getconfig<string> (db, "dbvers") != dbvers) {
      cerr << path << ": invalid database version\n";
      sqlite3_close_v2 (db);
      return nullptr;
    }
    getconfig<i64> (db, "self");
  }
  catch (sqldone_t) {
    cerr << path << ": invalid configuration\n";
    sqlite3_close_v2 (db);
    return nullptr;
  }
  catch (sqlerr_t &e) {
    cerr << path << ": " << e.what() << '\n';
    sqlite3_close_v2 (db);
    return nullptr;
  }

  return db;
}

versvector
get_sync_vector (sqlite3 *db)
{
  versvector vv;
  sqlstmt_t s (db, "SELECT replica, version FROM sync_vector;");
  while (s.step().row())
    vv.emplace (s.integer(0), s.integer(1));
  return vv;
}

string
show_sync_vector (const versvector &vv)
{
  ostringstream sb;
  sb << '<';
  bool first = true;
  for (auto ws : vv) {
    if (first)
      first = false;
    else
      sb << ",";
    sb << 'R' << ws.first << '=' << ws.second;
  }
  sb << '>';
  return sb.str();
}

istream &
read_writestamp (istream &in, writestamp &ws)
{
  input_match (in, 'R');
  in >> ws.first;
  input_match (in, '=');
  in >> ws.second;
  return in;
}

istream &
read_sync_vector (istream &in, versvector &vv)
{
  input_match (in, '<');
  vv.clear();
  for (;;) {
    char c;
    if ((in >> c) && c == '>')
      return in;
    in.unget();
    writestamp ws;
    if (!read_writestamp (in, ws))
      break;
    vv.insert (ws);
    if (!(in >> c) || c == '>')
      break;
    if (c != ',') {
      in.setstate (ios_base::failbit);
      break;
    }
  }
  return in;
}

void
sync_local_data (sqlite3 *sqldb, const string &maildir)
{
  print_time ("synchronizing muchsync database with Xapian");
  sqlexec (sqldb, "SAVEPOINT localsync;");

  try {
    i64 self = getconfig<i64>(sqldb, "self");
    sqlexec (sqldb, "UPDATE sync_vector"
	     " SET version = version + 1 WHERE replica = %lld;", self);
    if (sqlite3_changes (sqldb) != 1)
      throw runtime_error ("My replica id (" + to_string (self)
			   + ") not in sync vector");
    versvector vv = get_sync_vector (sqldb);
    i64 vers = vv.at(self);
    writestamp ws { self, vers };

    xapian_scan (sqldb, ws, maildir);
  }
  catch (...) {
    sqlexec (sqldb, "ROLLBACK TO localsync;");
    throw;
  }
  sqlexec (sqldb, "RELEASE localsync;");
  print_time ("finished synchronizing muchsync database with Xapian");
}

string
notmuch_maildir_location()
{
  string loc = cmd_output("notmuch config get database.path");
  while (loc.size() > 0 && (loc.back() == '\n' || loc.back() == '\r'))
    loc.resize(loc.size()-1);
  struct stat sb;
  if (!loc.size() || (!stat(loc.c_str(), &sb) && !S_ISDIR(sb.st_mode)))
    throw runtime_error("cannot find location of default maildir");
  return loc;
}

static string
get_notmuch_config()
{
  char *p = getenv("NOTMUCH_CONFIG");
  if (p && *p)
    return p;
  p = getenv("HOME");
  if (p && *p)
    return string(p) + "/.notmuch-config";
  cerr << "Cannot find HOME directory\n";
  exit(1);
}

static void
tag_stderr(string tag)
{
  infinistreambuf *isb =
    new infinistreambuf(new infinibuf_mt);
  streambuf *err = cerr.rdbuf(isb);
  thread t ([=]() {
      istream in (isb);
      ostream out (err);
      string line;
      while (getline(in, line))
	out << tag << line << endl;
    });
  t.detach();
  cerr.rdbuf(isb);
}

[[noreturn]] void
usage (int code = 1)
{
  (code ? cerr : cout) << "\
usage: muchsync\n\
       muchsync server [server-options]\n\
       muchsync --init maildir server [server-options]\n\
\n\
Additional options:\n\
   -C file       Specify path to notmuch config file\n\
   -F            Disable optimizations and do full maildir scan\n\
   -v            Increase verbosity\n\
   -r path       Specify path to notmuch executable on server\n\
   -s ssh-cmd    Specify ssh command and arguments\n\
   --config file Specify path to notmuch config file (same as -C)\n\
   --new         Run notmuch new first\n\
   --noup[load]  Do not upload changes to server\n\
   --upbg        Download mail in forground, then upload in background\n\
   --version     Print version number and exit\n\
   --help        Print usage\n";
  exit (code);
}

static void
server()
{
  ifdinfinistream ibin(0);
  cleanup _fixbuf ([](streambuf *sb){ cin.rdbuf(sb); },
		   cin.rdbuf(ibin.rdbuf()));
  tag_stderr("[SERVER] ");

  unique_ptr<notmuch_db> nmp;
  try {
    nmp.reset(new notmuch_db (opt_notmuch_config));
  } catch (whattocatch_t e) { cerr << e.what() << '\n'; exit (1); }
  notmuch_db &nm = *nmp;

  string dbpath = nm.maildir + muchsync_dbpath;

  if (!muchsync_init (nm.maildir))
    exit (1);
  if (opt_new)
    nm.run_new("[SERVER notmuch] ");

  sqlite3 *db = dbopen(dbpath.c_str());
  if (!db)
    exit(1);
  cleanup _c (sqlite3_close_v2, db);

  try {
    if (!opt_noscan)
      sync_local_data(db, nm.maildir);
    muchsync_server(db, nm);
  }
  catch (whattocatch_t &e) {
    cerr << e.what() << '\n';
    exit(1);
  }
}

static void
create_config(istream &in, ostream &out, string &maildir)
{
  if (!maildir.size() || !maildir.front())
    throw runtime_error ("illegal empty maildir path\n");
  string line;
  out << "conffile\n";
  get_response(in, line);
  get_response(in, line);
  size_t len = stoul(line.substr(4));
  if (len <= 0)
    throw runtime_error ("server did not return configuration file\n");
  string conf;
  conf.resize(len);
  if (!in.read(&conf.front(), len))
    throw runtime_error ("cannot read configuration file from server\n");

  int fd = open(opt_notmuch_config.c_str(), O_CREAT|O_TRUNC|O_WRONLY|O_EXCL,
		0666);
  if (fd < 0)
    throw runtime_error (opt_notmuch_config + ": " + strerror (errno));
  write(fd, conf.c_str(), conf.size());
  close(fd);

  if (maildir[0] != '/') {
    const char *p = getenv("PWD");
    if (!p)
      throw runtime_error ("no PWD in environment\n");
    maildir = p + ("/" + maildir);
  }

  notmuch_db nm (opt_notmuch_config);
  nm.set_config ("database.path", maildir.c_str(), nullptr);
}

static void
client(int ac, char **av)
{
  unique_ptr<notmuch_db> nmp;
  struct stat sb;
  int err = stat(opt_notmuch_config.c_str(), &sb);
  if (opt_init) {
    if (!err) {
      cerr << opt_notmuch_config << " should not exist with --init option\n";
      exit (1);
    }
    else if (errno != ENOENT) {
      cerr << opt_notmuch_config << ": " << strerror(errno) << '\n';
      exit (1);
    }
  }
  else if (err) {
    cerr << opt_notmuch_config << ": " << strerror(errno) << '\n';
    exit (1);
  }
  else {
    try {
      nmp.reset(new notmuch_db (opt_notmuch_config));
    } catch (whattocatch_t e) { cerr << e.what() << '\n'; exit (1); }
  }

  if (ac == 0) {
    if (!nmp)
      usage();
    if (!muchsync_init(nmp->maildir, true))
      exit (1);
    if (opt_new)
      nmp->run_new();
    string dbpath = nmp->maildir + muchsync_dbpath;
    sqlite3 *db = dbopen(dbpath.c_str());
    if (!db)
      exit (1);
    cleanup _c (sqlite3_close_v2, db);
    sync_local_data (db, nmp->maildir);
    exit(0);
  }

  ostringstream os;
  os << opt_ssh << ' ' << av[0] << ' ' << opt_remote_muchsync_path
     << " --server";
  for (int i = 1; i < ac; i++)
    os << ' ' << av[i];
  string cmd (os.str());
  int fds[2];
  cmd_iofds (fds, cmd);
  ofdstream out (fds[1]);
  ifdinfinistream in (fds[0]);
  in.tie (&out);

  if (opt_init) {
    create_config(in, out, opt_init_dest);
    try {
      nmp.reset(new notmuch_db (opt_notmuch_config, true));
    } catch (whattocatch_t e) { cerr << e.what() << '\n'; exit (1); }
  }
  if (!muchsync_init(nmp->maildir, opt_init))
    exit(1);
  if (opt_new)
    nmp->run_new();
#if 0
  if (opt_init && !mkdir((nmp->maildir + "/.notmuch/hooks").c_str(), 0777)) {
    int fd = open ((nmp->maildir + "/.notmuch/hooks/post-new").c_str(),
		   O_CREAT|O_WRONLY|O_EXCL|O_TRUNC, 0777);
    ofdstream of (fd);
    of << "#!/bin/sh\nmuchsync --upbg -r " << opt_remote_muchsync_path;
    for (int i = 0; i < ac; i++)
      of << ' ' << av[i];
    of << " --new" << flush;
  }
#endif
  string dbpath = nmp->maildir + muchsync_dbpath;
  sqlite3 *db = dbopen(dbpath.c_str(), true);
  if (!db)
    exit (1);
  cleanup _c (sqlite3_close_v2, db);

  try {
    muchsync_client (db, *nmp, in, out);
  }
  catch (whattocatch_t &e) {
    cerr << e.what() << '\n';
    exit (1);
  }
}

enum opttag {
  OPT_VERSION = 0x100,
  OPT_SERVER,
  OPT_NOSCAN,
  OPT_UPBG,
  OPT_NOUP,
  OPT_HELP,
  OPT_NEW,
  OPT_INIT
};

static const struct option muchsync_options[] = {
  { "version", no_argument, nullptr, OPT_VERSION },
  { "server", no_argument, nullptr, OPT_SERVER },
  { "noscan", no_argument, nullptr, OPT_NOSCAN },
  { "upbg", no_argument, nullptr, OPT_UPBG },
  { "noup", no_argument, nullptr, OPT_NOUP },
  { "noupload", no_argument, nullptr, OPT_NOUP },
  { "new", no_argument, nullptr, OPT_NEW },
  { "init", required_argument, nullptr, OPT_INIT },
  { "config", required_argument, nullptr, 'C' },
  { "help", no_argument, nullptr, OPT_HELP },
  { nullptr, 0, nullptr, 0 }
};

int
main(int argc, char **argv)
{
  umask (077);

  opt_notmuch_config = get_notmuch_config();

  int opt;
  while ((opt = getopt_long(argc, argv, "+C:Fr:s:v",
			    muchsync_options, nullptr)) != -1)
    switch (opt) {
    case 0:
      break;
    case 'C':
      opt_notmuch_config = optarg;
      break;
    case 'F':
      opt_fullscan = true;
      break;
    case 'r':
      opt_remote_muchsync_path = optarg;
      break;
    case 's':
      opt_ssh = optarg;
      break;
    case 'v':
      opt_verbose++;
      break;
    case OPT_VERSION:
      cout << PACKAGE_STRING << '\n';
      exit (0);
    case OPT_SERVER:
      opt_server = true;
      break;
    case OPT_NOSCAN:
      opt_noscan = true;
      break;
    case OPT_UPBG:
      opt_upbg = true;
      break;
    case OPT_NOUP:
      opt_noup = true;
      break;
    case OPT_NEW:
      opt_new = true;
      break;
    case OPT_INIT:
      opt_init = true;
      opt_init_dest = optarg;
      break;
    case OPT_HELP:
      usage(0);
    default:
      usage();
    }

  if (opt_server) {
    if (opt_init || opt_noup || opt_upbg || optind != argc)
      usage();
    server();
  }
  else if (opt_upbg) {
    int fds[2];
    if (pipe(fds)) {
      cerr << "pipe: " << strerror(errno) << '\n';
      exit (1);
    }
    fcntl(fds[1], F_SETFD, 1);
    if (fork() > 0) {
      char c;
      close(fds[1]);
      read(fds[0], &c, 1);
      if (opt_verbose)
	cerr << "backgrounding\n";
      exit(0);
    }
    close(fds[0]);
    opt_upbg_fd = fds[1];
    client(argc - optind, argv + optind);
  }
  else
    client(argc - optind, argv + optind);
  return 0;
}
