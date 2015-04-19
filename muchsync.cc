
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "misc.h"
#include "muchsync.h"
#include "infinibuf.h"

using namespace std;

#if 0
// This gives core dumps to make it easier to debug
struct no_such_exception_t {
  const char *what() noexcept { return "no such exception"; }
};
using whattocatch_t = no_such_exception_t;
#else
using whattocatch_t = const exception;
#endif

#define MUCHSYNC_DEFDIR "/.notmuch/muchsync"
const char muchsync_defdir[] = MUCHSYNC_DEFDIR;
const char muchsync_dbpath[] = MUCHSYNC_DEFDIR "/state.db";
const char muchsync_trashdir[] = MUCHSYNC_DEFDIR "/trash";
const char muchsync_tmpdir[] = MUCHSYNC_DEFDIR "/tmp";

constexpr char shell[] = "/bin/sh";

bool opt_fullscan;
bool opt_noscan;
bool opt_init;
bool opt_server;
bool opt_upbg;
bool opt_noup;
bool opt_nonew;
int opt_verbose;
int opt_upbg_fd = -1;
string opt_ssh = "ssh -CTaxq";
string opt_remote_muchsync_path = "muchsync";
string opt_notmuch_config;
string opt_init_dest;

static bool
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
   --nonew       No not run notmuch new first\n\
   --noup[load]  Do not upload changes to server\n\
   --upbg        Download mail in forground, then upload in background\n\
   --self        Print local replica identifier and exit\n\
   --version     Print version number and exit\n\
   --help        Print usage\n";
  exit (code);
}

static void
print_self()
{
  unique_ptr<notmuch_db> nmp;
  try {
    nmp.reset(new notmuch_db (opt_notmuch_config));
  } catch (whattocatch_t e) { cerr << e.what() << '\n'; exit (1); }
  notmuch_db &nm = *nmp;

  string dbpath = nm.maildir + muchsync_dbpath;

  sqlite3 *db = dbopen(dbpath.c_str(), false);
  if (!db)
    exit(1);
  cleanup _c (sqlite3_close_v2, db);
  cout << getconfig<i64>(db, "self") << '\n';
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

  if (!opt_nonew)
    nm.run_new();
  if (!muchsync_init (nm.maildir))
    exit (1);

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
cmd_iofds (int fds[2], const string &cmd)
{
  int ifds[2], ofds[2];
  if (pipe (ifds))
    throw runtime_error (string ("pipe: ") + strerror (errno));
  if (pipe (ofds)) {
    close (ifds[0]);
    close (ifds[1]);
    throw runtime_error (string ("pipe: ") + strerror (errno));
  }

  pid_t pid = fork();
  switch (pid) {
  case -1:
    close (ifds[0]);
    close (ifds[1]);
    close (ofds[0]);
    close (ofds[1]);
    throw runtime_error (string ("fork: ") + strerror (errno));
    break;
  case 0:
    close (ifds[0]);
    close (ofds[1]);
    if (ofds[0] != 0) {
      dup2 (ofds[0], 0);
      close (ofds[0]);
    }
    if (ifds[1] != 1) {
      dup2 (ifds[1], 1);
      close (ifds[1]);
    }
    execl (shell, shell, "-c", cmd.c_str(), nullptr);
    cerr << shell << ": " << strerror (errno) << '\n';
    _exit (1);
    break;
  default:
    close (ifds[1]);
    close (ofds[0]);
    fcntl (ifds[0], F_SETFD, 1);
    fcntl (ofds[1], F_SETFD, 1);
    fds[0] = ifds[0];
    fds[1] = ofds[1];
    break;
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
    if (!opt_nonew)
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
  if (!muchsync_init(nmp->maildir, true))
    exit(1);
  if (!opt_nonew)
    nmp->run_new();
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
  OPT_NONEW,
  OPT_SELF,
  OPT_INIT
};

static const struct option muchsync_options[] = {
  { "version", no_argument, nullptr, OPT_VERSION },
  { "server", no_argument, nullptr, OPT_SERVER },
  { "noscan", no_argument, nullptr, OPT_NOSCAN },
  { "upbg", no_argument, nullptr, OPT_UPBG },
  { "noup", no_argument, nullptr, OPT_NOUP },
  { "noupload", no_argument, nullptr, OPT_NOUP },
  { "nonew", no_argument, nullptr, OPT_NONEW },
  { "init", required_argument, nullptr, OPT_INIT },
  { "self", no_argument, nullptr, OPT_SELF },
  { "config", required_argument, nullptr, 'C' },
  { "help", no_argument, nullptr, OPT_HELP },
  { nullptr, 0, nullptr, 0 }
};

int
main(int argc, char **argv)
{
  umask (077);

  opt_notmuch_config = notmuch_db::default_notmuch_config();
  bool opt_self = false;

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
    case OPT_NONEW:
      opt_nonew = true;
      break;
    case OPT_SELF:
      opt_self = true;
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

  if (opt_self)
    print_self();
  else if (opt_server) {
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
