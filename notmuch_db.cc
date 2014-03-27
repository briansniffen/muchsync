
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "cleanup.h"
#include "infinibuf.h"
#include "notmuch_db.h"

using namespace std;

static unordered_set<string>
lines(const string &s)
{
  istringstream is (s);
  string line;
  unordered_set<string> ret;
  while (getline(is, line))
    ret.insert(line);
  return ret;
}

static string
chomp(string s)
{
  while (s.length() && (s.back() == '\n' || s.back() == '\r'))
    s.resize(s.length() - 1);
  return s;
}

static bool
conf_to_bool(string s)
{
  s = chomp(s);
  if (s.empty() || s == "false" || s == "0")
    return false;
  return true;
}

Xapian::docid
notmuch_db::get_docid(notmuch_message_t *message)
{
  struct fake_message {
    notmuch_database_t *notmuch;
    Xapian::docid doc_id;
  };
  /* This is massively evil, but looking through git history, doc_id
   * has been the second element of the structure for a long time. */
  return reinterpret_cast<const fake_message *>(message)->doc_id;
}

notmuch_db::message_t
notmuch_db::get_message(const char *msgid)
{
  notmuch_message_t *message;
  nmtry("notmuch_database_find_message",
	notmuch_database_find_message (notmuch(), msgid, &message));
  return message_t (message);
}

notmuch_db::message_t
notmuch_db::add_message(const string &path, tags_t *newtags, bool *was_new)
{
  nmtry("notmuch_database_begin_atomic",
	notmuch_database_begin_atomic(notmuch()));
  cleanup _c (notmuch_database_end_atomic, notmuch());

  notmuch_status_t err;
  notmuch_message_t *message;
  err = notmuch_database_add_message(notmuch(), path.c_str(), &message);
  if (err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID) {
    nmtry("notmuch_database_add_message", err);
    set_tags(message, newtags ? *newtags : new_tags);
  }
  if (was_new)
    *was_new = err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID;
  return message_t (message);
}

void
notmuch_db::set_tags(notmuch_message_t *msg, const tags_t &tags)
{
  // Deliberately don't unthaw message if we throw exception
  nmtry("notmuch_message_freeze", notmuch_message_freeze(msg));
  nmtry("notmuch_message_remove_all_tags",
	notmuch_message_remove_all_tags(msg));
  for (auto tag : tags)
    nmtry("notmuch_message_add_tag", notmuch_message_add_tag(msg, tag.c_str()));
  nmtry("notmuch_message_thaw", notmuch_message_thaw(msg));
}

string
notmuch_db::default_notmuch_config()
{
  char *p = getenv("NOTMUCH_CONFIG");
  if (p && *p)
    return p;
  p = getenv("HOME");
  if (p && *p)
    return string(p) + "/.notmuch-config";
  throw runtime_error ("Cannot find HOME directory\n");
}

string
notmuch_db::config_get(const char *config)
{
  const char *av[] { "notmuch", "config", "get", config, nullptr };
  return run_notmuch(av);
}

notmuch_db::notmuch_db(string config)
  : notmuch_config (config),
    maildir (chomp(config_get("database.path"))),
    new_tags (lines(config_get("new.tags"))),
    sync_flags (conf_to_bool(config_get("maildir.synchronize_flags")))
{
  if (maildir.empty())
    throw runtime_error(notmuch_config + ": no database.path in config file");
  struct stat sb;
  if (stat(maildir.c_str(), &sb) || !S_ISDIR(sb.st_mode))
    throw runtime_error(maildir + ": cannot access maildir");
}

notmuch_db::~notmuch_db()
{
  close();
}

string
notmuch_db::run_notmuch(const char **av)
{
  int fds[2];
  if (pipe(fds) != 0)
    throw runtime_error (string("pipe: ") + strerror(errno));
  pid_t pid = fork();
  switch (pid) {
  case -1:
    {
      string err = string("fork: ") + strerror(errno);
      ::close(fds[0]);
      ::close(fds[1]);
      throw runtime_error (err);
    }
  case 0:
    ::close(fds[0]);
    if (fds[1] != 1) {
      dup2(fds[1], 1);
      ::close(fds[1]);
    }
    setenv("NOTMUCH_CONFIG", notmuch_config.c_str(), 1);
    execvp("notmuch", const_cast<char *const*> (av));
    cerr << "notmuch: " << strerror(errno) << endl;
    raise(SIGINT);
    _exit(127);
  }

  ::close(fds[1]);
  ifdstream in (fds[0]);
  ostringstream os;
  os << in.rdbuf();

  int status;
  if (waitpid(pid, &status, 0) == pid
      && WIFSIGNALED(pid) && WTERMSIG(status) == SIGINT)
    throw runtime_error ("could not run notmuch");
  return os.str();
}


notmuch_database_t *
notmuch_db::notmuch ()
{
  if (!notmuch_) {
    notmuch_status_t err =
      notmuch_database_open (maildir.c_str(),
			     NOTMUCH_DATABASE_MODE_READ_WRITE,
			     &notmuch_);
    if (err)
      throw runtime_error (maildir + ": "
			   + notmuch_status_to_string(err));
  }
  return notmuch_;
}

void
notmuch_db::close()
{
  if (notmuch_)
    notmuch_database_destroy (notmuch_);
  notmuch_ = nullptr;
}

int
main(int argc, char **argv)
{
  cout << sizeof (unique_ptr<notmuch_message_t,
		  decltype(&notmuch_message_destroy)>) << '\n';
  cout << sizeof (notmuch_db::message_t) << '\n';
  return 0;

  notmuch_db nm (notmuch_db::default_notmuch_config());
  for (auto x : nm.new_tags)
    cout << x << '\n';
  for (int i = 1; i < argc; i++)
    cout << "**** " << argv[i] << '\n' << nm.config_get (argv[i]) << '\n';
  return 0;
}
