// -*- C++ -*-

#include <string>
#include <vector>
#include <unordered_set>
#include <notmuch.h>

using std::string;

class notmuch_db {
  notmuch_database_t *notmuch_ = nullptr;

  string run_notmuch(const char **av);

public:
  const string notmuch_config;
  const string maildir;
  const std::unordered_set<string> new_tags;
  const bool sync_flags;

  static string default_notmuch_config();
  string config_get(const char *);
  notmuch_db(string config);
  ~notmuch_db();

  notmuch_database_t *notmuch();
  void close();
};
