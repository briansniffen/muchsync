// -*- C++ -*-

#include <string>
#include <vector>
#include <notmuch.h>

using std::string;

class notmuch_db {
public:				// XXX
  notmuch_database_t *notmuch_ = nullptr;
  string maildir_;

  string run_notmuch(std::vector<const char *> &av);

public:
  const string notmuch_config;

  static string default_notmuch_config();
  notmuch_db(string config);
  ~notmuch_db();

  notmuch_database_t *notmuch();
  void close();
};
