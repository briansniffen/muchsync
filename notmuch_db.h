// -*- C++ -*-

#include <exception>
#include <string>
#include <vector>
#include <unordered_set>
#include <notmuch.h>

using std::string;

struct notmuch_err : std::exception {
  const char *const op_;
  const notmuch_status_t status_;
  const string what_;
  notmuch_err(const char *op, notmuch_status_t status)
    : op_(op), status_(status),
      what_(op_ + string(": ") + notmuch_status_to_string(status_)) {}
  const char *what() const noexcept override { return what_.c_str(); }
};

class notmuch_db {
  notmuch_database_t *notmuch_ = nullptr;

  static void nmtry(const char *op, notmuch_status_t stat) {
    if (stat)
      throw notmuch_err (op, stat);
  }
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

  void set_tags(notmuch_message_t *msg, const std::unordered_set<string> &tags);
  notmuch_database_t *notmuch();
  void close();
};
