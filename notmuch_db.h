// -*- C++ -*-

#include <exception>
#include <string>
#include <vector>
#include <unordered_set>
#include <notmuch.h>
#include <xapian.h>
#include "cleanup.h"

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
  using tags_t = std::unordered_set<string>;
  using message_t = unique_obj<notmuch_message_t, notmuch_message_destroy>;

  const string notmuch_config;
  const string maildir;
  const tags_t new_tags;
  const bool sync_flags;

  static string default_notmuch_config();
  static Xapian::docid get_docid(notmuch_message_t *msg) {
    struct fake_message {
      notmuch_database_t *notmuch;
      Xapian::docid doc_id;
    };
    /* This is massively evil, but looking through git history, doc_id
     * has been the second element of the structure for a long time. */
    return reinterpret_cast<const fake_message *>(msg)->doc_id;
  }

  string config_get(const char *);
  notmuch_db(string config);
  ~notmuch_db();

  void begin_atomic() {
    nmtry("begin_atomic", notmuch_database_begin_atomic(notmuch()));
  }
  void end_atomic() {
    nmtry("end_atomic", notmuch_database_end_atomic(notmuch()));
  }
  message_t get_message(const char *msgid);
  message_t add_message(const string &path,
			tags_t *new_tags = nullptr, 
			bool *was_new = nullptr);
  void set_tags(notmuch_message_t *msg, const tags_t &tags);

  notmuch_database_t *notmuch();
  void close();
};
