// -*- C++ -*-

#include <exception>
#include <iosfwd>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "sqlstmt.h"

using std::string;

struct hash_info {
  string hash;
  i64 size = -1;
  string message_id;
  writestamp hash_stamp = {0, 0};
  std::unordered_map<string,i64> dirs;
};

class hash_lookup {
  sqlstmt_t gethash_;
  sqlstmt_t getlinks_;
  sqlstmt_t makehash_;
  bool ok_ = false;
  hash_info hi_;
  i64 hash_id_;
  std::vector<std::pair<string,string>> links_;
  std::ifstream content_;
  i64 docid_;
public:
  const string maildir;

  hash_lookup(const string &maildir, sqlite3 *db);
  bool lookup(const string &hash);
  void create(const hash_info &info);
  bool ok() const { return ok_; }
  i64 hash_id() const { assert (ok()); return hash_id_; }
  const hash_info &info() const { assert (ok()); return hi_; }
  const std::vector<std::pair<string,string>> &links() const { 
    assert (ok());
    return links_;
  }
  i64 docid() const { assert (nlinks()); return docid_; }
  int nlinks() const { return links().size(); }
  string link_path(int i) const {
    auto &lnk = links().at(i);
    return maildir + "/" + lnk.first + "/" + lnk.second;
  }
  bool get_pathname(string *path, bool *from_trash = nullptr) const;
  std::streambuf *content();
};

struct tag_info {
  string message_id;
  writestamp tag_stamp = {0, 0};
  std::unordered_set<string> tags;
};

class tag_lookup {
  sqlstmt_t getmsg_;
  sqlstmt_t gettags_;
  bool ok_ = false;
  tag_info ti_;
  i64 docid_;
public:
  tag_lookup (sqlite3 *db);
  bool lookup(const string &msgid);
  bool ok() const { return ok_; }
  i64 docid() const { assert (ok()); return docid_; }
  const tag_info &info() const { assert (ok()); return ti_; }
};

class msg_sync {
  sqlite3 *db_;
  notmuch_db &nm_;
  sqlstmt_t update_hash_stamp_;
  sqlstmt_t add_file_;
  sqlstmt_t del_file_;
  sqlstmt_t set_link_count_;
  sqlstmt_t delete_link_count_;
  sqlstmt_t clear_tags_;
  sqlstmt_t add_tag_;
  sqlstmt_t update_message_id_stamp_;
  sqlstmt_t record_docid_;
  std::unordered_map<string,i64> dir_ids_;
  std::pair<i64,i64> mystamp_;

  i64 get_dir_docid (const string &dir);
public:
  hash_lookup hashdb;
  tag_lookup tagdb;
  msg_sync(notmuch_db &nm, sqlite3 *db);
  bool hash_sync(const versvector &remote_sync_vector,
		 const hash_info &remote_hash_info,
		 const string *sourcefile, const tag_info *tip);
  bool tag_sync(const versvector &remote_sync_vector,
		const tag_info &remote_tag_info);
  void commit();
};

std::ostream &operator<< (std::ostream &os, const hash_info &hi);
std::istream &operator>> (std::istream &is, hash_info &hi);
std::ostream &operator<< (std::ostream &os, const tag_info &ti);
std::istream &operator>> (std::istream &is, tag_info &ti);

inline bool
hash_ok (const string &hash)
{
  if (hash.size() != 2*hash_ctx::output_bytes)
    return false;
  for (char c : hash)
    if (c < '0' || c > 'f' || (c > '9' && c < 'a'))
      return false;
  return true;
}

inline string
trashname (const string &maildir, const string &hash)
{
  if (!hash_ok(hash))
    throw std::runtime_error ("illegal hash: " + hash);
  return maildir + muchsync_trashdir + "/" +
    hash.substr(0,2) + "/" + hash.substr(2);
}

