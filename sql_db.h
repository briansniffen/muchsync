// -*- C++ -*-

#ifndef _SQL_DB_H
#define _SQL_DB_H 1

/** \file sql_db.h
 *  \brief Data structures representing information in SQL database.
 */

#include <exception>
#include <iosfwd>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "sqlstmt.h"

using std::string;

/** Writestamp is the pair (replica-id, version-number). */
using writestamp = std::pair<i64,i64>;

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

std::istream &read_writestamp (std::istream &in, writestamp &ws);

std::ostream &operator<< (std::ostream &os, const hash_info &hi);
std::istream &operator>> (std::istream &is, hash_info &hi);
std::ostream &operator<< (std::ostream &os, const tag_info &ti);
std::istream &operator>> (std::istream &is, tag_info &ti);

string trashname (const string &maildir, const string &hash);

#endif /* !_SQL_DB_H */
