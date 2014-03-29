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

extern const char dbvers[];

/** Writestamp is the pair (replica-id, version-number). */
using writestamp = std::pair<i64,i64>;
std::istream &read_writestamp (std::istream &in, writestamp &ws);

/** A version vector is a set of ::writestamps with distinct
 *  replica-ids.
 */
using versvector = std::unordered_map<i64,i64>;
string show_sync_vector (const versvector &vv);
std::istream &read_sync_vector (std::istream &sb, versvector &vv);
versvector get_sync_vector (sqlite3 *db);

/** Open the SQL database containing muchsync state.
 *
 *  If the file does not exist, it is created and initialized with a
 *  fresh database.
 */
sqlite3 *dbopen (const char *path, bool exclusive = false);


/*
 * Example: getconfig(db, "key", &sqlstmt_t::integer)
 */
template<typename T> T
getconfig (sqlite3 *db, const string &key)
{
  static const char query[] = "SELECT value FROM configuration WHERE key = ?;";
  return sqlstmt_t(db, query).param(key).step().template column<T>(0);
}
template<typename T> void
setconfig (sqlite3 *db, const string &key, const T &value)
{
  static const char query[] =
    "INSERT OR REPLACE INTO configuration VALUES (?, ?);";
  sqlstmt_t(db, query).param(key, value).step();
}


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

std::ostream &operator<< (std::ostream &os, const hash_info &hi);
std::istream &operator>> (std::istream &is, hash_info &hi);
std::ostream &operator<< (std::ostream &os, const tag_info &ti);
std::istream &operator>> (std::istream &is, tag_info &ti);

string trashname (const string &maildir, const string &hash);

#endif /* !_SQL_DB_H */
