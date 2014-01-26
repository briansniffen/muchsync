
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include <limits>
#include <cstdio>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <openssl/rand.h>
#include <notmuch.h>
#include "muchsync.h"
#include "fdstream.h"

using namespace std;

static unordered_set<string> new_tags = notmuch_new_tags();

struct hash_info {
  string hash;
  i64 size = -1;
  string message_id;
  writestamp hash_stamp = {-1, -1};
  unordered_map<string,i64> dirs;
};

struct tag_info {
  string message_id;
  writestamp tag_stamp = {-1, -1};
  unordered_set<string> tags;
};

template<typename C, typename F> inline void
intercalate (const C &c, F &&each, function<void()> between)
{
  auto i = c.begin(), end = c.end();
  if (i != end) {
    each (i);
    while (++i != end) {
      between();
      each(i);
    }
  }
}

ostream &
operator<< (ostream &os, const hash_info &hi)
{
  os << "H " << hi.hash << ' ' << hi.size << ' '
     << permissive_percent_encode(hi.message_id)
     << " R" << hi.hash_stamp.first << '=' << hi.hash_stamp.second
     << " (";
  intercalate (hi.dirs,
	       [&](decltype(hi.dirs.begin()) i) {
		 os << i->second << '*' << permissive_percent_encode(i->first);
	       },
	       [&]() {os << ' ';});
  os << ')';
  return os;
}

istream &
operator>> (istream &is, hash_info &hi)
{
  {
    string msgid;
    input_match(is, 'H') >> hi.hash >> hi.size >> msgid;
    hi.message_id = percent_decode (msgid);
  }
  read_writestamp(is, hi.hash_stamp);
  input_match(is, '(');
  hi.dirs.clear();
  char c;
  while ((is >> skipws >> c) && c != ')') {
    is.putback (c);
    i64 nlinks;
    is >> nlinks;
    input_match(is, '*');
    string dir;
    is >> dir;
    if (is)
      hi.dirs.emplace (percent_decode (dir), nlinks);
  }
  return is;
}

ostream &
operator<< (ostream &os, const tag_info &ti)
{
  os << "T " << permissive_percent_encode(ti.message_id)
     << " R" << ti.tag_stamp.first << '=' << ti.tag_stamp.second
     << " (";
  intercalate (ti.tags,
	       [&](decltype(ti.tags.begin()) i) {
		 os << *i;
	       },
	       [&]() {os << ' ';});
  os << ')';
  return os;
}

istream &
operator>> (istream &is, tag_info &ti)
{
  {
    string msgid;
    input_match(is, 'T') >> msgid;
    ti.message_id = percent_decode (msgid);
  }
  read_writestamp(is, ti.tag_stamp);
  input_match(is, '(');
  ti.tags.clear();
  char c;
  while ((is >> skipws >> c) && c != ')') {
    is.putback (c);
    string tag;
    if (is >> tag)
      ti.tags.insert(tag);
  }
  return is;
}


struct hashmsg_info {
  string hash;
  i64 size = -1;
  string message_id;
  writestamp tag_stamp = {-1, -1};
  unordered_set<string> tags;
  writestamp dir_stamp = {-1, -1};
  unordered_map<string,i64> dirs;
};

class message_reader {
  static const char gethash_sql[];
  static const char gettags_sql[];

  const string maildir_;
  sqlstmt_t gethash_;
  sqlstmt_t gettags_;
  ifstream content_;
  hashmsg_info hi_;
  bool ok_ = false;
  string err_;
  vector<pair<string,string>> pathnames_;
  string openpath_;
public:
  message_reader(sqlite3 *db, const string &m) :
    maildir_(m), gethash_(db, gethash_sql), gettags_(db, gettags_sql) {}
  bool lookup(const string &hash);
  bool ok() const { return ok_; }
  const string &err() const { assert (!ok()); return err_; }
  const hashmsg_info &info() const { assert (ok()); return hi_; }
  bool present() const { assert (ok()); return !pathnames_.empty(); }
  const vector<pair<string,string>> &paths() const {
    assert (ok());
    return pathnames_;
  }
  streambuf *rdbuf();
  const string &openpath() {
    if (!rdbuf())
      throw runtime_error ("message_reader::openpath: no valid paths");
    return openpath_;
  }
  const string &maildir() const { return maildir_; }
};

class message_syncer {
  sqlite3 *db_;
  notmuch_database_t *notmuch_ = nullptr;

  writestamp mystamp_;
  sqlstmt_t lookup_hash_id_;
  sqlstmt_t create_hash_id_;
  sqlstmt_t update_hash_stamp_;
  sqlstmt_t update_links_;
  sqlstmt_t delete_links_;
  sqlstmt_t get_docid_;
  sqlstmt_t update_msgid_;
  sqlstmt_t delete_msgid_;

  unordered_map<string,i64> dir_ids_;

  static writestamp get_mystamp(sqlite3 *db);
  i64 get_hash_id(const string &hash, i64 size, const string &msgid);
  static constexpr i64 bad_dir_id = -1;
  i64 get_dir_id(const string &dir, bool create);
public:
  message_reader mr;

  message_syncer(sqlite3 *db, const string &m);
  ~message_syncer();
  notmuch_database_t *notmuch();
  void notmuch_close();

  bool sync_links(const versvector &remote_sync_vector,
		  const string &hash,
		  writestamp hash_stamp,
		  const unordered_map<string,i64> &dirs,
		  string *sourcep = nullptr);

  // Returns false if it needs a path to a copy of message in sourcep
  bool sync_message(const versvector &remote_sync_vector,
		    const hashmsg_info &remote_hashmsg_info,
		    string *sourcep = nullptr);
};


string
permissive_percent_encode (const string &raw)
{
  ostringstream outbuf;
  outbuf.fill('0');
  outbuf.setf(ios::hex, ios::basefield);
  for (char c : raw)
    if (c <= ' ' || c >= '\177' || c == '%' || c == '(' || c == ')')
      outbuf << '%' << setw(2) << int (uint8_t(c));
    else
      outbuf << c;
  return outbuf.str();
}

static void
sqlite_percent_encode (sqlite3_context *ctx, int argc, sqlite3_value **av)
{
  assert (argc == 1);
  string escaped = permissive_percent_encode (reinterpret_cast <const char *>
					      (sqlite3_value_text (av[0])));
  sqlite3_result_text (ctx, escaped.c_str(), escaped.size(),
		       SQLITE_TRANSIENT);
}

int
sqlite_register_percent_encode (sqlite3 *db)
{
  return sqlite3_create_function_v2 (db, "percent_encode", 1, SQLITE_UTF8,
				     nullptr, &sqlite_percent_encode, nullptr,
				     nullptr, nullptr);
}

string
show_hashmsg_info (const hashmsg_info &hi)
{
  ostringstream os;
  os << hi.hash << ' ' << hi.size << ' ' << hi.message_id << " R"
     << hi.tag_stamp.first << '=' << hi.tag_stamp.second
     << " (";
  bool first = true;
  for (auto s : hi.tags) {
    if (first)
      first = false;
    else
      os << ' ';
    os << s;
  }
  os << ") R" << hi.dir_stamp.first << '=' << hi.dir_stamp.second << " (";
  first = true;
  for (auto d : hi.dirs) {
    if (first)
      first = false;
    else
      os << ' ';
    os << d.second << '*' << d.first;
  }
  os << ')';
  return os.str();
}

static istream &
read_strings (istream &in, unordered_set<string> &out)
{
  char c;
  in >> c;
  if (c != '(') {
    in.setstate (ios_base::failbit);
    return in;
  }
  string content;
  getline (in, content, ')');
  istringstream is (content);
  string s;
  while (is >> s)
    out.insert (move (s));
  return in;
}

static istream &
read_dirs (istream &in, unordered_map<string,i64> &out)
{
  char c;
  in >> c;
  if (c != '(') {
    in.setstate (ios_base::failbit);
    return in;
  }
  string content;
  getline (in, content, ')');
  istringstream is (content);
  unsigned n;
  string s;
  while (is >> n >> c >> s) {
    if (c != '*') {
      in.setstate (ios_base::failbit);
      return in;
    }
    out[s] += n;
  }
  return in;
}

istream &
read_hashmsg_info (istream &in, hashmsg_info &outhi)
{
  hashmsg_info hi;
  in >> hi.hash;
  in >> hi.size;
  string t;
  in >> t;
  try { hi.message_id = percent_decode (t); }
  catch (...) { in.setstate(ios_base::failbit); }
  read_writestamp (in, hi.tag_stamp);
  read_strings (in, hi.tags);
  read_writestamp (in, hi.dir_stamp);
  read_dirs (in, hi.dirs);
  if (in)
    outhi = move(hi);
  return in;
}

const char message_reader::gethash_sql[] = R"(
SELECT dir_path, name, message_id, replica, version, size
FROM maildir_hashes JOIN maildir_files USING (hash_id)
                    JOIN maildir_dirs USING (dir_id)
WHERE hash = ? ORDER BY dir_id, name;)";
const char message_reader::gettags_sql[] =  R"(
SELECT tag, replica, version
FROM message_ids LEFT OUTER JOIN tags USING (docid)
WHERE message_id = ?;)";

bool
message_reader::lookup (const string &hash)
{
  hi_ = hashmsg_info();
  content_.close();
  pathnames_.clear();
  hi_.hash = hash;
  openpath_.clear();
  if (!gethash_.reset().param(hash).step().row()) {
    err_ = "510 hash not found";
    return ok_ = false;
  }
  hi_.message_id = gethash_.str(2);
  hi_.dir_stamp = { gethash_.integer(3), gethash_.integer(4) };
  hi_.size = gethash_.integer(5);

  gettags_.reset().param(gethash_.value(2)).step();
  if (gettags_.row())
    hi_.tag_stamp = { gettags_.integer(1), gettags_.integer(2) };
  for (; gettags_.row(); gettags_.step())
    hi_.tags.insert(gettags_.str(0));
    
  for (; gethash_.row(); gethash_.step()) {
    string dirpath (gethash_.str(0));
    pathnames_.emplace_back (dirpath,
			     maildir_ + "/" + dirpath + "/" + gethash_.str(1));
    ++hi_.dirs[dirpath];
  }
  return ok_ = true;
}

streambuf *
message_reader::rdbuf()
{
  assert (ok());
  if (content_.is_open()) {
    content_.seekg(0);
    return content_.rdbuf();
  }
  for (auto p : pathnames_) {
    content_.open (p.second, ios_base::in);
    if (content_.is_open()) {
      openpath_ = p.second;
      return content_.rdbuf();
    }
  }
  return nullptr;
}

inline i64
lookup_version (const versvector &vv, i64 replica)
{
  auto i = vv.find(replica);
  return i == vv.end() ? -1 : i->second;
}

message_syncer::message_syncer(sqlite3 *db, const string &m)
  : db_(db), mystamp_(get_mystamp(db)),
    lookup_hash_id_(db, "SELECT hash_id, size, message_id"
		    " FROM maildir_hashes WHERE hash = ?;"),
    create_hash_id_(db, "INSERT INTO maildir_hashes"
		    " (hash, size, message_id, replica, version)"
		    " VALUES (?, ?, ?, ?, ?);"),
    update_hash_stamp_(db, "UPDATE maildir_hashes "
		       "SET replica = ?, version = ? WHERE hash_id = ?;"),
    update_links_(db, "INSERT OR REPLACE INTO maildir_links"
		  " (hash_id, dir_id, link_count) VALUES (?, ?, ?);"),
    delete_links_(db, "DELETE FROM maildir_links"
		  " WHERE hash_id = ? & dir_id = ?;"),
    get_docid_(db, "SELECT docid FROM message_ids WHERE message_id = ?;"),
    update_msgid_(db, "INSERT OR REPLACE INTO"
		  " message_ids (message_id, docid, replica, version)"
		  " VALUES (?, ?, ?, ?);"),
    delete_msgid_(db, "DELETE FROM message_ids WHERE message_id = ?;"),
    mr(db, m)
{
  sqlstmt_t s (db, "SELECT dir_path, dir_id FROM maildir_dirs;");
  for (s.step(); s.row(); s.step())
    dir_ids_.emplace (s.str(0), s.integer(1));
}

message_syncer::~message_syncer()
{
  notmuch_close();
}

notmuch_database_t *
message_syncer::notmuch()
{
  if (notmuch_)
    return notmuch_;
  notmuch_status_t nmerr
    = notmuch_database_open (mr.maildir().c_str(),
			     NOTMUCH_DATABASE_MODE_READ_WRITE,
			     &notmuch_);
  if (nmerr)
    throw runtime_error (mr.maildir() + ": "
			 + notmuch_status_to_string (nmerr));
  return notmuch_;
}

void
message_syncer::notmuch_close()
{
  if (notmuch_) {
    notmuch_database_destroy (notmuch_);
    notmuch_ = nullptr;
  }
}

writestamp
message_syncer::get_mystamp(sqlite3 *db)
{
  sqlstmt_t s (db, "SELECT replica, version "
	       "FROM configuration JOIN sync_vector ON (value = replica) "
	       "WHERE key = 'self';");
  if (!s.step().row())
    throw runtime_error ("Cannot find myself in sync_vector");
  return { s.integer(0), s.integer(1) };
}

i64
message_syncer::get_hash_id (const string &hash, i64 size, const string &msgid)
{
  if (lookup_hash_id_.reset().param(hash).step().row()) {
    if (msgid != lookup_hash_id_.str(2))
      throw runtime_error ("disagreement over message ID of hash " + hash);
    if (size != lookup_hash_id_.integer(1))
      throw runtime_error ("disagreement over size of hash " + hash);
    return lookup_hash_id_.integer(0);
  }
  create_hash_id_.reset().param(hash, size, msgid, nullptr, nullptr).step();
  return sqlite3_last_insert_rowid(db_);
}

static bool
sanity_check_path (const string &path)
{
  if (path == "..")
    return false;
  if (path.size() < 3)
    return true;
  return (path.substr(0, 3) != "../"
	  && path.substr(path.size()-3) != "/.."
	  && path.find("/../") == string::npos);
}

inline bool
is_dir (const string &path)
{
  struct stat sb;
  return !stat (path.c_str(), &sb) && (errno = ENOTDIR, S_ISDIR (sb.st_mode));
}

bool
recursive_mkdir (string path)
{
  string::size_type n = 0;
  for (;;) {
    n = path.find_first_not_of ('/', n);
    if (n == string::npos)
      return true;
    n = path.find_first_of ('/', n);
    if (n != string::npos)
      path[n] = '\0';
    cerr << path.c_str() << '\n';
    if (!is_dir (path) && mkdir (path.c_str(), 0777))
      return false;
    if (n == string::npos)
      return true;
    path[n] = '/';
  };
}

i64
message_syncer::get_dir_id (const string &dir, bool create)
{
  auto i = dir_ids_.find(dir);
  if (i != dir_ids_.end())
    return i->second;

  if (!sanity_check_path (dir))
    throw runtime_error (dir + ": illegal directory name");
  if (!create)
    return bad_dir_id;
  string path = mr.maildir() + dir;
  if (!is_dir(path) && !recursive_mkdir(path))
    throw runtime_error (path + ": cannot create directory");
  
  sqlexec (db_, "INSERT INTO maildir_dirs (dir_path) VALUES (%Q);",
	   dir.c_str());
  i64 dir_id = sqlite3_last_insert_rowid (db_);
  dir_ids_.emplace (dir, dir_id);
  return dir_id;
}

bool
message_syncer::sync_links (const versvector &rvv,
			    const string &hash,
			    writestamp rws,
			    const unordered_map<string,i64> &dirs,
			    string *sourcep)
{
  return false;
#if 0
  static const hashmsg_info empty_hashmsg_info;
  const hashmsg_info &lhi = mr.lookup(rhi.hash) ? mr.info() : empty_hashmsg_info;

  bool links_conflict
    = (lhi.dir_stamp.second > lookup_version (rvv, lhi.dir_stamp.first)
       && rhi.dirs != lhi.dirs);
  bool deleting = rhi.dirs.empty() && (!links_conflict || lhi.dirs.empty());
  unordered_map<string,i64> needlinks (rhi.dirs);
  bool needsource = false;
  for (auto i : lhi.dirs)
    if ((needlinks[i.first] -= i.second) > 0)
      needsource = true;

  /* find copy of content */
  string source;
  if (needsource) {
    if (sourcep)
      source = *sourcep;
    else if (mr.ok() && mr.rdbuf())
      source = mr.openpath();
    else
      return false;
  }

  i64 hash_id = get_hash_id (rhi.hash, rhi.size, rhi.message_id);
  sqlexec (db_, "SAVEPOINT sync_message;");
  cleanup c (sqlexec, db_, "ROLLBACK TO sync_message;");

  /* Adjust link counts */
  for (auto li : needlinks)
    if (li.second != 0) {
      i64 dir_id = get_dir_id (li.first, li.second > 0);
      if (dir_id == bad_dir_id)
	continue;
      i64 newcount = find_default(0, lhi.dirs, li.first) + li.second;
      if (newcount > 0)
	update_links_.reset().param(hash_id, dir_id, newcount).step();
      else
	delete_links_.reset().param(hash_id, dir_id);
    }

  /* Set writestamp for new link counts */
  const writestamp *wsp = links_conflict ? &mystamp_ : &rhi.dir_stamp;
  update_hash_stamp_.reset().param(wsp->first, wsp->second, hash_id).step();

  notmuch_message_t *message = nullptr;
  cleanup _msg (notmuch_message_destroy, message);

  /* add missing links */
  for (auto li : needlinks)
    for (; li.second > 0; --li.second) {
      string newname = li.first + "/" + maildir_name();
      string target = mr.maildir() + "/" + newname;
      if (link (source.c_str(), target.c_str()))
	throw runtime_error (string("link (\"") + source + "\", \""
			     + target + "\"): " + strerror(errno));
      notmuch_database_add_message (notmuch_, newname.c_str(),
				    message ? nullptr : &message);
    }
  /* remove extra links */
  if (!links_conflict && mr.ok())
    for (auto p : mr.paths()) {
      i64 &n = needlinks[p.first];
      if (n < 0) {
	if (deleting) {
	  string trash = (mr.maildir() + muchsync_trashdir + "/" + rhi.hash);
	  if (!rename (p.second.c_str(), trash.c_str()))
	    ++n;
	}
	else if (!unlink (p.second.c_str()))
	  ++n;
	notmuch_database_remove_message (notmuch_, p.second.c_str());
      }
    }

  assert (!(deleting && (message || needsource)));
  if (deleting) {
    delete_msgid_.reset().param(rhi.message_id).step();
    c.disable();
    sqlexec (db_, "RELEASE sync_message;");
    return true;
  }

  if (!message) {
    notmuch_status_t err =
      notmuch_database_find_message (notmuch_, rhi.message_id.c_str(),
				     &message);
    if (err)
      throw runtime_error ("cannot find " + rhi.message_id + " in database: "
			   + notmuch_status_to_string(err));
  }
  i64 docid = _notmuch_message_get_doc_id (message);

  bool tags_conflict
    = (lhi.tag_stamp.second > lookup_version (rvv, lhi.tag_stamp.first)
       && rhi.tags != lhi.tags);
  unordered_set<string> newtags (rhi.tags);
  if (tags_conflict) {
    // Logically OR most tags
    for (auto i : lhi.tags)
      newtags.insert(i);
    // But logically AND new_tags
    for (auto i : new_tags)
      if (rhi.tags.find(i) == rhi.tags.end()
	  || lhi.tags.find(i) == lhi.tags.end())
	newtags.erase(i);
  }

  wsp = tags_conflict ? &mystamp_ : &rhi.tag_stamp;
  update_hash_stamp_.reset()
    .param(rhi.message_id, docid, wsp->first, wsp->second, hash_id).step();

  c.disable();
  sqlexec (db_, "RELEASE sync_message;");
  return true;
#endif
}



//extern "C" unsigned int _notmuch_message_get_doc_id (notmuch_message_t *);

bool
message_syncer::sync_message (const versvector &rvv,
			      const hashmsg_info &rhi,
			      string *sourcep)
{
  static const hashmsg_info empty_hashmsg_info;
  const hashmsg_info &lhi = mr.lookup(rhi.hash) ? mr.info() : empty_hashmsg_info;

  bool links_conflict
    = (lhi.dir_stamp.second > lookup_version (rvv, lhi.dir_stamp.first)
       && rhi.dirs != lhi.dirs);
  bool deleting = rhi.dirs.empty() && (!links_conflict || lhi.dirs.empty());
  unordered_map<string,i64> needlinks (rhi.dirs);
  bool needsource = false;
  for (auto i : lhi.dirs)
    if ((needlinks[i.first] -= i.second) > 0)
      needsource = true;

  /* find copy of content */
  string source;
  if (needsource) {
    if (sourcep)
      source = *sourcep;
    else if (mr.ok() && mr.rdbuf())
      source = mr.openpath();
    else
      return false;
  }

  i64 hash_id = get_hash_id (rhi.hash, rhi.size, rhi.message_id);
  sqlexec (db_, "SAVEPOINT sync_message;");
  cleanup c (sqlexec, db_, "ROLLBACK TO sync_message;");

  /* Adjust link counts */
  for (auto li : needlinks)
    if (li.second != 0) {
      i64 dir_id = get_dir_id (li.first, li.second > 0);
      if (dir_id == bad_dir_id)
	continue;
      i64 newcount = find_default(0, lhi.dirs, li.first) + li.second;
      if (newcount > 0)
	update_links_.reset().param(hash_id, dir_id, newcount).step();
      else
	delete_links_.reset().param(hash_id, dir_id);
    }

  /* Set writestamp for new link counts */
  const writestamp *wsp = links_conflict ? &mystamp_ : &rhi.dir_stamp;
  update_hash_stamp_.reset().param(wsp->first, wsp->second, hash_id).step();

  notmuch_message_t *message = nullptr;
  cleanup _msg (notmuch_message_destroy, message);

  /* add missing links */
  for (auto li : needlinks)
    for (; li.second > 0; --li.second) {
      string newname = li.first + "/" + maildir_name();
      string target = mr.maildir() + "/" + newname;
      if (link (source.c_str(), target.c_str()))
	throw runtime_error (string("link (\"") + source + "\", \""
			     + target + "\"): " + strerror(errno));
      notmuch_database_add_message (notmuch_, newname.c_str(),
				    message ? nullptr : &message);
    }
  /* remove extra links */
  if (!links_conflict && mr.ok())
    for (auto p : mr.paths()) {
      i64 &n = needlinks[p.first];
      if (n < 0) {
	if (deleting) {
	  string trash = (mr.maildir() + muchsync_trashdir + "/" + rhi.hash);
	  if (!rename (p.second.c_str(), trash.c_str()))
	    ++n;
	}
	else if (!unlink (p.second.c_str()))
	  ++n;
	notmuch_database_remove_message (notmuch_, p.second.c_str());
      }
    }

  assert (!(deleting && (message || needsource)));
  if (deleting) {
    delete_msgid_.reset().param(rhi.message_id).step();
    c.disable();
    sqlexec (db_, "RELEASE sync_message;");
    return true;
  }

  if (!message) {
    notmuch_status_t err =
      notmuch_database_find_message (notmuch_, rhi.message_id.c_str(),
				     &message);
    if (err)
      throw runtime_error ("cannot find " + rhi.message_id + " in database: "
			   + notmuch_status_to_string(err));
  }
#if 0
  i64 docid = _notmuch_message_get_doc_id (message);

  bool tags_conflict
    = (lhi.tag_stamp.second > lookup_version (rvv, lhi.tag_stamp.first)
       && rhi.tags != lhi.tags);
  unordered_set<string> newtags (rhi.tags);
  if (tags_conflict) {
    // Logically OR most tags
    for (auto i : lhi.tags)
      newtags.insert(i);
    // But logically AND new_tags
    for (auto i : new_tags)
      if (rhi.tags.find(i) == rhi.tags.end()
	  || lhi.tags.find(i) == lhi.tags.end())
	newtags.erase(i);
  }

  wsp = tags_conflict ? &mystamp_ : &rhi.tag_stamp;
  update_hash_stamp_.reset()
    .param(rhi.message_id, docid, wsp->first, wsp->second, hash_id).step();

  c.disable();
  sqlexec (db_, "RELEASE sync_message;");
#endif
  return true;
}

static void
set_peer_vector (sqlite3 *sqldb, const versvector &vv)
{
  sqlexec (sqldb, R"(
DROP TABLE IF EXISTS peer_vector;
CREATE TEMP TABLE peer_vector (replica INTEGER PRIMARY KEY,
  known_version INTEGER);
INSERT OR REPLACE INTO peer_vector
  SELECT DISTINCT replica, -1 FROM message_ids;
INSERT OR REPLACE INTO peer_vector
  SELECT DISTINCT replica, -1 FROM maildir_hashes;
)");
  sqlstmt_t pvadd (sqldb, "INSERT OR REPLACE INTO"
		   " peer_vector (replica, known_version) VALUES (?, ?);");
  for (writestamp ws : vv)
    pvadd.reset().param(ws.first, ws.second).step();
}

static void
cmd_hsyn (sqlite3 *sqldb, const versvector &vv)
{
  versvector myvv = get_sync_vector(sqldb);
  set_peer_vector (sqldb, vv);

  unordered_map<i64,string> dirs;
  {
    sqlstmt_t d (sqldb, "SELECT dir_id, dir_path FROM maildir_dirs;");
    while (d.step().row())
      dirs.emplace (d.integer(0), d.str(1));
  }

  sqlstmt_t changed (sqldb, R"(
SELECT h.hash_id, hash, size, message_id, h.replica, h.version,
       dir_id, link_count
FROM (peer_vector p JOIN maildir_hashes h
      ON ((p.replica = h.replica) & (p.known_version < h.version)))
LEFT OUTER JOIN maildir_links USING (hash_id);)");
  
  hash_info hi;
  changed.step();
  while (changed.row()) {
    i64 hash_id = changed.integer(0);
    hi.hash = changed.str(1);
    hi.size = changed.integer(2);
    hi.message_id = changed.str(3);
    hi.hash_stamp.first = changed.integer(4);
    hi.hash_stamp.second = changed.integer(5);
    hi.dirs.clear();
    if (changed.null(6))
      changed.step();
    else {
      hi.dirs.emplace(dirs[changed.integer(6)], changed.integer(7));
      while (changed.step().row() && changed.integer(0) == hash_id)
	hi.dirs.emplace(dirs[changed.integer(6)], changed.integer(7));
    }
    cout << "210-" << hi << '\n';
  }

  cout << "210 " << show_sync_vector(myvv) << '\n';
}

static void
cmd_tsyn (sqlite3 *sqldb, const versvector &vv)
{
  versvector myvv = get_sync_vector(sqldb);
  set_peer_vector (sqldb, vv);
  sqlstmt_t changed (sqldb, R"(
SELECT m.docid, m.message_id, m.replica, m.version, tags.tag
FROM (peer_vector p JOIN message_ids m
      ON ((p.replica = m.replica) & (p.known_version < m.version)))
      LEFT OUTER JOIN tags USING (docid);)");

  tag_info ti;
  changed.step();
  while (changed.row()) {
    i64 docid = changed.integer(0);
    ti.message_id = changed.str(1);
    ti.tag_stamp.first = changed.integer(2);
    ti.tag_stamp.second = changed.integer(3);
    ti.tags.clear();
    if (changed.null(4))
      changed.step();
    else {
      ti.tags.insert (changed.str(4));
      while (changed.step().row() && changed.integer(0) == docid)
	ti.tags.insert (changed.str(4));
    }
    cout << "210-" << ti << '\n';
  }

  cout << "210 " << show_sync_vector(myvv) << '\n';
}

static void
cmd_sync (sqlite3 *sqldb, const versvector &vv)
{
  sqlexec (sqldb, R"(
DROP TABLE IF EXISTS peer_vector;
CREATE TABLE peer_vector (replica INTEGER PRIMARY KEY,
known_version INTEGER);
)");
  sqlstmt_t pvadd (sqldb, "INSERT INTO peer_vector (replica, known_version)"
		   " VALUES (?, ?);");
  for (writestamp ws : vv)
    pvadd.reset().param(ws.first, ws.second).step();

  sqlstmt_t changed (sqldb, R"(
SELECT h.hash, h.replica, h.version,
       x.message_id, x.replica, x.version,
       cattags, catdirs, h.size
FROM 
    (maildir_hashes h LEFT OUTER JOIN peer_vector pvh USING (replica))
      LEFT OUTER JOIN
    (message_ids x LEFT OUTER JOIN peer_vector pvx USING (replica))
        USING (message_id)
      LEFT OUTER JOIN
    (SELECT docid, group_concat(tag, ' ') cattags FROM tags GROUP BY docid)
        USING (docid)
      LEFT OUTER JOIN
    (SELECT hash_id,
            group_concat(link_count || '*' || percent_encode(dir_path), ' ')
                           AS catdirs
     FROM maildir_links NATURAL JOIN maildir_dirs GROUP BY hash_id)
        USING(hash_id)
  WHERE (h.version > ifnull(pvh.known_version,-1))
        | (x.version > ifnull(pvx.known_version,-1))
;)");

  for (changed.step(); changed.row(); changed.step()) {
    cout << "210-" << changed.str(0) << ' '
	 << changed.str(8) << ' '
	 << permissive_percent_encode (changed.str(3))
	 << " R" << changed.integer(4) << '=' << changed.integer(5)
	 << " (" << changed.str(6)
	 << ") R" << changed.integer(1) << '=' << changed.integer(2)
	 << " (" << changed.str(7) << ")\n";
  }

  cout << "210 Synchronized " << show_sync_vector(vv) << '\n';
}

void
muchsync_server (sqlite3 *db, const string &maildir)
{
  {
    int ifd = spawn_infinite_input_buffer (0);
    switch (ifd) {
    case -1:
      exit (1);
    case 0:
      break;
    default:
      dup2 (ifd, 0);
      close (ifd);
    }
  }

  sqlite_register_percent_encode (db);
  message_reader mr (db, maildir);

  cout << "200 " << dbvers << '\n';
  string cmd;
  while ((cin >> cmd).good()) {
    if (cmd == "quit") {
      cout << "200 goodbye\n";
      return;
    }
    else if (cmd == "vect") {
      cout << "200 " << show_sync_vector (get_sync_vector (db)) << '\n';
    }
    else if (cmd == "send") {
      string hash;
      cin >> hash;
      streambuf *sb;
      if (mr.lookup(hash) && (sb = mr.rdbuf()))
	cout << "220-" << show_hashmsg_info (mr.info()) << '\n'
	     << sb
	     << "220 " << mr.info().hash << '\n';
      else if (mr.ok())
	cout << "420 cannot open file\n";
      else
	cout << mr.err() << '\n';
    }
    else if (cmd == "info") {
      string hash;
      cin >> hash;
      if (mr.lookup(hash))
	cout << "210-" << mr.info().size << " bytes\n"
	     << "210 " << show_hashmsg_info (mr.info()) << '\n';
      else
	cout << mr.err() << '\n';
    }
    else if (cmd == "sync") {
      versvector vv;
      string tail;
      if (!getline(cin, tail))
	cout << "500 could not parse vector\n";
      else {
	istringstream tailstream (tail);
	if (!read_sync_vector(tailstream, vv))
	  cout << "500 could not parse vector\n";
	else
	  cmd_sync (db, vv);
      }
      continue;
    }
    else if (cmd == "hsyn") {
      versvector vv;
      string tail;
      if (!getline(cin, tail))
	cout << "500 could not parse vector\n";
      else {
	istringstream tailstream (tail);
	if (!read_sync_vector(tailstream, vv))
	  cout << "500 could not parse vector\n";
	else
	  cmd_hsyn (db, vv);
      }
      continue;
    }
    else if (cmd == "tsyn") {
      versvector vv;
      string tail;
      if (!getline(cin, tail))
	cout << "500 could not parse vector\n";
      else {
	istringstream tailstream (tail);
	if (!read_sync_vector(tailstream, vv))
	  cout << "500 could not parse vector\n";
	else
	  cmd_tsyn (db, vv);
      }
      continue;
    }
    else
      cout << "500 unknown verb " << cmd << '\n';
    cin.ignore (numeric_limits<streamsize>::max(), '\n');
  }
}

static istream &
get_response (istream &in, string &line)
{
  if (!getline (in, line))
    throw runtime_error ("premature EOF");
  //cerr << "read " << line << '\n';
  if (line.empty())
    throw runtime_error ("unexpected empty line");
  if (line.size() < 4)
    throw runtime_error ("unexpected short line");
  if (line.front() != '2')
    throw runtime_error ("bad response: " + line);
  return in;
}

static string
myhostname()
{
  char buf[257];
  buf[sizeof(buf) - 1] = '\0';
  if (gethostname (buf, sizeof(buf) - 1))
    throw runtime_error (string("gethsotname: ") + strerror (errno));
  return buf;
}

static uint32_t
randint()
{
  uint32_t v;
  if (RAND_pseudo_bytes ((unsigned char *) &v, sizeof (v)) == -1)
    throw runtime_error ("RAND_pseudo_bytes failed");
  return v;
}

string
maildir_name ()
{
  static string hostname = myhostname();
  static int pid = getpid();
  static int ndeliveries = 0;

  ostringstream os;
  struct timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  os << ts.tv_sec << ".M" << ts.tv_nsec << 'P' << pid
     << 'Q' << ++ndeliveries
     << 'R' << setfill('0') << hex << setw(2 * sizeof(randint())) << randint()
     << '.' << hostname;
  return os.str();
}

void
muchsync_client (sqlite3 *db, const string &maildir,
		 int ac, char *const *av)
{
  ostringstream os;
  os << opt_ssh << ' ' << av[0] << ' ' << opt_remote_muchsync_path
     << " --server";
  for (int i = 1; i < ac; i++)
    os << ' ' << av[i];
  string cmd (os.str());
  int cmdfd = cmd_iofd (cmd);
  ofdstream out (cmdfd);
  ifdstream in (spawn_infinite_input_buffer (cmdfd));
  in.tie (&out);

  /* Any work done here gets overlapped with server */
  sync_local_data (db, maildir);
  versvector localvv {get_sync_vector (db)}, remotevv;
  message_syncer msgsync (db, maildir);
  i64 pending = 0;

  notmuch_database_t *notmuch;
  if (auto err = notmuch_database_open (maildir.c_str(),
					NOTMUCH_DATABASE_MODE_READ_WRITE,
					&notmuch))
    throw runtime_error (string("notmuch: ") + notmuch_status_to_string(err));
  cleanup _c (notmuch_database_destroy, notmuch);

  string line;
  get_response (in, line);

  out << "vect\n";
  get_response (in, line);
  istringstream is (line.substr(4));
  if (!read_sync_vector(is, remotevv))
    throw runtime_error ("cannot parse version vector " + line.substr(4));

  //cerr << "you got " << show_sync_vector(remotevv) << '\n';

  out << "sync " << show_sync_vector(localvv) << '\n';
  while (get_response (in, line) && line.at(3) == '-') {
    is.str(line.substr(4));
    hashmsg_info hi;
    if (!read_hashmsg_info(is, hi))
      throw runtime_error ("could not parse hashmsg_info: " + line.substr(4));
    if (!msgsync.sync_message (remotevv, hi)) {
      out << "send " << hi.hash << '\n';
      pending++;
    }
    //cerr << show_hashmsg_info (hi) << '\n';
  }

  for (; pending > 0; pending--) {
    //get_response (in, line);
  }
}
