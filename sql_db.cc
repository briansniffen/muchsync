
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <openssl/rand.h>
#include "misc.h"
#include "sql_db.h"

using namespace std;

const char dbvers[] = "muchsync 0";

const char muchsync_schema[] = R"(
-- General table
CREATE TABLE configuration (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT);
CREATE TABLE sync_vector (
  replica INTEGER PRIMARY KEY,
  version INTEGER);

-- Shadow copy of the Xapian database to detect changes
CREATE TABLE xapian_dirs (
  dir_path TEXT UNIQUE NOT NULL,
  dir_docid INTEGER PRIMARY KEY,
  dir_mtime INTEGER);
CREATE TABLE tags (
  tag TEXT NOT NULL,
  docid INTEGER NOT NULL,
  UNIQUE (docid, tag),
  UNIQUE (tag, docid));
CREATE TABLE message_ids (
  message_id TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY,
  replica INTEGER,
  version INTEGER);
CREATE INDEX message_ids_writestamp ON message_ids (replica, version);
CREATE TABLE xapian_files (
  dir_docid INTEGER NOT NULL,
  name TEXT NOT NULL,
  docid INTEGER,
  mtime REAL,
  inode INTEGER,
  hash_id INGEGER,
  PRIMARY KEY (dir_docid, name));
CREATE INDEX xapian_files_hash_id ON xapian_files (hash_id, dir_docid);
CREATE TABLE maildir_hashes (
  hash_id INTEGER PRIMARY KEY,
  hash TEXT UNIQUE NOT NULL,
  size INTEGER,
  message_id TEXT,
  replica INTEGER,
  version INTEGER);
CREATE INDEX maildir_hashes_message_id ON maildir_hashes (message_id);
CREATE INDEX maildir_hashes_writestamp ON maildir_hashes (replica, version);
CREATE TABLE xapian_nlinks (
  hash_id INTEGER NOT NULL,
  dir_docid INTEGER NOT NULL,
  link_count INTEGER,
  PRIMARY KEY (hash_id, dir_docid));
)";

static sqlite3 *
dbcreate (const char *path)
{
  i64 self = 0;
  if (RAND_pseudo_bytes ((unsigned char *) &self, sizeof (self)) == -1
      || self == 0) {
    cerr << "RAND_pseudo_bytes failed\n";
    return nullptr;
  }
  self &= ~(i64 (1) << 63);

  sqlite3 *db = nullptr;
  int err = sqlite3_open_v2 (path, &db,
			     SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, nullptr);
  if (err) {
    cerr << path << ": " << sqlite3_errstr (err) << '\n';
    return nullptr;
  }
  sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");

  try {
    sqlexec (db, "BEGIN;");
    sqlexec (db, muchsync_schema);
    setconfig (db, "dbvers", dbvers);
    setconfig (db, "self", self);
    sqlexec (db, "INSERT INTO sync_vector (replica, version)"
	     " VALUES (%lld, 1);", self);
    sqlexec (db, "COMMIT;");
  } catch (sqlerr_t exc) {
    sqlite3_close_v2 (db);
    cerr << exc.what () << '\n';
    return nullptr;
  }
  return db;
}

sqlite3 *
dbopen (const char *path, bool exclusive)
{
  sqlite3 *db = nullptr;
  if (access (path, 0) && errno == ENOENT)
    db = dbcreate (path);
  else {
    sqlite3_open_v2 (path, &db, SQLITE_OPEN_READWRITE, nullptr);
    if (exclusive)
      sqlexec(db, "PRAGMA locking_mode=EXCLUSIVE;");
  }
  if (!db)
    return nullptr;

  sqlexec (db, "PRAGMA secure_delete = 0;");

  try {
    if (getconfig<string> (db, "dbvers") != dbvers) {
      cerr << path << ": invalid database version\n";
      sqlite3_close_v2 (db);
      return nullptr;
    }
    getconfig<i64> (db, "self");
  }
  catch (sqldone_t) {
    cerr << path << ": invalid configuration\n";
    sqlite3_close_v2 (db);
    return nullptr;
  }
  catch (sqlerr_t &e) {
    cerr << path << ": " << e.what() << '\n';
    sqlite3_close_v2 (db);
    return nullptr;
  }

  return db;
}


istream &
read_writestamp (istream &in, writestamp &ws)
{
  input_match (in, 'R');
  in >> ws.first;
  input_match (in, '=');
  in >> ws.second;
  return in;
}

istream &
read_sync_vector (istream &in, versvector &vv)
{
  input_match (in, '<');
  vv.clear();
  for (;;) {
    char c;
    if ((in >> c) && c == '>')
      return in;
    in.unget();
    writestamp ws;
    if (!read_writestamp (in, ws))
      break;
    vv.insert (ws);
    if (!(in >> c) || c == '>')
      break;
    if (c != ',') {
      in.setstate (ios_base::failbit);
      break;
    }
  }
  return in;
}

string
show_sync_vector (const versvector &vv)
{
  ostringstream sb;
  sb << '<';
  bool first = true;
  for (auto ws : vv) {
    if (first)
      first = false;
    else
      sb << ",";
    sb << 'R' << ws.first << '=' << ws.second;
  }
  sb << '>';
  return sb.str();
}

static string
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
  os << "L " << hi.hash << ' ' << hi.size << ' '
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
  string hash, msgid;
  size_t size;
  writestamp stamp;
  decltype(hi.dirs) d;

  input_match(is, 'L') >> hash >> size >> msgid;
  if (is && !hash_ok(hash))
    is.setstate (ios_base::failbit);
  read_writestamp(is, stamp);
  input_match(is, '(');
  char c;
  while ((is >> skipws >> c) && c != ')') {
    is.putback (c);
    i64 nlinks;
    is >> nlinks;
    input_match(is, '*');
    string dir;
    is >> dir;
    if (dir.back() == ')') {
      is.putback (')');
      dir.resize(dir.size()-1);
    }
    if (!dir.empty())
      d.emplace (percent_decode (dir), nlinks);
  }

  if (is.good()) {
    hi.hash = hash;
    hi.size = size;
    hi.message_id = percent_decode (msgid);
    hi.hash_stamp = stamp;
    hi.dirs = move(d);
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
    is >> tag;
    if (tag.back() == ')') {
      is.putback (')');
      tag.resize(tag.size()-1);
    }
    if (!tag.empty())
      ti.tags.insert(tag);
  }
  return is;
}

hash_lookup::hash_lookup (const string &m, sqlite3 *db)
  : gethash_(db, "SELECT hash_id, size, message_id, replica, version"
	     " FROM maildir_hashes WHERE hash = ?;"),
    getlinks_(db, "SELECT dir_path, name, docid"
	      " FROM xapian_files JOIN xapian_dirs USING (dir_docid)"
	      " WHERE hash_id = ?;"),
    makehash_(db, "INSERT INTO maildir_hashes"
	      " (hash, size, message_id, replica, version)"
	      " VALUES (?, ?, ?, ?, ?);"),
    maildir(m)
{
}

bool
hash_lookup::lookup (const string &hash)
{
  ok_ = false;
  content_.close();
  if (!gethash_.reset().param(hash).step().row())
    return false;
  hash_id_ = gethash_.integer(0);
  hi_.hash = hash;
  hi_.size = gethash_.integer(1);
  hi_.message_id = gethash_.str(2);
  hi_.hash_stamp.first = gethash_.integer(3);
  hi_.hash_stamp.second = gethash_.integer(4);
  hi_.dirs.clear();
  links_.clear();
  docid_ = -1;
  for (getlinks_.reset().param(hash_id_).step();
       getlinks_.row(); getlinks_.step()) {
    string dir = getlinks_.str(0), name = getlinks_.str(1);
    ++hi_.dirs[dir];
    links_.emplace_back(dir, name);
    if (docid_ == -1)
      docid_ = getlinks_.integer(2);
  }
  return ok_ = true;
}

void
hash_lookup::create (const hash_info &rhi)
{
  ok_ = false;
  content_.close();
  makehash_.reset().param(rhi.hash, rhi.size, rhi.message_id,
			  rhi.hash_stamp.first, rhi.hash_stamp.second).step();
  hi_.hash = rhi.hash;
  hi_.size = rhi.size;
  hi_.message_id = rhi.message_id;
  hi_.hash_stamp = rhi.hash_stamp;
  hi_.dirs.clear();
  hash_id_ = sqlite3_last_insert_rowid(sqlite3_db_handle(makehash_.get()));
  ok_ = true;
}

bool
hash_lookup::get_pathname(string *out, bool *from_trash) const
{
  struct stat sb;
  string path;
  for (int i = 0, e = nlinks(); i < e; i++) {
    path = link_path(i);
    if (!stat(path.c_str(), &sb) && S_ISREG(sb.st_mode)
	&& sb.st_size == hi_.size) {
      if (out)
	*out = move(path);
      if (from_trash)
	*from_trash = false;
      return true;
    }
  }

  path = trashname(maildir, hi_.hash);
  int fd = open (path.c_str(), O_RDWR);
  if (fd < 0)
    return false;

  // Check size [not really necessary]
  if (fstat(fd, &sb) || sb.st_size != hi_.size) {
    close (fd);
    cerr << "deleting file with bad size " << path << '\n';
    unlink(path.c_str());
    return false;
  }

  // Check hash
  int n;
  char buf[16384];
  hash_ctx ctx;
  while ((n = read(fd, buf, sizeof(buf))) > 0)
    ctx.update(buf, n);
  if (hi_.hash != ctx.final()) {
    close(fd);
    cerr << "deleting corrupt file " << path << '\n';
    unlink(path.c_str());
    return false;
  }

  // Found it in the trash
  fsync(fd);			// Might just have downloaded it
  close(fd);
  if (out)
    *out = move(path);
  if (from_trash)
    *from_trash = true;
  return true;
}

streambuf *
hash_lookup::content()
{
  if (content_.is_open()) {
    content_.seekg(0);
    return content_.rdbuf();
  }
  for (int i = 0, e = nlinks(); i < e; i++) {
    content_.open (link_path(i));
    if (content_.is_open())
      return content_.rdbuf();
  }
  return nullptr;
}

tag_lookup::tag_lookup (sqlite3 *db)
  : getmsg_(db, "SELECT docid, replica, version"
	    " FROM message_ids WHERE message_id = ?;"),
    gettags_(db, "SELECT tag FROM tags WHERE docid = ?;")
{
}

bool
tag_lookup::lookup (const string &msgid)
{
  ok_ = false;
  if (!getmsg_.reset().param(msgid).step().row())
    return false;
  ti_.message_id = msgid;
  docid_ = getmsg_.integer(0);
  ti_.tag_stamp.first = getmsg_.integer(1);
  ti_.tag_stamp.second = getmsg_.integer(2);
  ti_.tags.clear();
  for (gettags_.reset().param(docid_).step(); gettags_.row(); gettags_.step())
    ti_.tags.insert(gettags_.str(0));
  return ok_ = true;
}

versvector
get_sync_vector (sqlite3 *db)
{
  versvector vv;
  sqlstmt_t s (db, "SELECT replica, version FROM sync_vector;");
  while (s.step().row())
    vv.emplace (s.integer(0), s.integer(1));
  return vv;
}


#include "muchsync.h"

string
trashname (const string &maildir, const string &hash)
{
  if (!hash_ok(hash))
    throw std::runtime_error ("illegal hash: " + hash);
  return maildir + muchsync_trashdir + "/" +
    hash.substr(0,2) + "/" + hash.substr(2);
}
