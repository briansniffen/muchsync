
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

class hash_lookup {
  sqlstmt_t gethash_;
  sqlstmt_t getlinks_;
  sqlstmt_t makehash_;
  bool ok_ = false;
  hash_info hi_;
  i64 hash_id_;
  vector<pair<string,string>> links_;
  ifstream content_;
public:
  const string maildir;

  hash_lookup(const string &maildir, sqlite3 *db);
  bool lookup(const string &hash);
  void create(const hash_info &info);
  bool ok() const { return ok_; }
  i64 hash_id() const { assert (ok()); return hash_id_; }
  const hash_info &info() const { assert (ok()); return hi_; }
  const vector<pair<string,string>> &links() const { 
    assert (ok());
    return links_;
  }
  int nlinks() const { return links().size(); }
  string link_path(int i) const {
    auto &lnk = links().at(i);
    return maildir + "/" + lnk.first + "/" + lnk.second;
  }
  bool get_pathname(string *path) const;
  streambuf *content();
};

class hash_sync {
  sqlite3 *db_;
  notmuch_database_t *notmuch_ = nullptr;
  sqlstmt_t update_hash_stamp_;
  sqlstmt_t set_link_count_;
  sqlstmt_t delete_link_count_;
  unordered_map<string,i64> dir_ids_;
  writestamp mystamp_;

  static constexpr i64 bad_dir_id = -1;
  notmuch_database_t *notmuch();
  i64 get_dir_id (const string &dir, bool create);
public:
  hash_lookup hashdb;
  hash_sync(const string &maildir, sqlite3 *db);
  ~hash_sync();
  bool sync(const versvector &remote_sync_vector,
	    const hash_info &remote_hash_info,
	    const string *sourcefile);
  void notmuch_close() {
    if (notmuch_)
      notmuch_database_destroy (notmuch_);
    notmuch_ = nullptr;
  }
};

struct tag_info {
  string message_id;
  writestamp tag_stamp = {-1, -1};
  unordered_set<string> tags;
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

inline string
trashname (const string &maildir, const string &hash)
{
  return maildir + muchsync_trashdir + "/" + hash;
}

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
  {
    string msgid;
    input_match(is, 'L') >> hi.hash >> hi.size >> msgid;
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
    if (dir.back() == ')') {
      is.putback (')');
      dir.resize(dir.size()-1);
    }
    if (!dir.empty())
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

writestamp
get_mystamp(sqlite3 *db)
{
  sqlstmt_t s (db, "SELECT replica, version "
	       "FROM configuration JOIN sync_vector ON (value = replica) "
	       "WHERE key = 'self';");
  if (!s.step().row())
    throw runtime_error ("Cannot find myself in sync_vector");
  return { s.integer(0), s.integer(1) };
}

hash_lookup::hash_lookup (const string &m, sqlite3 *db)
  : gethash_(db, "SELECT hash_id, size, message_id, replica, version"
	     " FROM maildir_hashes WHERE hash = ?;"),
    getlinks_(db, "SELECT dir_path, name"
	      " FROM maildir_files JOIN maildir_dirs USING (dir_id)"
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
  for (getlinks_.reset().param(hash_id_).step();
       getlinks_.row(); getlinks_.step()) {
    string dir = getlinks_.str(0), name = getlinks_.str(1);
    ++hi_.dirs[dir];
    links_.emplace_back(dir, name);
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
  ok_ = true;
}

bool
hash_lookup::get_pathname(string *out) const
{
  struct stat sb;
  string path;
  for (int i = 0, e = nlinks(); i < e; i++) {
    path = link_path(i);
    if (!stat(path.c_str(), &sb) && S_ISREG(sb.st_mode)
	&& sb.st_size == hi_.size) {
      *out = move(path);
      return true;
    }
  }
  path = trashname(maildir, hi_.hash);
  if (!stat(path.c_str(), &sb) && S_ISREG(sb.st_mode)
      && sb.st_size == hi_.size
      && hi_.hash == get_sha (AT_FDCWD, path.c_str(), nullptr)) {
    *out = move(path);
    return true;
  }
  return false;
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

hash_sync::hash_sync (const string &maildir, sqlite3 *db)
  : db_(db),
    update_hash_stamp_(db_, "UPDATE maildir_hashes "
		       "SET replica = ?, version = ? WHERE hash_id = ?;"),
    set_link_count_(db_, "INSERT OR REPLACE INTO maildir_links"
		    " (hash_id, dir_id, link_count) VALUES (?, ?, ?);"),
    delete_link_count_(db_, "DELETE FROM maildir_links"
		       " WHERE hash_id = ? & dir_id = ?;"),
    mystamp_(get_mystamp(db_)),
    hashdb (maildir, db_)
{
  sqlstmt_t s (db, "SELECT dir_path, dir_id FROM maildir_dirs;");
  for (s.step(); s.row(); s.step())
    dir_ids_.emplace (s.str(0), s.integer(1));
}

hash_sync::~hash_sync ()
{
  notmuch_close();
}

notmuch_database_t *
hash_sync::notmuch ()
{
  if (!notmuch_) {
    notmuch_status_t err =
      notmuch_database_open (hashdb.maildir.c_str(),
			     NOTMUCH_DATABASE_MODE_READ_WRITE,
			     &notmuch_);
    if (err)
      throw runtime_error (hashdb.maildir + notmuch_status_to_string(err));
  }
  return notmuch_;
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
    if (!is_dir (path)) {
      if (mkdir (path.c_str(), 0777)) {
	cerr << "creating directory " << path
	     << " failed (" << strerror(errno) << ")\n";
	return false;
      }
      cerr << "created directory " << path << '\n';
    }
    if (n == string::npos)
      return true;
    path[n] = '/';
  }
}

i64
hash_sync::get_dir_id (const string &dir, bool create)
{
  auto i = dir_ids_.find(dir);
  if (i != dir_ids_.end())
    return i->second;

  if (!sanity_check_path (dir))
    throw runtime_error (dir + ": illegal directory name");
  if (!create)
    return bad_dir_id;
  string path = hashdb.maildir + "/" + dir;
  if (!is_dir(path) && !recursive_mkdir(path))
    throw runtime_error (path + ": cannot create directory");
  
  sqlexec (db_, "INSERT INTO maildir_dirs (dir_path) VALUES (%Q);",
	   dir.c_str());
  i64 dir_id = sqlite3_last_insert_rowid (db_);
  dir_ids_.emplace (dir, dir_id);
  return dir_id;
}

bool
hash_sync::sync(const versvector &rvv,
		const hash_info &rhi,
		const string *sourcep)
{
  hash_info lhi;
  if (hashdb.lookup(rhi.hash)) {
    /* We might already be up to date from a previous sync that never
     * completed (meaning some hashes got ahead of the global sync
     * vector). */
    if (hashdb.info().hash_stamp == rhi.hash_stamp)
      return true;
    lhi = hashdb.info();
  }
  else
    lhi.hash = rhi.hash;


  bool links_conflict =
    lhi.hash_stamp.second > find_default (-1, rvv, lhi.hash_stamp.first);
  bool deleting = rhi.dirs.empty() && (!links_conflict || lhi.dirs.empty());
  unordered_map<string,i64> needlinks (rhi.dirs);
  bool needsource = false;
  for (auto i : lhi.dirs)
    needlinks[i.first] -= i.second;
  for (auto i : needlinks)
    if (i.second > 0)
      needsource = true;

  /* find copy of content, if needed */
  string source;
  if (needsource) {
    if (sourcep)
      source = *sourcep;
    else if (!hashdb.ok() || !hashdb.get_pathname (&source))
      return false;
  }

  sqlexec (db_, "SAVEPOINT hash_sync;");
  cleanup c (sqlexec, db_, "ROLLBACK TO hash_sync;");
  if (!hashdb.ok()) {
    hashdb.create(rhi);
    lhi = hashdb.info();
  }

  /* Adjust link counts in database */
  for (auto li : needlinks)
    if (li.second != 0) {
      i64 dir_id = get_dir_id (li.first, li.second > 0);
      if (dir_id == bad_dir_id)
	continue;
      i64 newcount = find_default(0, lhi.dirs, li.first) + li.second;
      if (newcount > 0)
	set_link_count_.reset()
	  .param(hashdb.hash_id(), dir_id, newcount).step();
      else
	delete_link_count_.reset().param(hashdb.hash_id(), dir_id).step();
    }

  /* Set writestamp for new link counts */
  const writestamp *wsp = links_conflict ? &mystamp_ : &rhi.hash_stamp;
  update_hash_stamp_.reset()
    .param(wsp->first, wsp->second, hashdb.hash_id()).step();

  /* add missing links */
  for (auto li : needlinks)
    for (; li.second > 0; --li.second) {
      string newname = li.first + "/" + maildir_name();
      string target = hashdb.maildir + "/" + newname;
      if (link (source.c_str(), target.c_str()))
	throw runtime_error (string("link (\"") + source + "\", \""
			     + target + "\"): " + strerror(errno));
      notmuch_status_t err =
	notmuch_database_add_message (notmuch(), target.c_str(), nullptr);
      if (err && err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID)
	throw runtime_error (hashdb.maildir + ": "
			     + notmuch_status_to_string(err));
    }
  /* remove extra links */
  if (!links_conflict)
    for (int i = 0, e = hashdb.nlinks(); i < e; i++) {
      i64 &n = needlinks[hashdb.links().at(i).first];
      if (n < 0) {
	string path = hashdb.link_path(i);
	bool err;
	if (deleting)
	  err = rename (path.c_str(),
			trashname(hashdb.maildir, rhi.hash).c_str());
	else
	  err = unlink (path.c_str());
	if (!err) {
	  ++n;
	  notmuch_status_t err =
	    notmuch_database_remove_message (notmuch(), path.c_str());
	  if (err && err != NOTMUCH_STATUS_DUPLICATE_MESSAGE_ID)
	    throw runtime_error ("remove " + path + ": " +
				 + notmuch_status_to_string(err));
	}
      }
    }

  c.disable();
  sqlexec (db_, "RELEASE hash_sync;");
  return true;
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
cmd_lsync (sqlite3 *sqldb, const versvector &vv)
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
cmd_tsync (sqlite3 *sqldb, const versvector &vv)
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

  hash_lookup hashdb(maildir, db);
  tag_lookup tagdb(db);

  cout << "200 " << dbvers << '\n';
  string cmdline;
  istringstream cmdstream;
  while (getline(cin, cmdline).good()) {
    cmdstream.clear();
    cmdstream.str(cmdline);
    string cmd;
    cmdstream >> cmd;
    if (cmd.empty()) {
      cout << "500 invalid empty line\n";
    }
    else if (cmd == "quit") {
      cout << "200 goodbye\n";
      return;
    }
    else if (cmd == "vect") {
      cout << "200 " << show_sync_vector (get_sync_vector (db)) << '\n';
    }
    else if (cmd == "send") {
      string hash;
      cmdstream >> hash;
      streambuf *sb;
      if (hashdb.lookup(hash) && (sb = hashdb.content()))
	cout << "220-" << hashdb.info() << '\n'
	     << sb
	     << "220 " << hashdb.info().hash << '\n';
      else if (hashdb.ok())
	cout << "420 cannot open file\n";
      else
	cout << "520 unknown hash\n";
    }
    else if (cmd.substr(1) == "info") {
      string key;
      cmdstream >> key;
      switch (cmd[0]) {
      case 'l':			// linfo command
	if (hashdb.lookup(key))
	  cout << "210 " << hashdb.info() << '\n';
	else
	  cout << "510 unknown hash\n";
	break;
      case 't':			// tinfo command
	if (tagdb.lookup(key))
	  cout << "210 " << tagdb.info() << '\n';
	else
	  cout << "510 unkown message id\n";
	break;
      default:
	cout << "500 unknown verb " << cmd << '\n';
	break;
      }
    }
    else if (cmd.substr(1) == "sync") {
      versvector vv;
      if (!read_sync_vector(cmdstream, vv))
	cout << "500 could not parse vector\n";
      else {
	switch (cmd[0]) {
	case 'l':			// lsync command
	  cmd_lsync (db, vv);
	  break;
	case 't':			// tsync command
	  cmd_tsync (db, vv);
	  break;
	default:
	  cout << "500 unknown verb " << cmd << '\n';
	  break;
	}
      }
    }
    else
      cout << "500 unknown verb " << cmd << '\n';
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

inline uint32_t
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
  int fds[2];
  cmd_iofds (fds, cmd);
  ofdstream out (fds[1]);
  ifdstream in (spawn_infinite_input_buffer (fds[0]));
  in.tie (&out);
  close (fds[0]);

  /* Any work done here gets overlapped with server */
  sync_local_data (db, maildir);
  versvector localvv {get_sync_vector (db)}, remotevv;
  string line;
  istringstream is;
  hash_sync hsync (maildir, db);
  i64 pending = 0;

  get_response (in, line);

  out << "vect\n";
  get_response (in, line);
  is.str(line.substr(4));
  if (!read_sync_vector(is, remotevv))
    throw runtime_error ("cannot parse version vector " + line.substr(4));

  sqlexec (db, "BEGIN;");
  out << "lsync " << show_sync_vector(localvv) << '\n';
  while (get_response (in, line) && line.at(3) == '-') {
    is.str(line.substr(4));
    hash_info hi;
    if (!(is >> hi))
      throw runtime_error ("could not parse hash_info: " + line.substr(4));
    bool ok = hsync.sync (remotevv, hi, nullptr);
    if (opt_verbose >= 2) {
      if (ok)
	cerr << hi << '\n';
      else
	cerr << hi.hash << " UNKNOWN\n";
    }
    if (!ok) {
      out << "send " << hi.hash << '\n';
      pending++;
    }
  }
  out << "tsync " << show_sync_vector(localvv) << '\n';

  ofstream tmp;
  hash_info hi;
  for (; pending > 0; pending--) {
    get_response (in, line);
    is.str(line.substr(4));
    if (!(is >> hi))
      throw runtime_error ("could not parse hash_info: " + line.substr(4));
    string path = maildir + muchsync_tmpdir + "/" + maildir_name();
    tmp.open(path);
    if (!tmp.is_open())
      throw runtime_error (path + ": " + strerror(errno));
    cleanup _unlink (unlink, path.c_str());
    i64 size = hi.size;
    while (size > 0) {
      char buf[16384];
      int n = min<i64>(sizeof(buf), size);
      in.read(buf, n);
      if (!in.good())
	throw runtime_error ("premature EOF");
      tmp.write(buf, n);
      if (!out.good())
	throw runtime_error (string("error writing mail file: ")
			     + strerror(errno));
      size -= n;
    }
    tmp.close();
    getline (in, line);
    if (line.size() < 4 || line.at(0) != '2' || line.at(3) != ' '
	|| line.substr(4) != hi.hash)
      throw runtime_error ("lost sync while receiving message: " + line);
    if (!hsync.sync (remotevv, hi, &path))
      throw runtime_error ("hash_sync::sync failed even with source");
    if (opt_verbose >= 2)
      cerr << hi << '\n';
  }

  hsync.notmuch_close();
  xapian_refresh_message_ids(db, maildir);

  while (get_response (in, line) && line.at(3) == '-') {
    is.str(line.substr(4));
    tag_info ti;
    if (!(is >> ti))
      throw runtime_error ("could not parse tag_info: " + line.substr(4));
    if (opt_verbose >= 2)
      cerr << ti << '\n';
  }

  {
    sqlstmt_t udvv(db, "INSERT OR REPLACE INTO sync_vector (replica, version)"
		   " VALUES (?, ?);");
    for (auto i : remotevv)
      if (i.second > find_default (-1, localvv, i.first))
	udvv.reset().param(i.first, i.second).step();
  }

  sqlexec (db, "COMMIT;");
}








#if 0
void
crap()
{
    get_docid_(db, "SELECT docid FROM message_ids WHERE message_id = ?;"),
    update_msgid_(db, "INSERT OR REPLACE INTO"
		  " message_ids (message_id, docid, replica, version)"
		  " VALUES (?, ?, ?, ?);"),
    delete_msgid_(db, "DELETE FROM message_ids WHERE message_id = ?;"),

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
}
#endif
