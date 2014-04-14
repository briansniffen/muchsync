
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
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <openssl/rand.h>
#include "misc.h"
#include "muchsync.h"
#include "infinibuf.h"

using namespace std;

bool interrupted;

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

static void
interrupt(int sig)
{
  interrupted = true;
}
static void
catch_interrupts(int sig, bool active)
{
  struct sigaction act;
  memset(&act, 0, sizeof(act));
  if (active) {
    act.sa_handler = &interrupt;
    act.sa_flags = SA_RESETHAND;
  }
  else {
    if (interrupted)
      exit(1);
    act.sa_handler = SIG_DFL;
  }
  sigaction(sig, &act, nullptr);
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

static string
maildir_name()
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

static string
new_maildir_path(const string &dir, string *namep = nullptr)
{
  string name = maildir_name();
  if ((dir.size() > 4 && !strncmp(dir.data() + (dir.size() - 4), "/cur", 4))
      || (dir.size() == 3 && dir == "cur"))
    name += ":2,";
  if (namep)
    *namep = name;
  if (dir.size() && dir.back() != '/')
    return dir + "/" + name;
  else
    return dir + name;
}

static writestamp
get_mystamp(sqlite3 *db)
{
  sqlstmt_t s (db, "SELECT replica, version "
	       "FROM configuration JOIN sync_vector ON (value = replica) "
	       "WHERE key = 'self';");
  if (!s.step().row())
    throw runtime_error ("Cannot find myself in sync_vector");
  return { s.integer(0), s.integer(1) };
}

msg_sync::msg_sync (notmuch_db &nm, sqlite3 *db)
  : db_(db), nm_ (nm),
    update_hash_stamp_(db_, "UPDATE maildir_hashes "
		       "SET replica = ?, version = ? WHERE hash_id = ?;"),
    add_file_(db_, "INSERT INTO xapian_files"
	      " (dir_docid, name, docid, mtime, inode, hash_id)"
	      " VALUES (?, ?, ?, ?, ?, ?);"),
    del_file_(db, "DELETE FROM xapian_files"
	      " WHERE (dir_docid = ?) & (name = ?);"),
    set_link_count_(db_, "INSERT OR REPLACE INTO xapian_nlinks"
		    " (hash_id, dir_docid, link_count) VALUES (?, ?, ?);"),
    delete_link_count_(db_, "DELETE FROM xapian_nlinks"
		       " WHERE (hash_id = ?) & (dir_docid = ?);"),
    clear_tags_(db_, "DELETE FROM tags WHERE docid = ?;"),
    add_tag_(db_, "INSERT OR IGNORE INTO tags (docid, tag) VALUES (?, ?);"),
    update_message_id_stamp_(db_, "UPDATE message_ids SET"
			     " replica = ?, version = ? WHERE docid = ?;"),
    record_docid_(db_, "INSERT OR IGNORE INTO message_ids"
		  " (message_id, docid, replica, version)"
		  " VALUES (?, ?, 0, 0);"),
    mystamp_(get_mystamp(db_)),
    hashdb (nm_.maildir, db_),
    tagdb (db_)
{
  sqlstmt_t s (db_, "SELECT dir_path, dir_docid FROM xapian_dirs;");
  while (s.step().row()) {
    string dir {s.str(0)};
    i64 dir_id {s.integer(1)};
    dir_ids_.emplace (dir, dir_id);
  }
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

static bool
recursive_mkdir(string path)
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
      if (opt_verbose > 0)
	cerr << "created directory " << path.c_str() << '\n';
    }
    if (n == string::npos)
      return true;
    path[n] = '/';
  }
}

static bool
maildir_mkdir(string path)
{
  if (!recursive_mkdir(path))
    return false;
  size_t pos = path.rfind('/');
  if (pos == string::npos)
    pos = 0;
  else
    pos++;
  string prefix = path.substr(0, pos);
  string suffix = path.substr(pos);
  if (suffix == "new") {
    if (!mkdir((prefix + "cur").c_str(), 0777) && opt_verbose > 0)
      cerr << "created directory " << prefix << "cur\n";
    if (!mkdir((prefix + "tmp").c_str(), 0777) && opt_verbose > 0)
      cerr << "created directory " << prefix << "tmp\n";
  }
  else if (suffix == "cur") {
    if (!mkdir((prefix + "new").c_str(), 0777) && opt_verbose > 0)
      cerr << "created directory " << prefix << "new\n";
    if (!mkdir((prefix + "tmp").c_str(), 0777) && opt_verbose > 0)
      cerr << "created directory " << prefix << "tmp\n";
  }
  return true;
}

inline Xapian::docid
notmuch_directory_get_document_id (const notmuch_directory_t *dir)
{
  struct fake_directory {
    notmuch_database_t *notmuch;
    Xapian::docid doc_id;
  };
  return reinterpret_cast<const fake_directory *>(dir)->doc_id;
}

i64
msg_sync::get_dir_docid(const string &dir)
{
  auto i = dir_ids_.find(dir);
  if (i != dir_ids_.end())
    return i->second;

  i64 dir_docid = nm_.get_dir_docid(dir.c_str());
  sqlexec (db_, "INSERT OR REPLACE INTO xapian_dirs"
	   " (dir_path, dir_docid, dir_mtime) VALUES (%Q, %lld, -1);",
	   dir.c_str(), i64(dir_docid));
  dir_ids_.emplace(dir, dir_docid);
  return dir_docid;
}

static void
resolve_one_link_conflict(const unordered_map<string,i64> &a,
			  const unordered_map<string,i64> &b,
			  const string &name,
			  unordered_map<string,i64> &out)
{
  if (out.find(name) != out.end())
    return;
  size_t pos = name.rfind('/');
  if (pos == string::npos)
    pos = 0;
  else
    pos++;
  string suffix = name.substr(pos);
  if (suffix != "cur" && suffix != "new") {
    out[name] = max(find_default(0, a, name), find_default(0, b, name));
    return;
  }

  string base = name.substr(0, pos);
  string newpath = base + "new", curpath = base + "cur";
  i64 curval = max(find_default(0, a, curpath), find_default(0, b, curpath));
  i64 newval = (max(find_default(0, a, curpath) + find_default(0, a, newpath),
		    find_default(0, b, curpath) + find_default(0, b, newpath))
		- curval);
  if (curval)
    out[curpath] = curval;
  if (newval)
    out[newpath] = newval;
}

static unordered_map<string,i64>
resolve_link_conflicts(const unordered_map<string,i64> &a,
		       const unordered_map<string,i64> &b)
{
  unordered_map<string,i64> ret;
  for (auto ia : a)
    resolve_one_link_conflict(a, b, ia.first, ret);
  for (auto ib : b)
    resolve_one_link_conflict(a, b, ib.first, ret);
  return ret;
}

bool
msg_sync::hash_sync(const versvector &rvv,
		    const hash_info &rhi,
		    const string *sourcep,
		    const tag_info *tip)
{
  hash_info lhi;
  i64 docid = -1;
  bool docid_valid = false;
  bool new_msgid = false;

  if (hashdb.lookup(rhi.hash)) {
    /* We might already be up to date from a previous sync that never
     * completed, in which case there is nothing to do. */
    if (hashdb.info().hash_stamp == rhi.hash_stamp)
      return true;
    lhi = hashdb.info();
  }
  else
    lhi.hash = rhi.hash;

  bool links_conflict =
    lhi.hash_stamp.second > find_default (0, rvv, lhi.hash_stamp.first);
  bool deleting = rhi.dirs.empty() && (!links_conflict || lhi.dirs.empty());

  unordered_map<string,i64> needlinks
    (links_conflict ? resolve_link_conflicts (lhi.dirs, rhi.dirs) : rhi.dirs);
  bool needsource = false;
  for (auto i : lhi.dirs)
    needlinks[i.first] -= i.second;
  for (auto i : needlinks)
    if (i.second > 0) {
      needsource = true;
      break;
    }

  /* find copy of content, if needed */
  string source;
  bool clean_trash = false;
  struct stat sb;
  if (needsource) {
    if (sourcep)
      source = *sourcep;
    else if (!hashdb.ok() || !hashdb.get_pathname (&source, &clean_trash))
      return false;
    if (stat(source.c_str(), &sb))
      return false;
  }

  if (!hashdb.ok()) {
    hashdb.create(rhi);
    lhi = hashdb.info();
  }

  /* Set writestamp for new link counts */
  const writestamp *wsp = links_conflict ? &mystamp_ : &rhi.hash_stamp;
  update_hash_stamp_.reset()
    .param(wsp->first, wsp->second, hashdb.hash_id()).step();

  auto save_needlinks = needlinks;

  /* add missing links */
  for (auto li : needlinks)
    for (; li.second > 0; --li.second) {
      if (!sanity_check_path(li.first))
	break;
      string newname;
      string target =
	new_maildir_path(hashdb.maildir + "/" + li.first, &newname);
      if (link(source.c_str(), target.c_str())
	  && (errno != ENOENT
	      || !maildir_mkdir(hashdb.maildir + "/" + li.first)
	      || link(source.c_str(), target.c_str())))
	  throw runtime_error (string("link (\"") + source + "\", \""
			       + target + "\"): " + strerror(errno));

      cleanup end_atomic;
      if (tip) {
	nm_.begin_atomic();
	end_atomic.reset(mem_fn(&notmuch_db::end_atomic), ref(nm_));
      }
      bool isnew;
      docid =
	notmuch_db::get_docid(nm_.add_message(target,
					      tip ? &tip->tags : nullptr,
					      &isnew));
      docid_valid = true;
      i64 dir_id = get_dir_docid(li.first);
      add_file_.reset().param(dir_id, newname, docid, ts_to_double(sb.st_mtim),
			      i64(sb.st_ino), hashdb.hash_id()).step();
      if (isnew) {
	new_msgid = true;
	// XXX tip might be NULL here when undeleting a file
	update_message_id_stamp_.reset()
	  .param(tip->tag_stamp.first, tip->tag_stamp.second, docid).step();
	add_tag_.reset().bind_int(1, docid);
	for (auto t : (tip ? tip->tags : nm_.new_tags))
	  add_tag_.reset().bind_text(2, t).step();
      }
    }
  /* remove extra links */
  if (!links_conflict)
    for (int i = 0, e = hashdb.nlinks(); i < e; i++) {
      i64 &n = needlinks[hashdb.links().at(i).first];
      if (n < 0) {
	string path = hashdb.link_path(i);
	bool err;
	if (deleting) {
	  string dest = trashname(hashdb.maildir, rhi.hash);
	  err = rename (path.c_str(), dest.c_str());
	  if (err)
	    cerr << "rename " << path << ' '
		 << trashname(hashdb.maildir, rhi.hash)
		 << ": " << strerror (errno) << '\n';
	  /* You can't rename a file onto itself, so if the trash
	   * already contains a hard link to the same inode, we need
	   * to delete the original. */
	  else
	    unlink (path.c_str());
	}
	else {
	  err = unlink (path.c_str());
	  if (err)
	    cerr << "unlink " << path << ' '
		 << ": " << strerror (errno) << '\n';
	}
	if (!err) {
	  ++n;
	  auto df = hashdb.links()[i];
	  i64 dir_docid = get_dir_docid(df.first);
	  del_file_.reset().param(dir_docid, df.second).step();
	  nm_.remove_message(path);
	}
      }
    }

  if (new_msgid && docid_valid)
    record_docid_.param(rhi.message_id, docid).step().reset();

  /* Adjust link counts in database */
  for (auto li : save_needlinks)
    if (li.second != 0) {
      i64 dir_docid = get_dir_docid(li.first);
      i64 newcount = find_default(0, lhi.dirs, li.first) + li.second;
      if (newcount > 0)
	set_link_count_.reset()
	  .param(hashdb.hash_id(), dir_docid, newcount).step();
      else
	delete_link_count_.reset().param(hashdb.hash_id(), dir_docid).step();
    }

  if (clean_trash)
    unlink (trashname(hashdb.maildir, rhi.hash).c_str());
  return true;
}

bool
msg_sync::tag_sync(const versvector &rvv, const tag_info &rti)
{
  if (!tagdb.lookup(rti.message_id)) {
    cerr << "warning: can't find " << rti.message_id << '\n';
    return false;
  }
  const tag_info &lti = tagdb.info();
  if (lti.tag_stamp == rti.tag_stamp)
    return true;

  sqlexec (db_, "SAVEPOINT tag_sync;");
  cleanup c (sqlexec, db_, "ROLLBACK TO tag_sync;");

  notmuch_db::message_t msg = nm_.get_message (rti.message_id.c_str());
  assert (tagdb.docid() == nm_.get_docid(msg));

  bool tags_conflict
    = lti.tag_stamp.second > find_default (0, rvv, lti.tag_stamp.first);
  unordered_set<string> newtags (rti.tags);
  if (tags_conflict) {
    // Logically OR most tags
    for (auto i : lti.tags)
      newtags.insert(i);
    // But logically AND new_tags
    for (auto i : nm_.new_tags)
      if (rti.tags.find(i) == rti.tags.end()
	  || lti.tags.find(i) == lti.tags.end())
	newtags.erase(i);
  }

  nm_.set_tags(msg, newtags);

  const writestamp *wsp = tags_conflict ? &mystamp_ : &rti.tag_stamp;
  update_message_id_stamp_.reset()
    .param(wsp->first, wsp->second, tagdb.docid())
    .step();
  clear_tags_.reset().param(tagdb.docid()).step();
  add_tag_.reset().bind_int(1, tagdb.docid());
  for (auto t : newtags)
    add_tag_.reset().bind_text(2, t).step();

  c.release();
  sqlexec (db_, "RELEASE tag_sync;");
  return true;
}

static string
receive_message (istream &in, const hash_info &hi, const string &maildir)
{
  string path (maildir + muchsync_tmpdir + "/" + maildir_name());
  ofstream tmp (path, ios_base::out|ios_base::trunc);
  if (!tmp.is_open())
    throw runtime_error (path + ": " + strerror(errno));
  cleanup _unlink (unlink, path.c_str());
  i64 size = hi.size;
  hash_ctx ctx;
  while (size > 0) {
    char buf[16384];
    int n = min<i64>(sizeof(buf), size);
    in.read(buf, n);
    if (!in.good())
      throw runtime_error ("premature EOF receiving message");
    ctx.update(buf, n);
    tmp.write(buf, n);
    if (!tmp.good())
      throw runtime_error (string("error writing mail file: ")
			   + strerror(errno));
    size -= n;
  }
  tmp.close();
  if (ctx.final() != hi.hash)
    throw runtime_error ("message received does not match hash");
  _unlink.release();
  return path;
}

static void
set_peer_vector (sqlite3 *sqldb, const versvector &vv)
{
  sqlexec (sqldb, R"(
CREATE TEMP TABLE IF NOT EXISTS peer_vector (
  replica INTEGER PRIMARY KEY,
  known_version INTEGER);
DELETE FROM peer_vector;
INSERT OR REPLACE INTO peer_vector
  SELECT DISTINCT replica, 0 FROM message_ids;
INSERT OR REPLACE INTO peer_vector
  SELECT DISTINCT replica, 0 FROM maildir_hashes;
)");
  sqlstmt_t pvadd (sqldb, "INSERT OR REPLACE INTO"
		   " peer_vector (replica, known_version) VALUES (?, ?);");
  for (writestamp ws : vv)
    pvadd.param(ws.first, ws.second).step().reset();
}

static void
record_peer_vector(sqlite3 *sqldb)
{
  sqlexec(sqldb, R"(
INSERT OR REPLACE INTO sync_vector (replica, version)
SELECT replica, p.known_version
  FROM peer_vector p LEFT OUTER JOIN sync_vector s USING (replica)
  WHERE ifnull (s.version, 0) < p.known_version)");
}

static i64
send_links (sqlite3 *sqldb, const string &prefix, ostream &out)
{
  unordered_map<i64,string> dirs;
  {
    sqlstmt_t d (sqldb, "SELECT dir_docid, dir_path FROM xapian_dirs;");
    while (d.step().row())
      dirs.emplace (d.integer(0), d.str(1));
  }

  sqlstmt_t changed (sqldb, R"(
SELECT h.hash_id, hash, size, message_id, h.replica, h.version,
       dir_docid, link_count
FROM (peer_vector p JOIN maildir_hashes h
      ON ((p.replica = h.replica) & (p.known_version < h.version)))
LEFT OUTER JOIN xapian_nlinks USING (hash_id);)");
  
  i64 count = 0;
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
    out << prefix << hi << '\n';
    if (opt_verbose > 3)
      cerr << prefix << hi << '\n';
    count++;
  }
  return count;
}

static i64
send_tags (sqlite3 *sqldb, const string &prefix, ostream &out)
{
  sqlstmt_t changed (sqldb, R"(
SELECT m.docid, m.message_id, m.replica, m.version, tags.tag
FROM (peer_vector p JOIN message_ids m
      ON ((p.replica = m.replica) & (p.known_version < m.version)))
      LEFT OUTER JOIN tags USING (docid);)");

  tag_info ti;
  changed.step();
  i64 count = 0;
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
    out << prefix << ti << '\n';
    if (opt_verbose > 3)
      cerr << prefix << ti << '\n';
    count++;
  }
  return count;
}

static bool
send_content(hash_lookup &hashdb, tag_lookup &tagdb, const string &hash,
	     const string &prefix, ostream &out)
{
  streambuf *sb;
  if (hashdb.lookup(hash) && (sb = hashdb.content())
      && tagdb.lookup(hashdb.info().message_id)) {
    out << prefix << hashdb.info()
	<< ' ' << tagdb.info() << '\n' << sb;
    return true;
  }
  return false;
}

void
muchsync_server(sqlite3 *db, notmuch_db &nm)
{
  msg_sync msync(nm, db);
  hash_lookup &hashdb = msync.hashdb;
  tag_lookup tagdb(db);
  bool remotevv_valid = false;
  versvector remotevv;
  bool transaction = false;
  auto xbegin = [&transaction,db]() {
    if (!transaction) {
      sqlexec(db, "BEGIN IMMEDIATE;");
      transaction = true;
    }
  };

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
    else if (cmd == "conffile") {
      ifstream is (opt_notmuch_config);
      ostringstream os;
      if (is.is_open() && (os << is.rdbuf())) {
	string conf (os.str());
	cout << "221-" << conf.length() << '\n'
	     << conf << "221 ok\n";
      }
      else
	cout << "410 cannot find configuration\n";
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
    else if (cmd == "send") {
      string hash;
      cmdstream >> hash;
      if (send_content(hashdb, tagdb, hash, "220-", cout))
	cout << "220 " << hash << '\n';
      else if (hashdb.ok())
	cout << "420 cannot open file\n";
      else
	cout << "520 unknown hash\n";
    }
    else if (cmd == "vect") {
      if (!read_sync_vector(cmdstream, remotevv)) {
	cout << "500 could not parse vector\n";
	remotevv_valid = false;
      }
      else {
	set_peer_vector(db, remotevv);
	remotevv_valid = true;
	cout << "200 " << show_sync_vector (get_sync_vector (db)) << '\n';
      }
    }
    else if (cmd == "link") {
      xbegin();
      hash_info hi;
      if (!remotevv_valid)
	cout << "500 must follow vect command\n";
      else if (!(cmdstream >> hi))
	cout << "500 could not parse hash_info\n";
      else if (msync.hash_sync(remotevv, hi, nullptr, nullptr)) {
	if (opt_verbose > 3)
	  cerr << "received-links " << hi << '\n';
	cout << "220 " << hi.hash << " ok\n";
      }
      else
	cout << "520 " << hi.hash << " missing content\n";
    }
    else if (cmd == "recv") {
      xbegin();
      hash_info hi;
      tag_info ti;
      if (!remotevv_valid)
	cout << "500 must follow vect command\n";
      else if (!(cmdstream >> hi >> ti))
	cout << "500 could not parse hash_info or tag_info\n";
      else {
	string path;
	try {
	  path = receive_message(cin, hi, nm.maildir);
	  if (!msync.hash_sync(remotevv, hi, &path, &ti))
	    cout << "550 failed to synchronize message\n";
	  else {
	    if (opt_verbose > 3)
	      cerr << "received-file " << hi << '\n';
	    cout << "250 ok\n";
	  }
	}
	catch (exception e) {
	  cerr << e.what() << '\n';
	  cout << "550 " << e.what() << '\n';
	}
	unlink(path.c_str());
      }
    }
    else if (cmd == "tags") {
      xbegin();
      tag_info ti;
      if (!remotevv_valid)
	cout << "500 must follow vect command\n";
      else if (!(cmdstream >> ti))
	cout << "500 could not parse hash_info\n";
      else if (msync.tag_sync(remotevv, ti)) {
	if (opt_verbose > 3)
	  cerr << "received-tags " << ti << '\n';
	cout << "220 ok\n";
      }
      else
	cout << "520 unknown message-id\n";
    }
    else if (cmd.substr(1) == "sync") {
      if (!remotevv_valid)
	cout << "500 must follow vect command\n";
      else
	switch (cmd[0]) {
	case 'l':			// lsync command
	  send_links (db, "210-", cout);
	  cout << "210 ok\n";
	  break;
	case 't':			// tsync command
	  send_tags (db, "210-", cout);
	  cout << "210 ok\n";
	  break;
	default:
	  cout << "500 unknown verb " << cmd << '\n';
	  break;
	}
    }
    else if (cmd == "commit") {
      if (!remotevv_valid)
	cout << "500 must follow vect command\n";
      record_peer_vector(db);
      if (transaction) {
	transaction = false;
	sqlexec(db, "COMMIT;");
      }
      cout << "200 ok\n";
      remotevv_valid = false;
    }
    else
      cout << "500 unknown verb " << cmd << '\n';
  }
}

istream &
get_response (istream &in, string &line)
{
  if (!getline (in, line))
    throw runtime_error ("premature EOF");
  if (opt_verbose > 3)
    cerr << line << '\n';
  if (line.empty())
    throw runtime_error ("unexpected empty line");
  if (line.size() < 4)
    throw runtime_error ("unexpected short line");
  if (line.front() != '2')
    throw runtime_error ("bad response: " + line);
  return in;
}

void
muchsync_client (sqlite3 *db, notmuch_db &nm,
		 istream &in, ostream &out)
{
  constexpr time_t commit_interval = 10;
  /* Any work done here gets overlapped with server */
  sync_local_data (db, nm.maildir);
  versvector localvv {get_sync_vector (db)}, remotevv;
  string line;
  istringstream is;
  msg_sync msync (nm, db);
  i64 pending = 0;

  int down_links = 0, down_body = 0, down_tags = 0,
    up_links = 0, up_body = 0, up_tags = 0;

  out << "vect " << show_sync_vector(localvv) << "\nlsync\n" << flush;
  sqlexec(db, "BEGIN IMMEDIATE;");
  get_response (in, line);
  get_response (in, line);
  is.str(line.substr(4));
  if (!read_sync_vector(is, remotevv))
    throw runtime_error ("cannot parse version vector " + line.substr(4));
  set_peer_vector(db, remotevv);
  print_time ("received server's version vector");

  catch_interrupts(SIGINT, true);
  catch_interrupts(SIGTERM, true);
  time_t last_commit = time(nullptr);
  auto maybe_commit = [&last_commit,&nm,db] () {
    time_t now = time(nullptr);
    if (interrupted) {
      cerr << "Interrupted\n";
      nm.close();
      sqlexec(db, "COMMIT;");
      exit(1);
    }
    else if (now - last_commit >= commit_interval) {
      nm.close();
      sqlexec(db, "COMMIT; BEGIN;");
      last_commit = now;
    }
  };

  while (get_response (in, line) && line.at(3) == '-') {
    is.str(line.substr(4));
    hash_info hi;
    if (!(is >> hi))
      throw runtime_error ("could not parse hash_info: " + line.substr(4));
    bool ok = msync.hash_sync (remotevv, hi, nullptr, nullptr);
    if (opt_verbose > 2) {
      if (ok)
	cerr << hi << '\n';
      else
	cerr << hi.hash << " UNKNOWN\n";
    }
    if (!ok) {
      out << "send " << hi.hash << '\n';
      pending++;
    }
    else
      down_links++;
    maybe_commit();
  }
  out << "tsync\n";
  print_time ("received hashes of new files");
  down_body = pending;

  hash_info hi;
  tag_info ti;
  for (; pending > 0; pending--) {
    get_response (in, line);
    is.str(line.substr(4));
    if (!(is >> hi >> ti))
      throw runtime_error ("could not parse hash_info: " + line.substr(4));
    string path = receive_message(in, hi, nm.maildir);
    cleanup _unlink (unlink, path.c_str());
    getline (in, line);
    if (line.size() < 4 || line.at(0) != '2' || line.at(3) != ' '
	|| line.substr(4) != hi.hash)
      throw runtime_error ("lost sync while receiving message: " + line);
    if (!msync.hash_sync (remotevv, hi, &path, &ti))
      throw runtime_error ("msg_sync::sync failed even with source");
    if (opt_verbose > 2)
      cerr << hi << '\n';
    maybe_commit();
  }
  print_time ("received content of missing messages");

  while (get_response (in, line) && line.at(3) == '-') {
    down_tags++;
    is.str(line.substr(4));
    if (!(is >> ti))
      throw runtime_error ("could not parse tag_info: " + line.substr(4));
    if (opt_verbose > 2)
      cerr << ti << '\n';
    msync.tag_sync(remotevv, ti);
    maybe_commit();
  }
  print_time ("received tags of new and modified messages");

  record_peer_vector(db);

  nm.close();
  sqlexec (db, "COMMIT;");
  print_time("commited changes to local database");

  if (opt_verbose || opt_noup || opt_upbg)
    cerr << "received " << down_body << " messages, "
	 << down_links << " link changes, "
	 << down_tags << " tag changes\n";
  catch_interrupts(SIGINT, false);
  catch_interrupts(SIGTERM, false);

  if (opt_noup)
    return;
  if (opt_upbg)
    close(opt_upbg_fd);

  pending = 0;
  i64 i = send_links(db, "link ", out);
  print_time("sent moved messages to server");
  while (i-- > 0) {
    getline(in, line);
    if (line.size() < 4 || (line.at(0) != '2' && line.at(0) != '5'))
      throw runtime_error ("lost sync while receiving message: " + line);
    if (line.at(0) == '5') {
      is.str(line.substr(4));
      string hash;
      is >> hash;
      if (send_content(msync.hashdb, msync.tagdb, hash, "recv ", out)) {
	pending++;
	up_body++;
      }
    }
    else
      up_links++;
  }
  print_time("sent content of new messages to server");
  up_tags = send_tags(db, "tags ", out);
  pending += up_tags;
  print_time("sent modified tags to server");
  out << "commit\n";

  if (opt_verbose)
    cerr << "sent " << up_body << " messages, "
	 << up_links << " link changes, "
	 << up_tags << " tag changes\n";

  while (pending-- > 0)
    get_response(in, line);
  get_response(in, line);
  print_time("commit succeeded on server");

  if (!opt_upbg || opt_verbose) {
    int w = 5;
    cerr << "SUMMARY:\n"
	 << "  received " << setw(w) << down_body << " messages, "
	 << setw(w) << down_links << " link changes, "
	 << setw(w) << down_tags << " tag changes\n";
    cerr << "      sent " << setw(w) << up_body << " messages, "
	 << setw(w) << up_links << " link changes, "
	 << setw(w) << up_tags << " tag changes\n";
  }
}
