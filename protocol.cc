
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
#include "muchsync.h"
#include "infinibuf.h"
#include "notmuch_db.h"

using namespace std;

struct hash_info {
  string hash;
  i64 size = -1;
  string message_id;
  writestamp hash_stamp = {0, 0};
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
  i64 docid_;
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
  i64 docid() const { assert (nlinks()); return docid_; }
  int nlinks() const { return links().size(); }
  string link_path(int i) const {
    auto &lnk = links().at(i);
    return maildir + "/" + lnk.first + "/" + lnk.second;
  }
  bool get_pathname(string *path, bool *from_trash = nullptr) const;
  streambuf *content();
};

struct tag_info {
  string message_id;
  writestamp tag_stamp = {0, 0};
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
  unordered_map<string,i64> dir_ids_;
  writestamp mystamp_;

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
    throw runtime_error ("illegal hash: " + hash);
  return maildir + muchsync_trashdir + "/" +
    hash.substr(0,2) + "/" + hash.substr(2);
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
      string newname (maildir_name());
      string target = hashdb.maildir + "/" + li.first + "/" + newname;
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
	notmuch_db::get_docid(nm_.add_message(target, &tip->tags, &isnew));
      docid_valid = true;
      i64 dir_id = get_dir_docid(li.first);
      add_file_.reset().param(dir_id, newname, docid, ts_to_double(sb.st_mtim),
			      i64(sb.st_ino), hashdb.hash_id()).step();
      if (isnew) {
	new_msgid = true;
	update_message_id_stamp_.reset()
	  .param(tip->tag_stamp.first, tip->tag_stamp.second, docid).step();
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
    else if (cmd == "conf") {
      string conf = cmd_output("notmuch config list");
      if (conf.length())
        cout << "221-" << conf.length() << '\n'
             << conf << "221 ok\n";
      else
        cout << "410 cannot find configuration\n";
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
  /* Any work done here gets overlapped with server */
  sync_local_data (db, nm.maildir);
  versvector localvv {get_sync_vector (db)}, remotevv;
  string line;
  istringstream is;
  msg_sync msync (nm, db);
  i64 pending = 0;

  out << "vect " << show_sync_vector(localvv) << "\nlsync\n" << flush;
  sqlexec(db, "BEGIN IMMEDIATE;");
  get_response (in, line);
  get_response (in, line);
  is.str(line.substr(4));
  if (!read_sync_vector(is, remotevv))
    throw runtime_error ("cannot parse version vector " + line.substr(4));
  set_peer_vector(db, remotevv);
  print_time ("received server's version vector");

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
  }
  out << "tsync\n";
  print_time ("received hashes of new files");

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
  }
  print_time ("received content of missing messages");

  while (get_response (in, line) && line.at(3) == '-') {
    is.str(line.substr(4));
    if (!(is >> ti))
      throw runtime_error ("could not parse tag_info: " + line.substr(4));
    if (opt_verbose > 2)
      cerr << ti << '\n';
    msync.tag_sync(remotevv, ti);
  }
  print_time ("received tags of new and modified messages");

  record_peer_vector(db);

  sqlexec (db, "COMMIT;");
  print_time("commited changes to local database");

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
      if (send_content(msync.hashdb, msync.tagdb, hash, "recv ", out))
	pending++;
    }
  }
  print_time("sent content of new messages to server");
  pending += send_tags(db, "tags ", out);
  print_time("sent modified tags to server");
  out << "commit\n";
  while (pending-- > 0)
    get_response(in, line);
  get_response(in, line);
  print_time("commit succeeded on server");
}
