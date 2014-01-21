
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
#include <openssl/rand.h>
#include <notmuch.h>
#include "muchsync.h"
#include "fdstream.h"

using namespace std;

static unordered_set<string> new_tags = notmuch_new_tags();

struct hash_info {
  string hash;
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
  i64 size_;
  hash_info hi_;
  bool ok_ = false;
  string err_;
  unordered_map<string,vector<string>> pathnames_;
  string openpath_;
public:
  message_reader(sqlite3 *db, const string &m) :
    maildir_(m), gethash_(db, gethash_sql), gettags_(db, gettags_sql) {}
  bool lookup(const string &hash);
  bool ok() const { return ok_; }
  const string &err() const { assert (!ok()); return err_; }
  i64 size() const { assert (ok()); return size_; }
  const hash_info &info() const { assert (ok()); return hi_; }
  bool present() const { return !pathnames_.empty(); }
  const unordered_map<string,vector<string>> &paths() const {
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
show_hash_info (const hash_info &hi)
{
  ostringstream os;
  os << hi.hash << ' ' << hi.message_id << " R"
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
read_hash_info (istream &in, hash_info &outhi)
{
  hash_info hi;
  in >> hi.hash;
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
  hi_ = hash_info();
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
  size_ = gethash_.integer(5);

  gettags_.reset().param(gethash_.value(2)).step();
  if (gettags_.row())
    hi_.tag_stamp = { gettags_.integer(1), gettags_.integer(2) };
  for (; gettags_.row(); gettags_.step())
    hi_.tags.insert(gettags_.str(0));
    
  for (; gethash_.row(); gethash_.step()) {
    string dirpath (gethash_.str(0));
    pathnames_[dirpath]
      .push_back (maildir_ + "/" + dirpath + "/" + gethash_.str(1));
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
  for (auto d : pathnames_)
    for (auto p : d.second) {
      content_.open (p, ios_base::in|ios_base::ate);
      if (content_.is_open()) {
	openpath_ = p;
	size_ = content_.tellg();
	content_.seekg(0);
	return content_.rdbuf();
      }
    }
  return nullptr;
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
       cattags, catdirs
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
	cout << "220-" << mr.size() << " bytes\n" << sb
	     << "220 " << show_hash_info (mr.info()) << '\n';
      else if (mr.ok())
	cout << "420 cannot open file\n";
      else
	cout << mr.err() << '\n';
    }
    else if (cmd == "info") {
      string hash;
      cin >> hash;
      if (mr.lookup(hash))
	cout << "210-" << mr.size() << " bytes\n"
	     << "210 " << show_hash_info (mr.info()) << '\n';
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

inline string
hash_cache_path (const string &maildir, const string &hash)
{
  return (maildir + muchsync_hashdir + "/" + hash.substr(0, 2)
	  + "/" + hash.substr(2));
}

inline i64
lookup_version (const versvector &vv, i64 replica)
{
  auto i = vv.find(replica);
  return i == vv.end() ? -1 : i->second;
}

static bool
sync_message (const versvector &rvv, message_reader &mr,
	      const hash_info &rhi, notmuch_database_t *notmuch)
{
  static const hash_info empty_hash_info;
  const hash_info &lhi = mr.lookup(rhi.hash) ? mr.info() : empty_hash_info;
    
  bool links_conflict
    = (lhi.dir_stamp.second > lookup_version (rvv, lhi.dir_stamp.first)
       && rhi.dirs != lhi.dirs);
  unordered_map<string,i64> newlinks (rhi.dirs);
  if (links_conflict) {
    // should be more clever if file moves from .../new to .../cur
    for (auto i : lhi.dirs)
      newlinks[i.first] = max (i.second, newlinks[i.first]);
  }
  else {
    // Make sure every old directory is there, even if the link count is 0
    for (auto i : lhi.dirs)
      newlinks[i.first];
  }

  for (auto i : newlinks) {
    auto j = lhi.dirs.find(i.first);
    for (i64 d = i.second - (j == lhi.dirs.end() ? 0 : j->second);
	 d > 0; d--) {
      string source;
      if (mr.ok() && mr.rdbuf())
	source = mr.openpath();
      else {
	source = mr.maildir() + "/" + hash_cache_path (mr.maildir(), rhi.hash);
	if (faccessat (AT_FDCWD, source.c_str(), 0, 0))
	  return false;
	string hash;
	try { hash = get_sha (AT_FDCWD, source.c_str()); }
	catch (...) { return false; }
	if (hash != rhi.hash)
	  return false;
      }
      string target = mr.maildir() + i.first + "/" + maildir_name();
      if (link (source.c_str(), target.c_str()))
	throw runtime_error (string("link (\"") + source + "\", \""
			     + target + "\"): " + strerror(errno));
    }
  }
  for (auto i : newlinks) {
    auto j = lhi.dirs.find(i.first);
    for (i64 d = i.second - (j == lhi.dirs.end() ? 0 : j->second);
	 d < 0; d++) {
      /* delete */
    }
  }

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

  return true;
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
  message_reader mr (db, maildir);
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
    hash_info hi;
    if (!read_hash_info(is, hi))
      throw runtime_error ("could not parse hash_info: " + line.substr(4));
    if (!sync_message (remotevv, mr, hi, notmuch)) {
      out << "send " << hi.hash << '\n';
      pending++;
    }
    //cerr << show_hash_info (hi) << '\n';
  }

}
