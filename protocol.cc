
#include <iostream>
#include <fstream>
#include <sstream>
#include <limits>
#include <cstdio>
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include "muchsync.h"

using namespace std;

struct hash_info {
  string hash;
  string message_id;
  writestamp tag_stamp = {-1, -1};
  vector<string> tags;
  writestamp dir_stamp = {-1, -1};
  unordered_map<string,unsigned> dirs;
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
public:
  message_reader(sqlite3 *db, const string &m) :
    maildir_(m), gethash_(db, gethash_sql), gettags_(db, gettags_sql) {}
  bool lookup(const string &hash);
  bool ok() const { return ok_; }
  const string &err() const { assert (!ok()); return err_; }
  i64 size() const { assert (ok()); return size_; }
  const hash_info &info() const { assert (ok()); return hi_; }
  streambuf *rdbuf() { assert (ok()); return content_.rdbuf(); }
};


void
connect_to (const string &destination)
{
  string cmd;
  auto n = destination.find (':');
  if (n == string::npos)
    cmd = opt_ssh + " " + destination + " muchsync --server";
  else
    cmd = opt_ssh + " " + destination.substr(0, n) + " muchsync --server "
      + destination.substr(n);
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

extern "C" void
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

istream &
read_strings (istream &in, vector<string> &out)
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
    out.push_back (move (s));
  return in;
}

istream &
read_dirs (istream &in, unordered_map<string,unsigned> &out)
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

unique_ptr<hash_info>
read_hash_info (istream &in)
{
  unique_ptr<hash_info> hip {new hash_info};

  in >> hip->hash;
  string t;
  in >> t;
  hip->message_id = percent_decode (t);
  read_writestamp (in, hip->tag_stamp);
  read_strings (in, hip->tags);
  read_writestamp (in, hip->dir_stamp);
  read_dirs (in, hip->dirs);
  if (!in)
    hip.reset();
  return hip;
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
  hi_.hash = hash;
  if (!gethash_.reset().param(hash).step().row()) {
    err_ = "510 hash not found";
    return ok_ = false;
  }
  hi_.message_id = gethash_.str(2);
  hi_.dir_stamp = { gethash_.integer(3), gethash_.integer(4) };

  gettags_.reset().param(gethash_.value(2)).step();
  if (gettags_.row())
    hi_.tag_stamp = { gettags_.integer(1), gettags_.integer(2) };
  for (; gettags_.row(); gettags_.step())
    hi_.tags.push_back(gettags_.str(0));
    
  for (; gethash_.row(); gethash_.step()) {
    string dirpath (gethash_.str(0));
    if (!content_.is_open()) {
      content_.open (maildir_ + "/" + dirpath + "/" + gethash_.str(1));
      size_ = gethash_.integer(5);
    }
    ++hi_.dirs[dirpath];
  }
  if (!content_.is_open()) {
    err_ = "420 cannot open file";
    return ok_ = false;
  }
  return ok_ = true;
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
      if (mr.lookup(hash))
	cout << "220-" << mr.size() << " bytes\n" << mr.rdbuf()
	     << "220 " << show_hash_info (mr.info()) << '\n';
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

