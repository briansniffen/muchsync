#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <openssl/sha.h>
#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

const char directories_def[] =
  R"(CREATE TABLE IF NOT EXISTS directories (
  docid INTEGER PRIMARY KEY,
  path TEXT UNIQUE,
  mctime REAL);)";

static double
time_stamp ()
{
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  return ts_to_double (ts);
}

static double start_time_stamp {time_stamp()};
static double last_time_stamp {start_time_stamp};

void
print_time (string msg)
{
  double now = time_stamp();
  cout << msg << "... " << now - start_time_stamp
       << " (+" << now - last_time_stamp << ")\n";
  last_time_stamp = now;
}

string
tag_from_term (const string &term)
{
  assert (!strncmp (term.c_str(), notmuch_tag_prefix.c_str(),
		    notmuch_tag_prefix.length()));

  ostringstream tagbuf;
  tagbuf.fill('0');
  tagbuf.setf(ios::hex, ios::basefield);

  char c;
  for (const char *p = term.c_str() + 1; (c = *p); p++)
    if (isalnum (c) || (c >= '+' && c <= '.')
	|| c == '_' || c == '@' || c == '=')
      tagbuf << c;
    else
      tagbuf << '%' << setw(2) << int (uint8_t (c));

  return tagbuf.str ();
}

inline int
hexdigit (char c)
{
  if (c >= '0' && c <= '9')
    return c - '0';
  else if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  else
    throw runtime_error ("illegal hexdigit " + string (1, c));
}

string
term_from_tag (const string &tag)
{
  ostringstream tagbuf;
  tagbuf << notmuch_tag_prefix;
  int escape_pos = 0, escape_val;
  char c;
  for (const char *p = tag.c_str() + 1; (c = *p); p++) {
    switch (escape_pos) {
    case 0:
      if (c == '%')
	escape_pos = 1;
      else
	tagbuf << c;
      break;
    case 1:
      escape_val = hexdigit(c) << 4;
      escape_pos = 2;
      break;
    case 2:
      escape_pos = 0;
      tagbuf << char (escape_val | hexdigit(c));
      break;
    }
  }
  if (escape_pos)
    throw runtime_error ("term_from_tag: incomplete escape");
  return tagbuf.str();
}

string
message_tags (notmuch_message_t *message)
{
  ostringstream tagbuf;
  tagbuf.fill('0');
  tagbuf.setf(ios::hex, ios::basefield);

  bool first = true;
  for (notmuch_tags_t *ti = notmuch_message_get_tags (message);
       notmuch_tags_valid (ti); notmuch_tags_move_to_next (ti)) {
    if (first)
      first = false;
    else
      tagbuf << ' ';
    for (const char *p = notmuch_tags_get (ti); *p; p++) {
      char c = *p;
      if (isalnum (c) || (c >= '+' && c <= '.')
	  || c == '_' || c == '@' || c == '=')
	tagbuf << c;
      else
	tagbuf << '%' << setw(2) << int (uint8_t (c));
    }
  }

  return tagbuf.str ();
}

#if 0
void
scan_notmuch (sqlite3 *sqldb, const string &path)
{
  notmuch_database_t *notmuch;
  notmuch_status_t err;

  save_old_table (sqldb, "messages", messages_def);
  sqlstmt_t s (sqldb, "INSERT INTO messages(message_id, tags) VALUES (?, ?);");

  err = notmuch_database_open (path.c_str(), NOTMUCH_DATABASE_MODE_READ_ONLY,
			       &notmuch);
  if (err)
    throw runtime_error (path + ": " + notmuch_status_to_string (err));
  cleanup _c (notmuch_database_destroy, notmuch);

  int pathprefixlen = path.length() + 1;
  notmuch_query_t *query = notmuch_query_create (notmuch, "");
  notmuch_query_set_omit_excluded (query, NOTMUCH_EXCLUDE_FALSE);
  notmuch_query_set_sort (query, NOTMUCH_SORT_UNSORTED);
  notmuch_messages_t *messages = notmuch_query_search_messages (query);
  while (notmuch_messages_valid (messages)) {
    notmuch_message_t *message = notmuch_messages_get (messages);
    const char *message_id = notmuch_message_get_message_id (message);
    string tags = message_tags (message);

    s.reset ();
    s.bind_text (1, message_id);
    s.bind_text (2, tags);
    s.step ();

    //printf ("%s (%s)\n", message_id, message_tags(message).c_str());

    /*
    fmtexec (db, "INSERT INTO messages(message_id, tags) VALUES (%Q,%Q);",
	     message_id, message_tags(message).c_str());

    notmuch_filenames_t *pathiter = notmuch_message_get_filenames (message);
    while (notmuch_filenames_valid (pathiter)) {
      const char *path = notmuch_filenames_get (pathiter);

      printf ("        %s\n", path + pathprefixlen);

      notmuch_filenames_move_to_next (pathiter);
    }
    */

    notmuch_message_destroy (message);
    notmuch_messages_move_to_next (messages);
  }
}
#endif

static void
scan_message_ids (sqlite3 *sqldb, Xapian::Database &xdb)
{
  fmtexec (sqldb, "DROP TABLE IF EXISTS current_messages; "
	   "CREATE TEMP TABLE current_messages"
	   " (docid INTEGER PRIMARY KEY, message_id TEXT NOT NULL,"
	   " tags TEXT DEFAULT '');");
  sqlstmt_t s (sqldb, "INSERT INTO current_messages (docid, message_id)"
	       " VALUES (?, ?);");

  for (Xapian::ValueIterator
	 vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
	 ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);
       vi != ve; vi++)
    s.reset().param(i64(vi.get_docid()), *vi).step();
}

static void
scan_tags (sqlite3 *sqldb, Xapian::Database &xdb)
{
    sqlstmt_t s (sqldb, "UPDATE current_messages SET tags = tags"
		 " || (CASE tags WHEN '' THEN '' ELSE ' ' END) || ?"
		 " WHERE docid = ?;");

  for (Xapian::TermIterator ti = xdb.allterms_begin(notmuch_tag_prefix),
	 te = xdb.allterms_end(notmuch_tag_prefix); ti != te; ti++) {
    string tag = tag_from_term (*ti);
    cout << "  " << tag << "\n";
    s.bind_text (1, tag);
    for (Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
	   pe = xdb.postlist_end (*ti); pi != pe; pi++) {
      s.bind_int (2, *pi);
      s.step ();
      s.reset ();
    }
  }
}

void
scan_directories (sqlite3 *sqldb, writestamp ws, Xapian::Database &xdb,
			 int rootfd)
{
  save_old_table (sqldb, "directories", directories_def);
  sqlstmt_t insert (sqldb, "INSERT INTO directories"
		    " (docid, path, mctime) VALUES (?,?,?);");

  for (Xapian::TermIterator
         ti = xdb.allterms_begin(notmuch_directory_prefix),
         te = xdb.allterms_end(notmuch_directory_prefix);
       ti != te; ti++) {
    string dir = (*ti).substr(notmuch_directory_prefix.length());
    if (dir.length() >= 4) {
      string end (dir.substr (dir.length() - 4));
      if (end != "/cur" && end != "/new")
        continue;
    }
    else if (dir != "cur" && dir != "new")
      continue;
    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);
    if (pi == pe)
      cerr << "warning: directory term " << *ti << " has no postings";
    else {
      insert.reset();
      insert.bind_int (1, *pi);
      insert.bind_text (2, dir);
      struct stat sb;
      if (fstatat (rootfd, dir.c_str(), &sb, 0)) {
	cerr << dir << ": " << strerror (errno) << '\n';
	insert.bind_null (3);
      }
      else {
	insert.bind_real (3, max (ts_to_double (sb.st_mtim),
				  ts_to_double (sb.st_ctim)));
      }
      insert.step();
      if (++pi != pe)
        cerr << "warning: directory term " << *ti << " has multiple postings";
    }
  }

  fmtexec (sqldb, "UPDATE files "
	   "SET docid = NULL, dir_docid = NULL, replica = %lld, version = %lld"
	   " WHERE dir_docid IN (SELECT docid FROM"
	   " (SELECT docid, path from old_directories"
	   " EXCEPT SELECT docid, path from directories))",
	   ws.first, ws.second);
}

unordered_map<Xapian::docid, string>
get_xapian_directories (sqlite3 *sqldb)
{
  unordered_map<Xapian::docid, string> ret;
  sqlstmt_t s (sqldb, "SELECT docid, path FROM directories;");
  while (s.step().row())
    ret.emplace (s.integer(0), s.str(1));
  return ret;
}

static void
scan_by_directory (sqlite3 *sqldb, writestamp ws, Xapian::Database &xdb)
{
  sqlstmt_t dirscan (sqldb, "SELECT docid, path"
		     " FROM directories n LEFT OUTER JOIN old_directories o"
		     " USING (docid, path) "
		     " WHERE (n.mctime IS NULL) | (n.mctime IS NOT o.mctime);");

  sqlstmt_t lookup (sqldb, "SELECT docid, dir_docid FROM files WHERE path = ?");
  sqlstmt_t insert (sqldb, "INSERT OR REPLACE INTO files"
		    " (docid, dir_docid, path, replica, version)"
		    " VALUES (?, ?, ?, %lld, %lld);", ws.first, ws.second);

  while (dirscan.step().row()) {
    Xapian::docid dir_docid = dirscan.integer(0);
    string dir = dirscan.str(1);
    string dirtermprefix = (notmuch_file_direntry_prefix
			    + to_string (dir_docid) + ":");
    for (Xapian::TermIterator
	   ti = xdb.allterms_begin(dirtermprefix),
	   te = xdb.allterms_end(dirtermprefix);
	 ti != te; ti++) {
      string dirent { (*ti).substr (dirtermprefix.length()) };
      Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
	pe = xdb.postlist_end (*ti);
      if (pi == pe)
	cerr << "warning: file direntry term " << *ti << " has no postings";
      else {
	string path = dir + "/" + dirent;
	if (lookup.reset().bind_text(1, path).step().done()
	    || lookup.integer(0) != *pi
	    || lookup.integer(1) != dir_docid)
	  insert.reset().param(i64(*pi), dir_docid, path).step();
	if (++pi != pe)
	  cerr << "warning: file direntry term " << *ti
	       << " has multiple postings";
      }
    }
  }
}

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setw (2) << setfill ('0');
  for (auto c : s)
    os << (int (c) & 0xff);
  return os.str ();
}

static string
get_sha (int dfd, const char *direntry)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);

  SHA_CTX ctx;
  SHA1_Init (&ctx);

  char buf[16384];
  int n;
  while ((n = read (fd, buf, sizeof (buf))) > 0)
    SHA1_Update (&ctx, buf, n);
  if (n < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  unsigned char resbuf[SHA_DIGEST_LENGTH];
  SHA1_Final (resbuf, &ctx);
  return hexdump ({ reinterpret_cast<const char *> (resbuf), sizeof (resbuf) });
}

static void
find_changed_files (sqlite3 *sqldb, writestamp ws, int fd)
{
  sqlstmt_t scan
    (sqldb,
     "SELECT f.rowid, path, mtime, size, hash, inode, docid, m.message_id"
     " FROM files f INNER JOIN current_messages m USING (docid)"
     " WHERE dir_docid IN (SELECT docid FROM"
     " (SELECT * FROM directories EXCEPT"
     " SELECT * FROM old_directories))");
  sqlstmt_t create_hash
    (sqldb, "INSERT OR IGNORE INTO hashes"
     " (hash, message_id, docid, replica, version,"
     " create_replica, create_version)"
     " VALUES (?, ?, ?, %lld, %lld, %lld, %lld);",
     ws.first, ws.second, ws.first, ws.second);
  sqlstmt_t update_hash
    (sqldb, "UPDATE hashes SET message_id = ?, docid = ?,"
     " replica = %lld, version = %lld WHERE hash = ?;", ws.first, ws.second);
  sqlstmt_t delfile (sqldb, "DELETE FROM files WHERE rowid = ?;");
  sqlstmt_t updfile (sqldb, "UPDATE files SET mtime = ?, size = ?, hash = ?,"
		     " replica = %lld, version = %lld, inode = ?"
		     " WHERE rowid = ?;", ws.first, ws.second);

  while (scan.step().row()) {
    struct stat sb;
    if (fstatat (fd, scan.c_str(1), &sb, 0) == 0) {
      double mtim = ts_to_double(sb.st_mtim);
      if (scan.null(4) || mtim != scan.real(2)
	  || sb.st_size != scan.integer(3)
	  || sb.st_ino != scan.integer(6)) {
	string hash;
	try { hash = get_sha (fd, scan.c_str(1)); }
	catch (runtime_error &e) { cerr << e.what(); continue; }
	cout << scan.str(1) << ": " << hash << '\n';
	create_hash.reset().param(hash, scan.value(7), scan.value(6)).step();
	if (sqlite3_changes (sqldb) <= 0)
	  update_hash.reset().param(scan.value(7), scan.value(6), hash).step();
	updfile.reset().param(mtim, i64(sb.st_size), hash,
			      i64(sb.st_ino), scan.integer(0)).step();
      }
    }
    else if (errno == ENOENT) {
      // cout << "deleting " << scan.str(1) << "\n";
      delfile.reset().param(scan.integer(0)).step();
    }
    else {
      cerr << scan.c_str(1) << ": " << strerror (errno) << '\n';
    }
  }
}

void
scan_xapian (sqlite3 *sqldb, writestamp ws, const string &path)
{
  double start_scan_time { time_stamp() };

  int rootfd = open (path.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (path + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  Xapian::Database xdb (path + "/.notmuch/xapian");

  print_time ("scanning message ids in xapian");
  scan_message_ids (sqldb, xdb);
  print_time ("gathering all tags");
  scan_tags (sqldb, xdb);
  print_time ("scanning directories in xapian");
  scan_directories (sqldb, ws, xdb, rootfd);
  print_time ("scanning filenames in modified directories");
  scan_by_directory (sqldb, ws, xdb);
  print_time ("finding changed files in file system");
  find_changed_files (sqldb, ws, rootfd);
  print_time ("processing deleted messages");
  fmtexec (sqldb, "UPDATE messages SET"
	   " docid = NULL, tags = NULL, replica = %lld, version = %lld"
	   " WHERE docid NOT IN"
	   " (SELECT docid FROM current_messages INNER JOIN messages"
	   " USING (docid, message_id));", ws.first, ws.second);
  print_time ("updating tags");
  fmtexec (sqldb, "INSERT OR REPLACE INTO"
	   " messages (docid, message_id, tags, replica, version) "
	   "SELECT cm.docid, cm.message_id, cm.tags, %lld, %lld"
	   " FROM current_messages cm LEFT OUTER JOIN messages m"
	   " USING (docid) WHERE m.tags IS NOT cm.tags",
	   ws.first, ws.second);
  print_time ("done");

  setconfig (sqldb, "last_scan", start_scan_time);
}
