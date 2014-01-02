#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <fcntl.h>
#include <unistd.h>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

const char messages_def[] =
  R"(CREATE TABLE IF NOT EXISTS messages (
  docid INTEGER UNIQUE,
  message_id TEXT UNIQUE NOT NULL,
  tags TEXT,
  replica INTEGER,
  version INTEGER);)";
const char xapian_filenames_def[] =
  R"(CREATE TABLE IF NOT EXISTS xapian_filenames (
  docid INTEGER,
  dir_docid INTEGER,
  path TEXT PRIMARY KEY,
  replica INTEGER,
  version INTEGER);)";
const char xapian_directories_def[] =
  R"(CREATE TABLE IF NOT EXISTS xapian_directories (
  docid INTEGER PRIMARY KEY,
  path TEXT UNIQUE);)";

static double
time_stamp ()
{
  timespec ts;
  clock_gettime (CLOCK_MONOTONIC, &ts);
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
scan_xapian_directories (sqlite3 *sqldb, writestamp ws, Xapian::Database &xdb)
{
  save_old_table (sqldb, "xapian_directories", xapian_directories_def);
  sqlstmt_t insert (sqldb, "INSERT INTO xapian_directories (docid, path)"
                    " VALUES (?,?);");

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
      insert.param (i64 (*pi), dir).step().reset();
      if (++pi != pe)
        cerr << "warning: directory term " << *ti << " has multiple postings";
    }
  }

  fmtexec (sqldb, "UPDATE xapian_filenames "
	   "SET docid = NULL, dir_docid = NULL, replica = %lld, version = %lld"
	   " WHERE dir_docid IN (SELECT docid FROM"
	   " (SELECT * from old_xapian_directories"
	   " EXCEPT SELECT * from xapian_directories))",
	   ws.first, ws.second);
}

unordered_map<Xapian::docid, string>
get_xapian_directories (sqlite3 *sqldb)
{
  unordered_map<Xapian::docid, string> ret;
  sqlstmt_t s (sqldb, "SELECT docid, path FROM xapian_directories;");
  while (s.step().row())
    ret.emplace (s.integer(0), s.str(1));
  return ret;
}

void
scan_xapian_filenames (sqlite3 *sqldb, writestamp ws, Xapian::Database &xdb)
{
  auto dirs = get_xapian_directories (sqldb);

  sqlstmt_t lookup (sqldb, "SELECT docid, dir_docid FROM xapian_filenames"
		    " WHERE path = ?");
  sqlstmt_t insert (sqldb, "INSERT INTO xapian_filenames"
		    " (docid, dir_docid, path, replica, version)"
		    " VALUES (?, ?, ?, %lld, %lld);", ws.first, ws.second);

  for (Xapian::TermIterator
	 ti = xdb.allterms_begin(notmuch_file_direntry_prefix),
	 te = xdb.allterms_end(notmuch_file_direntry_prefix);
       ti != te; ti++) {
    string term = *ti;
    char *dirent;
    Xapian::docid dirno;
    if (term.size() <= notmuch_file_direntry_prefix.size()
	|| (dirno = strtoul(term.c_str() + notmuch_file_direntry_prefix.size(),
			    &dirent, 10), *dirent++ != ':')) {
      cerr << "warning: malformed file direntry " << term << '\n';
      continue;
    }
    auto dp = dirs.find (dirno);
    if (dp == dirs.end()) {
      cerr << "warning: dir entry with unknown directory " << term << '\n';
      continue;
    }

    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);
    if (pi == pe)
      cerr << "warning: file direntry term " << *ti << " has no postings";
    else {
      string path = dp->second + "/" + dirent;
      if (lookup.reset().bind_text(1, path).step().done()
	  || lookup.integer(0) != *pi
	  || lookup.integer(1) != dirno)
	insert.reset().param(i64(*pi), dirno, path).step();
      if (++pi != pe)
	cerr << "warning: file direntry term " << *ti
	     << " has multiple postings";
    }
  }
}

void
find_deleted (sqlite3 *sqldb, writestamp ws, const string &path)
{
  int fd = open (path.c_str(), O_RDONLY);
  if (fd < 0)
    throw runtime_error (path + ": " + strerror (errno));
  cleanup _c (close, fd);

  sqlstmt_t scan (sqldb, "SELECT rowid, path FROM xapian_filenames"
		  " WHERE docid IS NOT NULL;");
  sqlstmt_t delfile (sqldb, "UPDATE xapian_filenames "
		     "SET docid = NULL, replica = %lld, version = %lld"
		     " WHERE rowid = ?;", ws.first, ws.second);
  while (scan.step().row())
    if (faccessat (fd, scan.c_str(1), 0, 0) && errno == ENOENT) {
      cout << "deleting " << scan.str(1) << "\n";
      delfile.reset().param(scan.integer(0)).step();
    }
}

void
scan_xapian (sqlite3 *sqldb, writestamp ws, const string &path)
{
  Xapian::Database xdb (path + "/.notmuch/xapian");

  fmtexec (sqldb, "pragma secure_delete = 0;");
  fmtexec (sqldb, messages_def);
  fmtexec (sqldb, xapian_filenames_def);

  print_time ("scanning directories");
  scan_xapian_directories (sqldb, ws, xdb);
  print_time ("scanning filenames");
  scan_xapian_filenames (sqldb, ws, xdb);
  print_time ("finding deleted files");
  find_deleted (sqldb, ws, path);
  print_time ("scanning message ids");
  scan_message_ids (sqldb, xdb);
  print_time ("gathering all tags");
  scan_tags (sqldb, xdb);
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
}
