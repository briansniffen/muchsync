#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";
const string notmuch_directory_prefix = "XDIRECTORY";
const string notmuch_file_direntry_prefix = "XFDIRENTRY";

const char messages_def[] = R"(CREATE TABLE IF NOT EXISTS messages (
 docid INTEGER PRIMARY KEY,
 message_id TEXT UNIQUE NOT NULL,
 tags TEXT);)";
const char xapian_filenames_def[] =
  R"(CREATE TABLE IF NOT EXISTS xapian_filenames (
  docid INTEGER,
  filename TEXT UNIQUE);)";

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

  unique_ptr<notmuch_database_t, decltype(&notmuch_database_destroy)>
    _cleanup {notmuch, notmuch_database_destroy};

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


void
scan_message_ids (sqlite3 *sqldb, Xapian::Database &xdb)
{
  save_old_table (sqldb, "messages", messages_def);
  sqlstmt_t s (sqldb, "INSERT INTO messages(docid, message_id, tags)"
	       " VALUES (?, ?, '');");

  for (Xapian::ValueIterator
	 vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
	 ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);
       vi != ve; vi++) {
    // printf ("%lld '%s'\n", vi.get_docid(), (*vi).c_str());
    s.reset ();
    s.bind_int (1, vi.get_docid ());
    s.bind_text (2, *vi);
    s.step ();
  }
}

void
scan_tags (sqlite3 *sqldb, Xapian::Database &xdb)
{
    sqlstmt_t s (sqldb, R"(
UPDATE messages
  SET tags = (tags || (CASE tags WHEN '' THEN '' ELSE ' ' END) || ?)
  WHERE docid = ?;)");

  for (Xapian::TermIterator ti = xdb.allterms_begin(notmuch_tag_prefix),
	 te = xdb.allterms_end(notmuch_tag_prefix); ti != te; ti++) {
    string tag = tag_from_term (*ti);
    cout << tag << "\n";
    s.bind_text (1, tag);
    for (Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
	   pe = xdb.postlist_end (*ti); pi != pe; pi++) {
      s.bind_int (2, *pi);
      s.step ();
      s.reset ();
    }
  }
}

unordered_map<Xapian::docid, string>
xapian_directories (Xapian::Database &xdb)
{
  unordered_map<Xapian::docid, string> ret;

  for (Xapian::TermIterator
	 ti = xdb.allterms_begin(notmuch_directory_prefix),
	 te = xdb.allterms_end(notmuch_directory_prefix);
       ti != te; ti++) {
    string dir = (*ti).substr(notmuch_directory_prefix.length());
    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);
    if (pi == pe)
      cerr << "warning: directory term " << *ti << " has no postings";
    else {
      // cout << *pi << ' ' << dir << '\n';
      ret.emplace (*pi, dir);
      if (++pi != pe)
	cerr << "warning: directory term " << *ti << " has multiple postings";
    }
  }

  return ret;
}

void
scan_xapian_filenames (sqlite3 *sqldb, Xapian::Database &xdb)
{
  auto dirs = xapian_directories (xdb);
  save_old_table (sqldb, "xapian_filenames", xapian_filenames_def);
  sqlstmt_t s (sqldb, "INSERT INTO xapian_filenames(docid, filename)"
	       " VALUES (?, ?);");

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
    string path;
    try { path = dirs.at (dirno) + "/" + dirent; } catch (out_of_range) {
      cerr << "warning: unknown directory for " << term << '\n';
      continue;
    }

    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);
    if (pi == pe)
      cerr << "warning: file direntry term " << *ti << " has no postings";
    else {
      s.bind_int (1, *pi);
      s.bind_text (2, path);
      s.step();
      s.reset();
      if (++pi != pe)
	cerr << "warning: file direntry term " << *ti
	     << " has multiple postings";
    }
  }
}

void
scan_xapian (sqlite3 *sqldb, const string &path)
{
  Xapian::Database xdb (path + "/.notmuch/xapian");
  scan_xapian_filenames (sqldb, xdb);
  scan_message_ids (sqldb, xdb);
  scan_tags (sqldb, xdb);
}
