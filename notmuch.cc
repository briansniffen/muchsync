#include <cstring>
#include <iomanip>
#include <memory>
#include <iostream>
#include <sstream>
#include <string>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";

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

string
sanitize_tag (const string &tag)
{
  assert (!strncmp (tag.c_str(), notmuch_tag_prefix.c_str(),
		    notmuch_tag_prefix.length()));

  ostringstream tagbuf;
  tagbuf.fill('0');
  tagbuf.setf(ios::hex, ios::basefield);

  char c;
  for (const char *p = tag.c_str() + 1; (c = *p); p++)
    if (isalnum (c) || (c >= '+' && c <= '.')
	|| c == '_' || c == '@' || c == '=')
      tagbuf << c;
    else
      tagbuf << '%' << setw(2) << int (uint8_t (c));

  return tagbuf.str ();
}

const char message_ids_def[] = R"(CREATE TABLE IF NOT EXISTS messages (
 docid INTEGER PRIMARY KEY,
 message_id TEXT UNIQUE NOT NULL,
 tags TEXT);)";

void
scan_message_ids (sqlite3 *sqldb, Xapian::Database &xdb)
{
  fmtexec (sqldb, message_ids_def);
  fmtexec (sqldb, "DROP TABLE IF EXISTS old_messages; "
	   "ALTER TABLE messages RENAME TO old_messages");
  fmtexec (sqldb, message_ids_def);

  sqlstmt_t s (sqldb, "INSERT INTO messages(docid, message_id, tags)"
	       " VALUES (?, ?, '');");

  for (Xapian::ValueIterator
	 vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
	 ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);
       vi != ve; vi++) {
    // printf ("%lld '%s'\n", vi.get_docid(), (*vi).c_str());
    s.reset ();
    s.bind (1, vi.get_docid ());
    s.bind (2, *vi);
    s.step ();
  }

  /*
  Xapian::Enquire enquire (xdb);
  enquire.set_weighting_scheme (Xapian::BoolWeight());
  enquire.set_query (Xapian::Query ("Tmail"));
  Xapian::MSet mset = enquire.get_mset (0, xdb.get_doccount());
  for (Xapian::MSetIterator m = mset.begin(); m != mset.end(); m++) {
    Xapian::Document doc = m.get_document ();
    s.reset ();
    s.bind (1, *m);
    s.bind (2, doc.get_value (1));
    s.step ();
  }
  */
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
    string tag = sanitize_tag (*ti);
    cout << tag << "\n";
    s.bind (1, tag);
    for (Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
	   pe = xdb.postlist_end (*ti); pi != pe; pi++) {
      i64 docid = *pi;
      s.bind (2, docid);
      s.step ();
      s.reset ();
    }
  }
}

void
scan_xapian (sqlite3 *sqldb, const string &path)
{
  Xapian::Database xdb (path + "/.notmuch/xapian");
  scan_message_ids (sqldb, xdb);
  scan_tags (sqldb, xdb);
}
