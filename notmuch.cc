#include <iomanip>
#include <memory>
#include <sstream>
#include <string>

#include <xapian.h>

#include "muchsync.h"

using namespace std;

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

const char message_ids_def[] = "\
CREATE TABLE IF NOT EXISTS message_ids (\
 docid INTEGER PRIMARY KEY,\
 message_id TEXT UNIQUE NOT NULL);";

bool
scan_message_ids (sqlite3 *sqldb, const string &path)
{
  Xapian::Database xdb (path + "/.notmuch/xapian");

  if (fmtexec (sqldb, message_ids_def)
      || fmtexec (sqldb, "DROP TABLE IF EXISTS old_message_ids; "
		  "ALTER TABLE message_ids RENAME TO old_message_ids")
      || fmtexec (sqldb, message_ids_def))
    return false;

  for (Xapian::ValueIterator vi = xdb.valuestream_begin (1),
	 ve = xdb.valuestream_end (1);
       vi != ve; vi++) {
    printf ("%lld %s\n", vi.get_docid(), (*vi).c_str());
  }

  return true;
}
