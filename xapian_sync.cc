#include <functional>
#include <iostream>
#include <xapian.h>

#include "muchsync.h"

using namespace std;

constexpr int NOTMUCH_VALUE_MESSAGE_ID = 1;
const string notmuch_tag_prefix = "K";

const char xapian_schema_def[] =
  R"(CREATE TABLE IF NOT EXISTS tags (
  tag TEXT NOT NULL,
  docid INTEGER NOT NULL,
  UNIQUE (docid, tag),
  UNIQUE (tag, docid));
CREATE TABLE IF NOT EXISTS message_ids (
  message_id TEXT UNIQUE NOT NULL,
  docid INTEGER PRIMARY KEY);
)";

template<typename T> void
sync_table (sqlstmt_t &s, T &t, T &te,
	    function<int(sqlstmt_t &s, T &t)> cmpfn,
	    function<void(sqlstmt_t *s, T *t)> update)
{
  s.step();
  while (s.row()) {
    int cmp {t == te ? -1 : cmpfn (s, t)};
    if (cmp == 0) {
      update (&s, &t);
      s.step();
      ++t;
    }
    else if (cmp < 0) {
      update (&s, nullptr);
      s.step();
    }
    else {
      update (nullptr, &t);
      ++t;
    }
  }
  while (t != te) {
    update (NULL, &t);
    ++t;
  }
}

static void
xapian_scan_tags (sqlite3 *sqldb, Xapian::Database &xdb)
{
  static const char modified_docids_def[] =
    R"(DROP TRIGGER IF EXISTS tag_delete;
DROP TRIGGER IF EXISTS tag_insert;
CREATE TEMP TRIGGER tag_delete AFTER DELETE ON main.tags
  WHEN old.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (old.docid); END;
CREATE TEMP TRIGGER tag_insert AFTER INSERT ON main.tags
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;
)";
  fmtexec (sqldb, modified_docids_def);

  sqlstmt_t
    scan (sqldb, "SELECT docid FROM tags WHERE tag = ? ORDER BY docid ASC;"),
    add_tag (sqldb, "INSERT INTO tags (docid, tag) VALUES (?, ?);"),
    del_tag (sqldb, "DELETE FROM tags WHERE docid = ? & tag = ?;");

  for (Xapian::TermIterator ti = xdb.allterms_begin(notmuch_tag_prefix),
	 te = xdb.allterms_end(notmuch_tag_prefix); ti != te; ti++) {
    string tag = tag_from_term (*ti);
    cout << "  " << tag << "\n";
    scan.reset().bind_text(1, tag);
    add_tag.reset().bind_text(2, tag);
    del_tag.reset().bind_text(2, tag);

    Xapian::PostingIterator pi = xdb.postlist_begin (*ti),
      pe = xdb.postlist_end (*ti);

    sync_table<Xapian::PostingIterator>
      (scan, pi, pe,
       [] (sqlstmt_t &s, Xapian::PostingIterator &p) -> int {
	 return s.integer(0) - *p;
       },
       [&] (sqlstmt_t *sp, Xapian::PostingIterator *pp) {
	 if (!sp)
	   add_tag.reset().bind_int(1, **pp).step();
	 else if (!pp)
	   del_tag.reset().bind_value(1, sp->value(0)).step();
       });
  }
}

static void
xapian_scan_message_ids (sqlite3 *sqldb, Xapian::Database xdb)
{
  static const char deleted_docids_def[] =
    R"(DROP TABLE IF EXISTS deleted_docids;
DROP TRIGGER IF EXISTS message_id_delete;
DROP TRIGGER IF EXISTS message_id_insert;
CREATE TABLE deleted_docids (docid INTEGER PRIMARY KEY, message_id UNIQUE);
CREATE TEMP TRIGGER message_id_delete AFTER DELETE ON main.message_ids
  BEGIN INSERT INTO deleted_docids (docid, message_id)
        VALUES (old.docid, old.message_id);
  END;
CREATE TEMP TRIGGER message_id_insert AFTER INSERT ON main.message_ids
  WHEN new.docid NOT IN (SELECT * FROM modified_docids)
  BEGIN INSERT INTO modified_docids (docid) VALUES (new.docid); END;)";
  fmtexec (sqldb, deleted_docids_def);

  sqlstmt_t
    scan (sqldb, "\
SELECT message_id, docid FROM message_ids ORDER BY docid ASC;"),
    add_message (sqldb, "\
INSERT INTO message_ids (message_id, docid) VALUES (?, ?);"),
    del_message (sqldb, "DELETE FROM message_ids WHERE docid = ?;");

  Xapian::ValueIterator
    vi = xdb.valuestream_begin (NOTMUCH_VALUE_MESSAGE_ID),
    ve = xdb.valuestream_end (NOTMUCH_VALUE_MESSAGE_ID);

  sync_table<Xapian::ValueIterator>
    (scan, vi, ve,
     [] (sqlstmt_t &s, Xapian::ValueIterator &vi) -> int {
       return s.integer(1) - vi.get_docid();
     },
     [&] (sqlstmt_t *sp, Xapian::ValueIterator *vip) {
       if (!sp)
	 add_message.reset().param(**vip, i64(vip->get_docid())).step();
       else if (!vip)
	 del_message.reset().param(sp->value(1)).step();
       else if (sp->str(0) != **vip) {
	 // This should be really unusual
	 cerr << "warning: message id changed from <"
	      << sp->str(0) << "> to <" << **vip << ">\n";
	 del_message.reset().param(sp->value(1)).step();
	 add_message.reset().param(**vip, i64(vip->get_docid())).step();
       }
     });
	      
}

void
xapian_scan (sqlite3 *sqldb, writestamp ws, const string &path)
{
  Xapian::Database xdb (path + "/.notmuch/xapian");

  fmtexec (sqldb, xapian_schema_def);
  fmtexec (sqldb, "DROP TABLE IF EXISTS modified_docids;"
	   "CREATE TABLE modified_docids (docid INTEGER PRIMARY KEY);");

  print_time ("scanning message IDs");
  xapian_scan_message_ids (sqldb, xdb);
  print_time ("scanning tags");
  xapian_scan_tags (sqldb, xdb);
  print_time ("done tags");
}
