
#include <stdio.h>
#include <stdlib.h>
#include "muchsync.h"

using namespace std;

int
main (int argc, char **argv)
{
  if (argc != 3) {
    fprintf (stderr, "usage error\n");
    exit (1);
  }

  sqlite3 *db = dbopen (argv[1]);
  if (!db)
    exit (1);

  printf ("self = %lld\n", getconfig<i64>(db, "self"));

  fmtexec (db, "BEGIN;");
  try {
    // scan_xapian (db, argv[2]);
    //scan_notmuch (db, argv[2]);
    scan_maildir (db, argv[2]);
    fmtexec(db, "COMMIT;"); // see what happened
  }
  catch (std::runtime_error e) {
    fprintf (stderr, "%s\n", e.what ());
    fmtexec(db, "COMMIT;"); // XXX - see what happened
    exit (1);
  }
  sqlite3_close_v2 (db);

  return 0;
}
