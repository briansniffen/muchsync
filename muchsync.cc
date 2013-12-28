
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
  sqlite3_exec (db, "BEGIN;", NULL, NULL, NULL);
  /*
  if (scan_notmuch (argv[2], db)) {
    fprintf (stderr, "scan_notmuch failed\n");
    fmtexec(db, "ROLLBACK;");
    exit (1);
  }
  */
  if (!scan_message_ids (db, argv[2])) {
    fprintf (stderr, "scan_notmuch failed\n");
    fmtexec(db, "ROLLBACK;");
    exit (1);
  }
  sqlite3_exec (db, "COMMIT;", NULL, NULL, NULL);
  sqlite3_close_v2 (db);


  return 0;
}
