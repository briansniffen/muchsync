
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
  scan_notmuch (argv[2], db);
  sqlite3_exec (db, "COMMIT;", NULL, NULL, NULL);
  sqlite3_close_v2 (db);


  return 0;
}
