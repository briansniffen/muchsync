
#include <iostream>
#include <limits>
#include <cstdio>
#include <unistd.h>

#include "muchsync.h"

using namespace std;

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

extern "C" void
sqlite_percent_encode (sqlite3_context *ctx, int argc, sqlite3_value **av)
{
  assert (argc == 1);
  string escaped = percent_encode (reinterpret_cast <const char *>
				   (sqlite3_value_text (av[0])));
  sqlite3_result_text (ctx, escaped.c_str(), escaped.size(),
		       SQLITE_TRANSIENT);
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
    (SELECT hash_id, group_concat(link_count || ':' || percent_encode(dir_path)) catdirs
     FROM maildir_links NATURAL JOIN maildir_dirs GROUP BY hash_id)
        USING(hash_id)
  WHERE (h.version > ifnull(pvh.known_version,-1))
        | (x.version > ifnull(pvx.known_version,-1))
;)");

  for (changed.step(); changed.row(); changed.step()) {
    cout << "200-" << changed.str(0) << ' '
	 << percent_encode (changed.str(3))
	 << " R" << changed.integer(4) << '=' << changed.integer(5)
	 << " (" << changed.str(6)
	 << ") R" << changed.integer(1) << '=' << changed.integer(2)
	 << " (" << changed.str(7) << ")\n";
  }

  cout << "200 Synchronized " << show_sync_vector(vv) << '\n';
}

void
muchsync_server (sqlite3 *db, const string &maildir)
{
  sqlite3_create_function_v2 (db, "percent_encode", 1, SQLITE_UTF8,
			      nullptr, &sqlite_percent_encode, nullptr,
			      nullptr, nullptr);

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
    else if (cmd == "sync") {
      versvector vv;
      string tail;
      if (!getline(cin, tail) || !read_sync_vector(tail, vv))
	cout << "500 could not parse vector\n";
      else
	cmd_sync (db, vv);
      continue;
    }
    else
      cout << "500 unknown verb " << cmd << '\n';
    cin.ignore (numeric_limits<streamsize>::max(), '\n');
  }
}

