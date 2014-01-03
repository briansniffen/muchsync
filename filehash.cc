#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/sha.h>
#include <xapian.h>

#include "muchsync.h"

using namespace std;

const char hashes_schema[] =
R"(CREATE TABLE IF NOT EXISTS hashes (
  hash_id INTEGER PRIMARY KEY AUTOINCREMENT,
  hash TEXT UNIQUE NOT NULL,
  size INTEGER NOT NULL,
  message_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS file_hashes (
  file_id INTEGER PRIMARY KEY,
  hash_id INTEGER NOT NULL,
  mtime REAL,
  inode INTEGER);
CREATE INDEX IF NOT EXISTS inode_index
  ON file_hashes (inode, mtime, hash_id);
)";

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
get_sha (int dfd, const char *direntry, struct stat *sbp)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);
  if (sbp && fstat (fd, sbp))
    throw runtime_error (string() + direntry + ": " + strerror (errno));

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

void
hash_files (sqlite3 *sqldb, writestamp ws, const string &path)
{
  int rootfd = open (path.c_str(), O_RDONLY);
  if (rootfd < 0)
    throw runtime_error (path + ": " + strerror (errno));
  cleanup _c (close, rootfd);

  fmtexec (sqldb, hashes_schema);
  sqlstmt_t
    need_hash (sqldb,
R"(SELECT file_id, path || '/' || name, docid, message_id
 FROM xapian_files NATURAL JOIN xapian_dirs NATURAL JOIN message_ids
 WHERE file_id NOT IN (SELECT file_id FROM file_hashes);)"),
    find_hash (sqldb, "SELECT hash_id FROM hashes WHERE hash = ?;"),
    create_hash (sqldb, "INSERT INTO hashes"
		 " (hash, size, message_id) VALUES (?,?,?);"),
    create_filehash (sqldb, "INSERT INTO file_hashes"
		     " (file_id, hash_id, mtime, inode) VALUES (?,?,?,?);");
 

  print_time ("hashing new files");

  while (need_hash.step().row()) {
    struct stat sb;
    const char *pathname = need_hash.c_str(1);
    string hashval;
    i64 hash_id;

    try { hashval = get_sha (rootfd, pathname, &sb); }
    catch (runtime_error e) { cerr << e.what() << '\n'; continue; }

    if (find_hash.reset().param(hashval).step().row()) {
      hash_id = find_hash.integer(0);
    }
    else {
      create_hash.reset()
	.param(hashval, i64(sb.st_size), need_hash.value(3))
	.step();
      hash_id = sqlite3_last_insert_rowid (sqldb);
    }

    create_filehash.reset()
      .param(need_hash.value(0), hash_id,
	     ts_to_double(sb.st_mtim), i64(sb.st_ino))
      .step();

#if 0
    find_rename.reset().param(i64(sb.st_ino), ts_to_double(sb.st_mtim),
			      i64(sb.st_size)).step();
    i64 hash_id;
    if (find_rename.row()) {
      cout << "assuming " << find_rename.str(1) << " was renamed or linked to "
	   << pathname << '\n';
      hash_id = find_rename.integer(0);
    }
    else {
      try {
	string hash = get_sha (rootfd, pathname, &sb);
      } catch (runtime_error e) { cerr << e.what() << '\n'; continue; }
      create_hash.reset().param(hash_id, i64(sb.st_size),
				need_hash.value(3)).step();
      hash_id = sqlite3_last_insert_rowid (sqldb);
    }
    create_filehash.reset().param(need_hash.value(0),hash_id,
				  ts_to_double(sb.st_mtim),
				  i64(sb.st_ino)).step();
#endif
  }

  print_time ("cleaning up");

  fmtexec (sqldb, "DELETE FROM file_hashes WHERE file_id IN"
	   " (SELECT file_id FROM deleted_files); ");

  print_time ("done");
}
