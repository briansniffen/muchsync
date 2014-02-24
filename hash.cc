#include <iomanip>
#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "muchsync.h"

using namespace std;

static string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setfill('0');
  for (auto c : s)
    os << setw(2) << (int (c) & 0xff);
  string ret = os.str();
  if (ret.size() != 2 * s.size()) {
    cerr << ret.size() << " != 2 * " << s.size () << "\n";
    cerr << "s[0] == " << hex << unsigned (s[0]) << ", s.back() = "
	 << unsigned (s.back()) << "\n";
    terminate();
  }
  return ret;
}

string
get_sha (int dfd, const char *direntry, i64 *sizep)
{
  int fd = openat(dfd, direntry, O_RDONLY);
  if (fd < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  cleanup _c (close, fd);

  hash_ctx ctx;
  char buf[32768];
  int n;
  i64 sz = 0;
  while ((n = read (fd, buf, sizeof (buf))) > 0) {
    ctx.update (buf, n);
    sz += n;
  }
  if (n < 0)
    throw runtime_error (string() + direntry + ": " + strerror (errno));
  if (sizep)
    *sizep = sz;
  return ctx.final();
}

string
hash_ctx::final()
{
  unsigned char resbuf[output_bytes];
  SHA1_Final (resbuf, &ctx_);
  return hexdump ({ reinterpret_cast<const char *> (resbuf), sizeof (resbuf) });
}
