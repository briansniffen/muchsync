#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "misc.h"

using namespace std;

string
percent_encode (const string &raw)
{
  ostringstream outbuf;
  outbuf.fill('0');
  outbuf.setf(ios::hex, ios::basefield);

  for (char c : raw) {
    if (isalnum (c) || (c >= '+' && c <= '.')
	|| c == '_' || c == '@' || c == '=')
      outbuf << c;
    else
      outbuf << '%' << setw(2) << int (uint8_t (c));
  }
  return outbuf.str ();
}

inline int
hexdigit (char c)
{
  if (c >= '0' && c <= '9')
    return c - '0';
  else if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  else
    throw runtime_error ("precent_decode: illegal hexdigit " + string (1, c));
}

string
percent_decode (const string &encoded)
{
  ostringstream outbuf;
  int escape_pos = 0, escape_val = 0;
  for (char c : encoded) {
    switch (escape_pos) {
    case 0:
      if (c == '%')
	escape_pos = 1;
      else
	outbuf << c;
      break;
    case 1:
      escape_val = hexdigit(c) << 4;
      escape_pos = 2;
      break;
    case 2:
      escape_pos = 0;
      outbuf << char (escape_val | hexdigit(c));
      break;
    }
  }
  if (escape_pos)
    throw runtime_error ("percent_decode: incomplete escape");
  return outbuf.str();
}

std::istream &
input_match (std::istream &in, char want)
{
  char got;
  if ((in >> got) && got != want)
    in.setstate (std::ios_base::failbit);
  return in;
}

bool
hash_ok (const string &hash)
{
  if (hash.size() != 2*hash_ctx::output_bytes)
    return false;
  for (char c : hash)
    if (c < '0' || c > 'f' || (c > '9' && c < 'a'))
      return false;
  return true;
}

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
hash_ctx::final()
{
  unsigned char resbuf[output_bytes];
  SHA1_Final (resbuf, &ctx_);
  return hexdump ({ reinterpret_cast<const char *> (resbuf), sizeof (resbuf) });
}

using stp = std::chrono::time_point<std::chrono::steady_clock>;

stp start_time_stamp{stp::clock::now()};
stp last_time_stamp{start_time_stamp};

void
print_time (string msg)
{
  using namespace std::chrono;
  stp now = stp::clock::now();
  if (opt_verbose > 0) {
    auto oldFlags = cerr.flags();
    cerr.setf (ios::fixed, ios::floatfield);
    cerr << msg << "... " << duration<double>(now - start_time_stamp).count()
	 << " (+" << duration<double>(now - last_time_stamp).count() << ")\n";
    cerr.flags (oldFlags);
  }
  last_time_stamp = now;
}
