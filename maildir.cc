
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fts.h>
#include <sys/stat.h>
#include <openssl/sha.h>

#include "muchsync.h"

using namespace std;

bool
foreach_msg (const string &path, function<void (FTSENT *)> action)
{
  char *paths[] {const_cast<char *> (path.c_str()), nullptr};
  unique_ptr<FTS, decltype (&fts_close)>
    ftsp {fts_open (paths, FTS_LOGICAL, nullptr), fts_close};
  if (!ftsp)
    return false;
  bool incur = false, foundcur = false;
  while (FTSENT *f = fts_read (ftsp.get())) {
    if (incur) {
      if (f->fts_info == FTS_D)
	fts_set (ftsp.get(), f, FTS_SKIP);
      else if (f->fts_info == FTS_DP) {
	assert (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new"));
	incur = false;
      }
      else if (f->fts_name[0] != '.' && strchr (f->fts_name, ':'))
	action (f);
    }
    else if (f->fts_info == FTS_D) {
      if (!strcmp (f->fts_name, "cur") || !strcmp (f->fts_name, "new"))
	incur = foundcur = true;
      else if (f->fts_statp->st_nlink <= 2)
	fts_set (ftsp.get(), f, FTS_SKIP);
    }
  }
  return foundcur;
}

bool
get_header (istream &in, string &name, string &value)
{
  string line;
  getline (in, line);
  string::size_type idx = line.find (':');
  if (idx == string::npos)
    return false;
  name.resize (idx);
  for (string::size_type i = 0; i < idx; i++)
    name[i] = tolower (line[i]);
  value = line.substr (idx+1, string::npos);
  while (in.peek() == ' ' || in.peek() == '\t') {
    getline (in, line);
    value += line;
  }
  return true;
}

string
hexdump (const string &s)
{
  ostringstream os;
  os << hex << setw (2) << setfill ('0');
  for (auto c : s)
    os << (int (c) & 0xff);
  return os.str ();
}

string
get_sha (ifstream &msg)
{
  msg.seekg (0, ios_base::beg);
  SHA_CTX ctx;
  SHA1_Init (&ctx);
  while (msg) {
    char buf[8192];
    streamsize n = msg.readsome (buf, sizeof (buf));
    if (n <= 0)
      break;
    SHA1_Update (&ctx, buf, n);
  }
  unsigned char resbuf[SHA256_DIGEST_LENGTH];
  SHA1_Final (resbuf, &ctx);
  string res { reinterpret_cast<const char *> (resbuf), sizeof (resbuf) };
  return msg.fail() ? "" : hexdump (res);
}

bool
get_msgid (ifstream &msg, string &msgid)
{
  msg.seekg (0, ios_base::beg);
  string name, val;
  while (get_header (msg, name, val))
    if (name == "message-id") {
      string::size_type b{0}, e{val.size()};
      while (b < e && isspace(val[b]))
	++b;
      if (b < e && val[b] == '<')
	++b;
      while (b < e && isspace(val[e-1]))
	--e;
      if (b < e && val[e-1] == '>')
	--e;
      msgid = val.substr (b, e-b);
      return true;
    }
  return false;
}

bool
get_msgid (const string &file, string &msgid)
{
  ifstream msg (file);
  return get_msgid (msg, msgid);
}
