// -*- C++ -*-

#ifndef _MUCHSYNC_MISC_H_
#define _MUCHSYNC_MISC_H_ 1

#include <cstddef>
#include <string>
#include <time.h>
#include <sys/time.h>
#include <openssl/sha.h>

#ifndef ST_MTIM
#define ST_MTIM 1
#endif //!ST_MTIM

using std::string;

extern int opt_verbose;

template<typename C> inline typename C::mapped_type
find_default (typename C::mapped_type def, const C &c, typename C::key_type k)
{
  auto i = c.find(k);
  return i == c.end() ? def : i->second;
}

std::istream &input_match (std::istream &in, char want);
string percent_encode (const string &raw);
string percent_decode (const string &escaped);

class hash_ctx {
  SHA_CTX ctx_;
public:
  static constexpr size_t output_bytes = SHA_DIGEST_LENGTH;
  hash_ctx() { init(); }
  void init() { SHA1_Init(&ctx_); }
  void update(const void *buf, size_t n) { SHA1_Update (&ctx_, buf, n); }
  string final();
};
bool hash_ok (const string &hash);

constexpr double
ts_to_double (const timespec &ts)
{
  return ts.tv_sec + ts.tv_nsec / 1000000000.0;
}

void print_time (string msg);

#endif /* !_MUCHSYNC_MISC_H_ 1 */
