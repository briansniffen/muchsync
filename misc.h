// -*- C++ -*-

#ifndef _MUCHSYNC_MISC_H_
#define _MUCHSYNC_MISC_H_ 1

#include <cstddef>
#include <string>
#include <openssl/sha.h>

using std::string;

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

#endif /* !_MUCHSYNC_MISC_H_ 1 */
