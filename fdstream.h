// -*- C++ -*-

#include <iostream>
#include <memory>

/* C++ sucks because the standard A) requires that the standard
 * library work with C FILE *s for file descriptors 0, 1, and 2, and
 * B) goes out of its way to make sure you can't re-use the same code
 * with other file descriptors.  So we're just going to require gcc's
 * particular implementation of this feature to use with other file
 * descriptors.  Too bad this will break LLVM.  */
#include <ext/stdio_filebuf.h>

template<typename Stream, std::ios_base::openmode default_openmode>
class genfdstream
  : public Stream
{
protected:
  using buf_t = __gnu_cxx::stdio_filebuf<typename Stream::char_type>;
  std::unique_ptr<buf_t> buf_;
public:
  explicit genfdstream(int fd, std::ios_base::openmode mode = default_openmode)
    : buf_(new buf_t (fd, mode)) {
    this->rdbuf(buf_.get());
  }
  genfdstream(genfdstream &&s) : buf_(move(s.buf_)) {
    this->rdbuf(buf_.get());
  }
};

using ifdstream = genfdstream<std::istream, std::ios_base::in>;
using ofdstream = genfdstream<std::ostream, std::ios_base::out>;
using fdstream = genfdstream<std::basic_iostream<char>,
			     std::ios_base::in|std::ios_base::out>;
