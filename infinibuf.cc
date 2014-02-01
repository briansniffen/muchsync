#include <array>
#include <cassert>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <streambuf>
#include <unistd.h>
#include <sys/socket.h>

#include <thread>

using namespace std;

class infinibuf {
protected:
  static constexpr int chunksize_ = 0x10000;

  deque<char *> data_;
  int gpos_;
  int ppos_;
  bool eof_{false};
  int errno_{0};
  const int startpos_;		// For putback

public:
  explicit infinibuf(int sp = 8)
    : gpos_(sp), ppos_(sp), startpos_(sp) {
    data_.push_back (new char[chunksize_]);
  }
  infinibuf(const infinibuf &) = delete;
  ~infinibuf() { for (char *p : data_) delete[] p; }
  infinibuf &operator= (const infinibuf &) = delete;
		   
  // These functions are not thread safe
  bool empty() { return data_.size() == 1 && gpos_ == ppos_; }
  bool eof() { return eof_; }
  int err() { return errno_; }
  void err(int num) { eof_ = num; errno_ = num; }

  char *eback() { return data_.front(); }
  char *gptr() { return eback() + gpos_; }
  int gsize() { return (data_.size() > 1 ? chunksize_ : ppos_) - gpos_; }
  char *egptr() { return gptr() + gsize(); }
  void gbump(int n);

  char *pbase() { return data_.back(); }
  char *pptr() { return pbase() + ppos_; }
  int psize() { return chunksize_ - gpos_; }
  char *epptr() { return pptr() + psize(); }
  bool pbump(int n);
  void peof() { eof_ = true; }

  // These functions are thread safe
  virtual void lock() {}
  virtual void unlock() {}
  virtual void notempty() {}
  virtual void gwait() {}

  bool output(int fd);
  bool input(int fd);
};

class infinibuf_infd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_infd (int fd, int sp = 8)
    : infinibuf(sp), fd_(fd) {}
  void gwait() override { input(fd_); }
};

class infinibuf_outfd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_outfd (int fd, int sp = 8)
    : infinibuf(sp), fd_(fd) {}
  void notempty() override { output(fd_); }
};

void
infinibuf::gbump(int n)
{
  gpos_ += n;
  assert (gpos_ > 0 && gpos_ <= chunksize_);
  if (gpos_ == chunksize_) {
    assert (data_.size() > 1);
    delete[] data_.front();
    data_.pop_front();
    gpos_ = startpos_;
  }
}

bool
infinibuf::pbump(int n)
{
  assert (n >= 0 && n <= psize() && !eof_);
  bool wasempty (empty());
  ppos_ += n;
  if (ppos_ == chunksize_) {
    char *chunk = new char[chunksize_];
    memcpy(chunk, data_.back() + chunksize_ - startpos_, startpos_);
    data_.push_back(chunk);
    ppos_ = startpos_;
  }
  return wasempty;
}

bool
infinibuf::output(int fd)
{
  for (;;) {
    lock();
    char *p = gptr();
    size_t nmax = gsize();
    bool iseof = eof();
    int error = err();
    unlock();

    if (error)
      throw runtime_error (string("infinibuf::output: ") + strerror(error));
    else if (!nmax && iseof) {
      shutdown(fd, SHUT_WR);
      return false;
    }
    ssize_t n = write(fd, p, nmax);
    if (n > 0) {
      lock();
      gbump(n);
      unlock();
    }
    else {
      if (errno == EAGAIN)
	return true;
      lock();
      err(error = errno);
      unlock();
    }
  }
}

bool
infinibuf::input (int fd)
{
  lock();
  char *p = pptr();
  size_t nmax = psize();
  int error = err();
  unlock();

  if (error)
    throw runtime_error (string("infinibuf::input: ") + strerror(error));
  ssize_t n = read(fd, p, nmax);
  if (n < 0) {
    if (errno == EAGAIN)
      return true;
    lock();
    err(errno);
    unlock();
    throw runtime_error (string("infinibuf::input: ") + strerror(errno));
  }

  lock();
  bool wasempty = empty();
  if (n > 0)
    pbump(n);
  else
    peof();
  if (wasempty)
    notempty();
  unlock();
  return n > 0;
}

class infinistreambuf : public streambuf {
protected:
  infinibuf *ib_;
  int_type underflow() override;
  int_type overflow(int_type ch) override;
  int sync() override;
public:
  explicit infinistreambuf (infinibuf *ib);
  infinibuf *get_infinibuf() { return ib_; }
};

infinistreambuf::int_type
infinistreambuf::underflow()
{
  ib_->lock();
  ib_->gbump(gptr() - ib_->gptr());
  while (ib_->gsize() == 0 && !ib_->eof())
    ib_->gwait();
  setg(ib_->eback(), ib_->gptr(), ib_->egptr());
  bool eof = ib_->eof();
  ib_->unlock();
  return eof ? traits_type::eof() : traits_type::to_int_type (*gptr());
}

infinistreambuf::int_type
infinistreambuf::overflow(int_type ch)
{
  sync();
  *pptr() = ch;
  pbump(1);
  return traits_type::not_eof(ch);
}

int
infinistreambuf::sync()
{
  ib_->lock();
  ib_->pbump(pptr() - ib_->pptr());
  setp(ib_->pptr(), ib_->epptr());
  int err = ib_->err();
  ib_->unlock();
  return err ? -1 : 0;
}

infinistreambuf::infinistreambuf (infinibuf *ib)
  : ib_(ib)
{
  ib->lock();
  setg(ib_->eback(), ib_->gptr(), ib_->egptr());
  setp(ib_->pptr(), ib_->epptr());
  ib->unlock();
}


#if 0


void
reader(shared_ptr<chan> c, int fd)
{
  while (c->input(fd))
    ;
}

void
writer(shared_ptr<chan> c, int fd)
{
  while (c->output(fd))
    c->gwait();
}

int
omain (int argc, char **argv)
{
  chanbuf icb;
  thread it (reader, icb.getchan(), 0);
  istream xin(&icb);

  chanbuf ocb;
  thread ot (writer, ocb.getchan(), 1);
  ostream xout(&ocb);

  xin.tie (&xout);

  string x;
  while ((xin >> x).good()) {
    cout << "word: " << x << '\n';
  }

  ocb.getchan()->peof();

  //writer (c, 1);
  it.join();
  ot.join();

  return 0;
}
#endif

int
main (int argc, char **argv)
{
  infinibuf_infd iib(0);
  infinistreambuf inb (&iib);
  istream xin (&inb);

  infinibuf_outfd oib(1);
  infinistreambuf outb (&oib);
  ostream xout (&outb);

  string x;
  while ((xin >> x).good()) {
    cout << "word: " << x << '\n';
  }
  oib.peof();
}

/*

c++ -std=c++11 -Wall -Werror -pthread infinibuf.cc

*/
