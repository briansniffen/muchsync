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
  static constexpr int default_startpos_ = 8;
  static constexpr int chunksize_ = 0x10000;

  deque<char *> data_;
  int gpos_;
  int ppos_;
  bool eof_{false};
  int errno_{0};
  const int startpos_;		// For putback

  virtual void notempty() {}

public:
  explicit infinibuf(int sp = default_startpos_)
    : gpos_(sp), ppos_(sp), startpos_(sp) {
    data_.push_back (new char[chunksize_]);
  }
  infinibuf(const infinibuf &) = delete;
  virtual ~infinibuf() { for (char *p : data_) delete[] p; }
  infinibuf &operator= (const infinibuf &) = delete;
		   
  // These functions are never thread safe:

  bool empty() { return data_.size() == 1 && gpos_ == ppos_; }
  bool eof() { return eof_; }
  int err() { return errno_; }
  void err(int num) { errno_ = num; peof(); }

  char *eback() { return data_.front(); }
  char *gptr() { return eback() + gpos_; }
  int gsize() { return (data_.size() > 1 ? chunksize_ : ppos_) - gpos_; }
  char *egptr() { return gptr() + gsize(); }
  void gbump(int n);
  virtual void gwait() {}

  char *pbase() { return data_.back(); }
  char *pptr() { return pbase() + ppos_; }
  int psize() { return chunksize_ - gpos_; }
  char *epptr() { return pptr() + psize(); }
  void pbump(int n);
  void peof() { eof_ = true; if (empty()) notempty(); }

  // These functions are thread safe for some subtypes:

  virtual void lock() {}
  virtual void unlock() {}
  bool output(int fd);
  bool input(int fd);

  static void output_loop(shared_ptr<infinibuf> ib, int fd);
  static void input_loop(shared_ptr<infinibuf> ib, int fd);
};

class infinibuf_infd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_infd (int fd, int sp = default_startpos_)
    : infinibuf(sp), fd_(fd) {}
  void gwait() override { input(fd_); }
};

class infinibuf_outfd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_outfd (int fd, int sp = default_startpos_)
    : infinibuf(sp), fd_(fd) {}
  void notempty() override { output(fd_); }
};

class infinibuf_mt : public infinibuf {
  mutex m_;
  condition_variable cv_;
public:
  explicit infinibuf_mt (int sp = default_startpos_) : infinibuf(sp) {}
  void lock() override { m_.lock(); }
  void unlock() override { m_.unlock(); }
  void notempty() override { cv_.notify_all(); }
  void gwait() override {
    if (empty() && !eof()) {
      unique_lock<mutex> ul (m_, adopt_lock);
      cv_.wait(ul);
      ul.release();
    }
  }
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

void
infinibuf::pbump(int n)
{
  assert (n >= 0);
  assert (n <= psize());
  assert (!eof_);
  bool wasempty (empty());
  ppos_ += n;
  if (ppos_ == chunksize_) {
    char *chunk = new char[chunksize_];
    memcpy(chunk, data_.back() + chunksize_ - startpos_, startpos_);
    data_.push_back(chunk);
    ppos_ = startpos_;
  }
  if (wasempty)
    notempty();
}

bool
infinibuf::output(int fd)
{
  unique_lock<infinibuf> lk (*this);
  for (;;) {
    char *p = gptr();
    size_t nmax = gsize();
    bool iseof = eof();
    int error = err();
    if (error)
      throw runtime_error (string("infinibuf::output: ") + strerror(error));
    else if (!nmax && iseof) {
      assert (empty());
      shutdown(fd, SHUT_WR);
      return false;
    }
    if (!nmax)
      return true;

    lk.unlock();
    ssize_t n = write(fd, p, nmax);
    lk.lock();

    if (n > 0)
      gbump(n);
    else {
      if (errno == EAGAIN)
	return true;
      err(error = errno);
    }
  }
}

bool
infinibuf::input (int fd)
{
  unique_lock<infinibuf> lk (*this);
  char *p = pptr();
  size_t nmax = psize();
  int error = err();
  if (error)
    throw runtime_error (string("infinibuf::input: ") + strerror(error));

  lk.unlock();
  ssize_t n = read(fd, p, nmax);
  lk.lock();

  if (n < 0) {
    if (errno == EAGAIN)
      return true;
    err(errno);
    throw runtime_error (string("infinibuf::input: ") + strerror(errno));
  }

  if (n > 0)
    pbump(n);
  else
    peof();
  return n > 0;
}

void
infinibuf::output_loop (shared_ptr<infinibuf> ib, int fd)
{
  while (ib->output(fd)) {
    lock_guard<infinibuf> _lk (*ib);
    ib->gwait();
  }
}

void
infinibuf::input_loop (shared_ptr<infinibuf> ib, int fd)
{
  while (ib->input(fd))
    ;
}

class infinistreambuf : public streambuf {
protected:
  shared_ptr<infinibuf> ib_;
  int_type underflow() override;
  int_type overflow(int_type ch) override;
  int sync() override;
public:
  explicit infinistreambuf (shared_ptr<infinibuf> ib);
  explicit infinistreambuf (infinibuf *ib)
    : infinistreambuf(shared_ptr<infinibuf>(ib)) {}
  shared_ptr<infinibuf> get_infinibuf() { return ib_; }
};

infinistreambuf::int_type
infinistreambuf::underflow()
{
  lock_guard<infinibuf> _lk (*ib_);
  ib_->gbump(gptr() - ib_->gptr());
  while (ib_->gsize() == 0 && !ib_->eof())
    ib_->gwait();
  setg(ib_->eback(), ib_->gptr(), ib_->egptr());
  bool eof = ib_->eof() && ib_->gsize() == 0;
  return eof ? traits_type::eof() : traits_type::to_int_type (*gptr());
}

infinistreambuf::int_type
infinistreambuf::overflow(int_type ch)
{
  if (sync() == -1)
    return traits_type::eof();
  *pptr() = ch;
  pbump(1);
  return traits_type::not_eof(ch);
}

int
infinistreambuf::sync()
{
  lock_guard<infinibuf> _lk (*ib_);
  ib_->pbump(pptr() - ib_->pptr());
  setp(ib_->pptr(), ib_->epptr());
  int err = ib_->err();
  return err ? -1 : 0;
}

infinistreambuf::infinistreambuf (shared_ptr<infinibuf> ib)
  : ib_(ib)
{
  lock_guard<infinibuf> _lk (*ib_);
  setg(ib_->eback(), ib_->gptr(), ib_->egptr());
  setp(ib_->pptr(), ib_->epptr());
}


#if 0
int
main (int argc, char **argv)
{
  infinistreambuf inb (new infinibuf_mt);
  istream xin (&inb);
  thread it (infinibuf::input_loop, inb.get_infinibuf(), 0);

  infinistreambuf outb (new infinibuf_mt);
  ostream xout (&outb);
  thread ot (infinibuf::output_loop, outb.get_infinibuf(), 1);
  xin.tie (&xout);

#if 0
  char c;
  long count = 0;
  while (xin.get (c)) {
    count++;
    xout.put (c);
  }
  cerr << "flushing " << count << " bytes\n";
  xout.flush();
#endif

  xout << xin.rdbuf() << flush;

  /*
  xout << "waiting for input\n";
  string x;
  xin >> x;
  xout << "got " << x << "\n" << flush;
  */
  
  auto oib = outb.get_infinibuf();
  oib->lock();
  oib->peof();
  oib->unlock();
  ot.join();

  it.join();

  return 0;
}
#endif

#if 0
int
main (int argc, char **argv)
{
  infinibuf_infd iib(0);
  infinistreambuf inb (&iib);
  istream xin (&inb);

  infinibuf_outfd oib(1);
  infinistreambuf outb (&oib);
  ostream xout (&outb);
  xin.tie(&xout);

  xout << xin.rdbuf();
#if 0
  long count = 0;
  char c;
  while (xin.get (c)) {
    xout.put (c);
    count++;
  }
  cerr << "Total count " << count << '\n';
#endif

  outb.pubsync();
  oib.peof();
}
#endif

/*

c++ -g -std=c++11 -Wall -Werror -pthread infinibuf.cc

*/
