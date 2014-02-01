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

#include <thread>

using namespace std;

class infinibuf {
  static constexpr int chunksize_ = 0x10000;

  deque<char *> data_;
  bool eof_{false};
  const int startpos_;		// For putback
  int gpos_;
  int ppos_;

public:
  explicit infinibuf(int sp = 8)
    : startpos_(sp), gpos_(startpos_), ppos_(startpos_) {
    data_.push_back (new char[chunksize_]);
  }
  infinibuf(const infinibuf &) = delete;
  ~infinibuf() { for (char *p : data_) delete[] p; }
  infinibuf &operator= (const infinibuf &) = delete;
		   
  bool empty() { return data_.size() == 1 && gpos_ == ppos_; }
  bool eof() { return eof_; }

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

#if 0
  // These return false if the infinibuf has been drained and peof() called
  bool gwait();
  bool gparms(char **bufp, size_t *np);
  bool gparms(char **gbegp, char **gcurrp, char **gendp);
  void gsetp(char *p);
  bool output(int fd);

  void pparms(char **bufp, size_t *np);
  void pparms(char **pbegp, char **pendp);
  void psetp(char *p);
  bool input(int fd);		// returns false if fd returns EOF
#endif
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


#if 0
bool
infinibuf::gwait()
{
  unique_lock<mutex> lock (lock_);
  while (empty_() && !eof_)
    nonempty_.wait(lock);
  return !eof_;
}

bool
infinibuf::gparms(char **bufp, size_t *np)
{
  lock_guard<mutex> _lock (lock_);
  *bufp = gbufptr();
  *np = gsize();
  return !eof_ || !empty_();
}

bool
infinibuf::gparms(char **gbegp, char **gcurrp, char **gendp)
{
  lock_guard<mutex> _lock (lock_);
  *gbegp = data_.front()->data();
  *gcurrp = gbufptr();
  *gendp = gbufptr() + gsize();
  return !eof_ || !empty_();
}

void
infinibuf::gsetp(char *p)
{
  lock_guard<mutex> _lock (lock_);
  gpos_ = p - data_.front()->data();
  assert (gpos_ > 0 && gpos_ <= chunksize_);
  if (gpos_ == chunksize_) {
    assert (data_.size() > 1);
    delete data_.front();
    data_.pop_front();
    gpos_ = startpos_;
  }
}

bool
infinibuf::output(int fd)
{
  for (;;) {
    char *p;
    size_t nmax;
    bool alive = gparms(&p, &nmax);
    if (!nmax)
      return alive;
    ssize_t n = write(fd, p, nmax);
    if (n > 0)
      gbump(n);
    else if (errno == EAGAIN)
      return true;
    else
      throw runtime_error (string("infinibuf::output: ") + strerror(errno));
  }
}

void
infinibuf::pparms(char **bufp, size_t *np)
{
  lock_guard<mutex> _lock (lock_);
  *bufp = pbufptr();
  *np = psize();
}

void
infinibuf::pparms(char **pbegp, char **pendp)
{
  lock_guard<mutex> _lock (lock_);
  char *pbeg = pbufptr();
  *pbegp = pbeg;
  *pendp = pbeg + psize();
}

void
infinibuf::psetp(char *p)
{
  lock_guard<mutex> _lock (lock_);
  bool wasempty (empty_());
  assert (p >= data_.back()->data() && p <= data_.back()->data() + chunksize_);
  ppos_ = p - data_.back()->data();
  if (ppos_ == chunksize_) {
    buf_type *chunk = new buf_type;
    memcpy(chunk->data(), data_.back()->data() + chunksize_ - startpos_,
	   startpos_);
    data_.push_back(chunk);
    ppos_ = startpos_;
  }
  if (wasempty)
    nonempty_.notify_all();
}

void
infinibuf::peof()
{
  unique_lock<mutex> lock (lock_);
  eof_ = true;
  nonempty_.notify_all();
}

bool
infinibuf::input(int fd)
{
  char *p;
  size_t nmax;
  pparms(&p, &nmax);
  ssize_t n = read (fd, p, nmax);
  if (n > 0) {
    pbump(n);
    return true;
  }
  if (n == 0) {
    peof();
    return false;
  }
  if (errno == EAGAIN)
    return true;
  peof();
  throw runtime_error (string("infinibuf::input: ") + strerror(errno));
}
#endif

#if 0
class chanbuf : public streambuf {
protected:
  shared_ptr<chan> c_;
  int_type underflow() override;
  int_type overflow(int_type ch) override;
  int sync() override;
public:
  explicit chanbuf (shared_ptr<chan> c);
  chanbuf() : chanbuf(shared_ptr<chan>(new chan)) {}
  shared_ptr<chan> getchan() { return c_; }
};

chanbuf::int_type
chanbuf::underflow()
{
  c_->gsetp(gptr());
  char *begp, *currp, *endp;
  bool alive;
  do {
    alive = c_->gparms(&begp, &currp, &endp);
  } while (alive && currp == endp && (alive = c_->gwait()));
  setg(begp, currp, endp);
  return alive ? traits_type::to_int_type (*currp) : traits_type::eof();
}

chanbuf::int_type
chanbuf::overflow(int_type ch)
{
  sync();
  *pptr() = ch;
  pbump(1);
  return traits_type::not_eof(ch);
}

int
chanbuf::sync()
{
  c_->psetp(pptr());
  char *begp, *endp;
  c_->pparms(&begp, &endp);
  setp(begp, endp);
  return 0;
}

chanbuf::chanbuf (shared_ptr<chan> c)
  : c_ (c)
{
  char *begp, *currp, *endp;
  c_->gparms(&begp, &currp, &endp);
  setg(begp, currp, endp);
  c_->pparms(&begp, &endp);
  setp(begp, endp);
}




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
  return 0;
}

/*

c++ -std=c++11 -Wall -Werror -pthread chanbuf.cc

*/
