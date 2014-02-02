// -*- C++ -*-

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>



/** \file infinibuf.h
 *  \brief iostreams-friendly buffers that can grow without bounds
 */

/**
 * \brief Abstract buffer-management class for unbounded buffers.
 *
 * A derived class must at a minimum override either `notempty()` (for
 * output buffers) or `gwait()` (for input buffers).
 *
 * Most methods are not thread-safe.
 */
class infinibuf {
protected:
  static constexpr int default_startpos_ = 8;
  static constexpr int chunksize_ = 0x10000;

  std::deque<char *> data_;
  int gpos_;
  int ppos_;
  bool eof_{false};
  int errno_{0};
  const int startpos_;		// For putback

  /** Called to signal when the buffer transitions from empty to
   *  non-empty. */
  virtual void notempty() {}

public:
  explicit infinibuf(int sp = default_startpos_)
    : gpos_(sp), ppos_(sp), startpos_(sp) {
    data_.push_back (new char[chunksize_]);
  }
  infinibuf(const infinibuf &) = delete;
  virtual ~infinibuf() = 0;
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
  /** Called to wait for the buffer to be non-empty. */
  virtual void gwait() {}

  char *pbase() { return data_.back(); }
  char *pptr() { return pbase() + ppos_; }
  int psize() { return chunksize_ - gpos_; }
  char *epptr() { return pptr() + psize(); }
  void pbump(int n);
  void peof() { eof_ = true; if (empty()) notempty(); }

  // These functions are thread safe for some subtypes:

  /** By default `lock()` and `unlock()` do nothing, but threadsafe
   *  derived classes must override these functions. */
  virtual void lock() {}
  /** See comment at unlock. */
  virtual void unlock() {}

  /** Drain the current contents of the buffer.
   * \param fd The file descriptor to write to.
   * \return `false` at EOF if there is no point in ever calling
   * `output` again.
   * \throws runtime_error if the `write` system call fails and
   * `errno` is not `EAGAIN`. */
  bool output(int fd);

  /** Fill the buffer from a file descriptor.
   * \param fd The file descriptor to read from.
   * \return `false` at EOF if there is no point in ever calling
   * `output` again.
   * \throws runtime_error if the `read` system call fails and
   * `errno` is not `EAGAIN`. */
  bool input(int fd);

  static void output_loop(std::shared_ptr<infinibuf> ib, int fd);
  static void input_loop(std::shared_ptr<infinibuf> ib, int fd);
};

/** \brief An `infinibuf` that synchronously reads from a file
 *  descriptor when the buffer underflows.
 *
 *  Closes the file descriptor upon destruction. */
class infinibuf_infd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_infd (int fd, bool closeit = true,
			 int sp = default_startpos_)
    : infinibuf(sp), fd_(fd) {}
  ~infinibuf_infd();
  void gwait() override { input(fd_); }
};

/** \brief An `infinibuf` that synchronously writes to a file
 *  descriptor when the buffer overflows or is synced.
 *
 *  Closes the file descriptor upon destruction. */
class infinibuf_outfd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_outfd (int fd)
    : infinibuf(0), fd_(fd) {}
  ~infinibuf_outfd();
  void notempty() override { output(fd_); }
};

/** \brief Thread-safe infinibuf.
 *
 * This infinibuf can safely be used in an `iostream` by one thread,
 * while a different thread fills or drains the buffer (for instance
 * executing `infinibuf::output_loop` or `infinibuf::input_loop`).
 */
class infinibuf_mt : public infinibuf {
  std::mutex m_;
  std::condition_variable cv_;
public:
  explicit infinibuf_mt (int sp = default_startpos_) : infinibuf(sp) {}
  void lock() override { m_.lock(); }
  void unlock() override { m_.unlock(); }
  void notempty() override { cv_.notify_all(); }
  void gwait() override {
    if (empty() && !eof()) {
      std::unique_lock<std::mutex> ul (m_, std::adopt_lock);
      cv_.wait(ul);
      ul.release();
    }
  }
};

/** \brief `infinibuf`-based `streambuf`.
 *
 * This streambuf can make use of any buffer type derived from
 * `infinibuf`.  The `infinibuf` is always converted to a
 * `shared_ptr`, even if it is passed in as a raw `infinibuf*`.
 */
class infinistreambuf : public std::streambuf {
protected:
  std::shared_ptr<infinibuf> ib_;
  int_type underflow() override;
  int_type overflow(int_type ch) override;
  int sync() override;
public:
  explicit infinistreambuf (std::shared_ptr<infinibuf> ib);
  explicit infinistreambuf (infinibuf *ib)
    : infinistreambuf(std::shared_ptr<infinibuf>(ib)) {}
  std::shared_ptr<infinibuf> get_infinibuf() { return ib_; }
};
