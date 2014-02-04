// -*- C++ -*-

#include <cassert>
#include <condition_variable>
#include <exception>
#include <list>

struct Chan_eof : std::exception {
  const char *what() const noexcept override { return "EOF from Chan"; }
};

template<typename T> class Chan {
  std::list<T> data_;
  std::mutex m_;
  std::condition_variable cv_;
  bool eof_{false};
public:
  Chan() = default;
  Chan(const Chan&) = delete;
  template<class... Args> void write(Args&&... args) {
    std::lock_guard<std::mutex> _lk (m_);
    assert (!eof_);
    data_.emplace_back(std::forward<Args>(args)...);
  }
  void writeeof() {
    std::lock_guard<std::mutex> _lk (m_);
    assert (!eof_);
    eof_ = true;
  }
  T read() {
    std::unique_lock<std::mutex> lk (m_);
    while (data_.empty()) {
      if (eof_)
	throw Chan_eof();
      cv_.wait(lk);
    }
    T ret (std::move(data_.front()));
    data_.pop_front();
    return ret;
  }
};
