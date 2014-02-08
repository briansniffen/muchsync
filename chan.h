// -*- C++ -*-

#include <cassert>
#include <condition_variable>
#include <exception>
#include <forward_list>
#include <list>
#include <queue>
#include <thread>

struct ChanEOF : std::exception {
  const char *what() const noexcept override { return "EOF from Chan"; }
};

template<typename T> class Chan {
  std::queue<T,std::list<T>> data_;
  std::mutex m_;
  std::condition_variable cv_;
  bool eof_{false};
public:
  Chan() = default;
  Chan(const Chan&) = delete;
  template<class... Args> void write(Args&&... args) {
    std::lock_guard<std::mutex> _lk (m_);
    assert (!eof_);
    data_.emplace(std::forward<Args>(args)...);
    cv_.notify_one();
  }
  void writeeof() {
    std::lock_guard<std::mutex> _lk (m_);
    eof_ = true;
    cv_.notify_all();
  }
  bool eof() {
    std::lock_guard<std::mutex> _lk (m_);
    return eof_ && data_.empty();
  }
  T read() {
    std::unique_lock<std::mutex> lk (m_);
    while (data_.empty()) {
      if (eof_)
	throw ChanEOF();
      cv_.wait(lk);
    }
    T ret (std::move(data_.front()));
    data_.pop();
    return ret;
  }
};

template<typename T> class WorkChan : protected Chan<T> {
  std::forward_list<std::thread> workers_;
  void work(std::function<void(T)> f) {
    try {
      for (;;)
	f(this->read());
    } catch (ChanEOF) {}
  }
public:
  using Chan<T>::write;
  WorkChan(std::function<std::function<void(T)>()> mkWorker) {
    for(unsigned i = std::max(std::thread::hardware_concurrency(), 2U); i; --i)
      workers_.emplace_front(mem_fun(&WorkChan::work), this, mkWorker());
  }
  ~WorkChan() {
    this->writeeof();
    while (!workers_.empty()) {
      workers_.front().join();
      workers_.pop_front();
    }
  }
};
