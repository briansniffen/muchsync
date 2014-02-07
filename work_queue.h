#include <condition_variable>
#include <forward_list>
#include <list>
#include <queue>
#include <thread>

class work_queue {
  bool eof_{false};
  std::mutex m_;
  std::condition_variable todo_;
  std::queue<std::function<void()>,std::list<std::function<void()>>> q_;
  std::forward_list<std::thread> helpers_;
  void work();
public:
  work_queue();
  work_queue(const work_queue &) = delete;
  ~work_queue();
  template<typename T> void enqueue (T &&t) {
    std::lock_guard<std::mutex> _lk (m_);
    q_.emplace(std::forward<T>(t));
    todo_.notify_one();
  }
};
