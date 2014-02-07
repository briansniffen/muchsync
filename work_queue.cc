
#include "work_queue.h"

#include <unistd.h>

using namespace std;

work_queue::work_queue()
{
  lock_guard<mutex> _lk (m_);
  for(unsigned i = max(thread::hardware_concurrency(), 2U); i; --i)
    helpers_.emplace_front(mem_fun(&work_queue::work), this);
}

work_queue::~work_queue()
{
  m_.lock();
  eof_ = true;
  todo_.notify_all();
  m_.unlock();

  while (!helpers_.empty()) {
    helpers_.front().join();
    helpers_.pop_front();
  }
}

void
work_queue::work()
{
  unique_lock<mutex> lk (m_);
  for (;;) {
    while (q_.empty())
      if (eof_)
	return;
      else
	todo_.wait(lk);

    auto f (move (q_.front()));
    q_.pop();
    lk.unlock();
    f();
    lk.lock();
  }
}
