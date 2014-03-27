// -*- C++ -*-

/** \file cleanup.h
 *  \brief Classes to facilitate use of RIAA cleanup.
 */

#ifndef _CLEANUP_H_
#define _CLEANUP_H_ 1

#include <functional>

/** \brief Container for a cleanup action.
 */
class cleanup {
  std::function<void()> action_;
public:
  cleanup() : action_ ([] () {}) {}
  cleanup(const cleanup &) = delete;
  cleanup(cleanup &&c) : action_(c.action_) { c.release(); }
  template<typename... Args> cleanup (Args... args)
    : action_(std::bind(args...)) {}
  ~cleanup() { action_(); }
  void reset() {
    std::function<void()> old (action_);
    release();
    old();
  }
  template<typename... Args> void reset(Args... args) {
    std::function<void()> old (action_);
    action_ = std::bind(args...);
    old();
  }
  void release() { action_ = [] () {}; }
};

/** \brief Like a \ref std::unique_ptr, but half the size because the
 *  cleanup function is specified as part of the type.
 */
template<class T, void(destructor)(T*)>
class unique_obj {
  T *obj_;
public:
  explicit unique_obj(T *obj) noexcept : obj_(obj) {}
  unique_obj(unique_obj &&uo) noexcept : obj_(uo.obj_) { uo.obj_ = nullptr; }
  ~unique_obj() { if (obj_) destructor(obj_); }
  void reset(T *obj) { T *old = obj_; obj_ = obj; destructor(old); }
  T *release() noexcept { T *old = obj_; obj_ = nullptr; return old; }
  T *get() const noexcept { return obj_; }
  operator T*() const noexcept { return obj_; }
};

#endif /* !_CLEANUP_H_ */
