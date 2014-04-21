// -*- C++ -*-

/** \file cleanup.h
 *  \brief Classes to facilitate use of RIAA cleanup.
 */

#ifndef _CLEANUP_H_
#define _CLEANUP_H_ 1

#include <functional>

inline std::function<void()> &&
voidify(std::function<void()> &&f) {
  return move(f);
}
inline const std::function<void()> &
voidify(const std::function<void()> &f) {
  return f;
}
template<typename F> inline std::function<void()>
voidify(F &&f)
{
  return [f]() { f(); };
}


/** \brief Container for a cleanup action.
 */
class cleanup {
  std::function<void()> action_;
  static void nop() {}
public:
  cleanup() : action_ (nop) {}
  cleanup(const cleanup &) = delete;
  cleanup(cleanup &&c) : action_(c.action_) { c.release(); }
  template<typename F> cleanup(F &&f) : action_ (std::forward<F>(f)) {}
  template<typename... Args> cleanup(Args... args)
    : action_(voidify(std::bind(args...))) {}
  ~cleanup() { action_(); }
  cleanup &operator=(cleanup &&c) { action_.swap(c.action_); return *this; }
  void reset() {
    std::function<void()> old (action_);
    release();
    old();
  }
  template<typename F> void reset(F &&f) {
    std::function<void()> old (action_);
    action_ = std::forward<F>(f);
    old();
  }
  template<typename... Args> void reset(Args... args) {
    std::function<void()> old (action_);
    action_ = std::bind(args...);
    old();
  }
  void release() { action_ = nop; }
};

/** \brief Like a \ref std::unique_ptr, but half the size because the
 *  cleanup function is specified as part of the type.
 */
template<typename T, void(destructor)(T*)>
class unique_obj {
  T *obj_;
public:
  unique_obj() noexcept : obj_(nullptr) {}
  explicit unique_obj(T *obj) noexcept : obj_(obj) {}
  unique_obj(unique_obj &&uo) noexcept : obj_(uo.obj_) { uo.obj_ = nullptr; }
  ~unique_obj() { if (obj_) destructor(obj_); }
  void reset(T *obj) { T *old = obj_; obj_ = obj; destructor(old); }
  T *release() noexcept { T *old = obj_; obj_ = nullptr; return old; }
  T *get() const noexcept { return obj_; }
  T *&get() noexcept { return obj_; }
  operator T*() const noexcept { return obj_; }
};

#endif /* !_CLEANUP_H_ */
