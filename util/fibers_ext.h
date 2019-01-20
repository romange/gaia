// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <experimental/optional>
#include <ostream>

namespace std {

inline ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op) {
  using ::boost::fibers::channel_op_status;
  if (op == channel_op_status::success) {
    o << "success";
  } else if (op == channel_op_status::closed) {
    o << "closed";
  } else if (op == channel_op_status::full) {
    o << "full";
  } else if (op == channel_op_status::empty) {
    o << "empty";
  } else if (op == channel_op_status::timeout) {
    o << "timeout";
  }
  return o;
}

}  // namespace std

namespace util {
namespace fibers_ext {

// Wrap canonical pattern for condition_variable + bool flag
class Done {
 public:
  explicit Done(bool val = false) : ready_(val)  {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  void Wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  void Notify() {
    // notify_one/all guarantee to unwake already waiting thread.
    // Therefore, to provide a consistent behavior on the wait side we should
    // update ready_ under mutex_.
    mutex_.lock();
    ready_ = true;
    mutex_.unlock();
    cond_.notify_one();
  }

  void Reset() {
    mutex_.lock();
    ready_ = false;
    mutex_.unlock();
  }

  bool IsReady() const { return ready_; }

 private:
  ::boost::fibers::condition_variable cond_;
  ::boost::fibers::mutex mutex_;
  bool ready_;
};

class BlockingCounter {
 public:
  using mutex = ::boost::fibers::mutex;

  explicit BlockingCounter(unsigned count) : count_(count) {
  }

  // Producer side -> should decrement the counter.
  void Dec() {
    if (0 == count_)  // should not happen
      return;

    std::lock_guard<mutex> g(mutex_);
    --count_;
    if (count_ == 0)
      cond_.notify_one();
  }

  void Wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this] { return count_ == 0; });
  }

  void Add(unsigned delta) {
    std::lock_guard<mutex> g(mutex_);
    count_ += delta;
  }
 private:
  unsigned count_;

  ::boost::fibers::condition_variable cond_;
  ::boost::fibers::mutex mutex_;
};

// For synchronizing fibers in single-threaded environment.
struct NoOpLock {
  void lock() {}
  void unlock() {}
};

template<typename Pred> void Await(::boost::fibers::condition_variable_any& cv, Pred&& pred) {
  NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

// Single threaded synchronization primitive between fibers.
// fibers::unbufferred_channel has bad design with respect to move semantics and
// "try_push" method because it will move the value even if it was not pushed.
// Therefore, for single producer, single consumer single threaded case we can use this
// Cell class for emulating unbufferred_channel.
template<typename T> class Cell {
  std::experimental::optional<T> val_;
  ::boost::fibers::condition_variable_any cv_;
 public:
  bool IsEmpty() const { return !bool(val_); }

  // Might block the calling fiber.
  void Emplace(T&& val) {
    fibers_ext::Await(cv_, [this] { return IsEmpty();});
    val_.emplace(std::forward<T>(val));
    cv_.notify_one();
  }

  void WaitTillFull() {
    fibers_ext::Await(cv_, [this] { return !IsEmpty();});
  }

  T& value() {
    return *val_;   // optional stays engaged.
  }

  void Clear() {
    val_ = std::experimental::nullopt;
    cv_.notify_one();
  }
};

}  // namespace fibers_ext

namespace detail {

template <typename R> class ResultMover {
  R r_;  // todo: to set as optional to support objects without default c'tor.
 public:
  template <typename Func> void Apply(Func&& f) { r_ = f(); }

  // Returning rvalue-reference means returning the same object r_ instead of creating a
  // temporary R{r_}. Please note that when we return function-local object, we do not need to
  // return rvalue because RVO eliminates redundant object creation.
  // But for returning data member r_ it's more efficient.
  // "get() &&" means you can call this function only on rvalue ResultMover&& object.
  R&& get() && { return std::forward<R>(r_); }
};

template <> class ResultMover<void> {
 public:
  template <typename Func> void Apply(Func&& f) { f(); }
  void get() {}
};

}  // namespace detail
}  // namespace util
