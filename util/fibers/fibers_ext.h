// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <experimental/optional>
#include <ostream>

#include "util/fibers/condition_variable.h"
#include "util/fibers/event_count.h"

namespace std {

ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op);

}  // namespace std

namespace util {
namespace fibers_ext {

// Wrap canonical pattern for condition_variable + bool flag
class Done {
 public:
  explicit Done(bool val = false) : ready_(val) {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  void Wait() {
    ec_.await([this] { return ready_.load(std::memory_order_acquire); });
  }

  void Notify();

  void Reset() {
    ready_.store(false, std::memory_order_release);
  }

  bool IsReady() const { return ready_.load(std::memory_order_acquire); }

 private:
  EventCount ec_;
  std::atomic_bool ready_;
};

class BlockingCounter {
 public:
  using mutex = ::boost::fibers::mutex;

  explicit BlockingCounter(unsigned count) : count_(count) {}

  // Producer side -> should decrement the counter.
  void Dec();

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

  condition_variable_any cond_;
  ::boost::fibers::mutex mutex_;
};

class Semaphore {
 public:
  Semaphore(uint32_t cnt) : count_(cnt) {}

  void Wait(uint32_t nr = 1) {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    Wait(lock, nr);
  }

  void Signal(uint32_t nr = 1) {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    count_ += nr;
    lock.unlock();

    cond_.notify_all();
  }

  template <typename Lock> void Wait(Lock& l, uint32_t nr = 1) {
    cond_.wait(l, [&] { return count_ >= nr; });
    count_ -= nr;
  }

 private:
  condition_variable_any cond_;
  ::boost::fibers::mutex mutex_;
  uint32_t count_;
};

// For synchronizing fibers in single-threaded environment.
struct NoOpLock {
  void lock() {}
  void unlock() {}
};

template <typename Pred> void Await(condition_variable_any& cv, Pred&& pred) {
  NoOpLock lock;
  cv.wait(lock, std::forward<Pred>(pred));
}

// Single threaded synchronization primitive between fibers.
// fibers::unbufferred_channel has problematic design? with respect to move semantics and
// "try_push" method because it will move the value even if it was not pushed.
// Therefore, for single producer, single consumer single threaded case we can use this
// Cell class for emulating unbufferred_channel.
template <typename T> class Cell {
  std::experimental::optional<T> val_;
  condition_variable_any cv_;

 public:
  bool IsEmpty() const { return !bool(val_); }

  // Might block the calling fiber.
  void Emplace(T&& val) {
    fibers_ext::Await(cv_, [this] { return IsEmpty(); });
    val_.emplace(std::forward<T>(val));
    cv_.notify_one();
  }

  void WaitTillFull() {
    fibers_ext::Await(cv_, [this] { return !IsEmpty(); });
  }

  T& value() {
    return *val_;  // optional stays engaged.
  }

  void Clear() {
    val_ = std::experimental::nullopt;
    cv_.notify_one();
  }
};


inline void Done::Notify() {
  // notify_one/all guarantee to unwake already waiting thread.
  // Therefore, to provide a consistent behavior on the wait side we should
  // update ready_ under mutex_.
  ready_.store(true, std::memory_order_release);
  ec_.notify();
}

inline void BlockingCounter::Dec() {
  if (0 == count_)  // should not happen
    return;

  std::lock_guard<mutex> g(mutex_);
  --count_;
  if (count_ == 0)
    cond_.notify_one();
}

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
