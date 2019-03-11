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
  using mutex_t = ::boost::fibers::mutex;

 public:
  explicit Done(bool val = false) : ready_(val) {}

  ~Done() {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  // Wait on the ready_ flag.
  void Wait() {
    ec_.await([this] { return ready_.load(std::memory_order_acquire); });
  }

  void Notify();

  void Reset() {
    ready_ = false;
  }

  bool IsReady() const {
    return ready_.load(std::memory_order_acquire);
  }

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
    ec_.await([this] { return count_.load(std::memory_order_acquire) == 0; });
  }

  void Add(unsigned delta) {
    count_.fetch_add(delta, std::memory_order_acq_rel);
  }

 private:
  std::atomic_long count_;
  EventCount ec_;
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

// We use EventCount to wake threads without blocking.
inline void Done::Notify() {
  ready_.store(true, std::memory_order_release);
  ec_.notify();
}

inline void BlockingCounter::Dec() {
  auto prev = count_.fetch_sub(1, std::memory_order_acq_rel);
  if (prev == 1)
    ec_.notify();
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
