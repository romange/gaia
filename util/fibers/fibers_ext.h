// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/channel_op_status.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <experimental/optional>
#include <ostream>

namespace std {

ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op);

}  // namespace std

namespace util {
namespace fibers_ext {

// Boost.Fibers has a data race bug, see https://github.com/boostorg/fiber/issues/194
// Until it's fixed we must reimplement condition_variable_any ourselves and also
// prohibit in our codebase from using Fibers version.
class condition_variable_any {
 private:
  typedef ::boost::fibers::context::wait_queue_t wait_queue_t;
  using spinlock_lock_t = ::boost::fibers::detail::spinlock_lock;

  ::boost::fibers::detail::spinlock wait_queue_splk_{};
  wait_queue_t wait_queue_{};

 public:
  condition_variable_any() = default;

  ~condition_variable_any() {}

  condition_variable_any(condition_variable_any const&) = delete;
  condition_variable_any& operator=(condition_variable_any const&) = delete;

  void notify_one() noexcept;

  void notify_all() noexcept;

  template <typename LockType> void wait(LockType& lt);

  template <typename LockType, typename Pred> void wait(LockType& lt, Pred pred) {
    while (!pred()) {
      wait(lt);
    }
  }

  template <typename LockType, typename Clock, typename Duration>
  ::boost::fibers::cv_status wait_until(
      LockType& lt, std::chrono::time_point<Clock, Duration> const& timeout_time_);

  template <typename LockType, typename Clock, typename Duration, typename Pred>
  bool wait_until(LockType& lt, std::chrono::time_point<Clock, Duration> const& timeout_time,
                  Pred pred) {
    while (!pred()) {
      if (::boost::fibers::cv_status::timeout == wait_until(lt, timeout_time)) {
        return pred();
      }
    }
    return true;
  }

  template <typename LockType, typename Rep, typename Period>
  ::boost::fibers::cv_status wait_for(LockType& lt,
                                      std::chrono::duration<Rep, Period> const& timeout_duration) {
    return wait_until(lt, std::chrono::steady_clock::now() + timeout_duration);
  }

  template <typename LockType, typename Rep, typename Period, typename Pred>
  bool wait_for(LockType& lt, std::chrono::duration<Rep, Period> const& timeout_duration,
                Pred pred) {
    return wait_until(lt, std::chrono::steady_clock::now() + timeout_duration, pred);
  }
};

// Wrap canonical pattern for condition_variable + bool flag
class Done {
 public:
  explicit Done(bool val = false) : ready_(val) {}
  Done(const Done&) = delete;

  void operator=(const Done&) = delete;

  void Wait() {
    std::unique_lock<::boost::fibers::mutex> lock(mutex_);
    cond_.wait(lock, [this]() { return ready_; });
  }

  void Notify();

  void Reset() {
    std::lock_guard<::boost::fibers::mutex> g(mutex_);
    ready_ = false;
  }

  bool IsReady() const { return ready_; }

 private:
  condition_variable_any cond_;
  ::boost::fibers::mutex mutex_;
  bool ready_;
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

template <typename LockType> void condition_variable_any::wait(LockType& lt) {
  using namespace ::boost;
  auto* active_ctx = fibers::context::active();

  // atomically call lt.unlock() and block on *this
  // store this fiber in waiting-queue
  spinlock_lock_t lk{wait_queue_splk_};

  active_ctx->wait_link(wait_queue_);
  active_ctx->twstatus.store(static_cast<std::intptr_t>(0), std::memory_order_release);
  // unlock external lt
  lt.unlock();

  // suspend this fiber
  active_ctx->suspend(lk);
  // relock external again before returning
  try {
    lt.lock();
  } catch (...) {
    std::terminate();
  }
}

template <typename LockType, typename Clock, typename Duration>
::boost::fibers::cv_status condition_variable_any::wait_until(
    LockType& lt, std::chrono::time_point<Clock, Duration> const& timeout_time_) {
  using namespace ::boost;
  auto* active_ctx = fibers::context::active();
  fibers::cv_status status = fibers::cv_status::no_timeout;
  std::chrono::steady_clock::time_point timeout_time = fibers::detail::convert(timeout_time_);

  // atomically call lt.unlock() and block on *this
  // store this fiber in waiting-queue
  spinlock_lock_t lk{wait_queue_splk_};

  active_ctx->wait_link(wait_queue_);
  active_ctx->twstatus.store(reinterpret_cast<std::intptr_t>(this), std::memory_order_release);

  // unlock external lt
  lt.unlock();
  // suspend this fiber
  if (!active_ctx->wait_until(timeout_time, lk)) {
    status = fibers::cv_status::timeout;
    // relock local lk
    lk.lock();
    // remove from waiting-queue
    wait_queue_.remove(*active_ctx);
    // unlock local lk
    lk.unlock();
  }
  // relock external again before returning
  try {
    lt.lock();
  } catch (...) {
    std::terminate();
  }
  // post-conditions
  return status;
}

inline void Done::Notify() {
  // notify_one/all guarantee to unwake already waiting thread.
  // Therefore, to provide a consistent behavior on the wait side we should
  // update ready_ under mutex_.
  mutex_.lock();
  ready_ = true;
  mutex_.unlock();
  cond_.notify_one();
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
