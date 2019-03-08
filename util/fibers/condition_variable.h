// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/condition_variable.hpp>

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

}  // namespace fibers_ext

}  // namespace util
