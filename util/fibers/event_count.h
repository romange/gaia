// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Based on the design of folly event_count which in turn based on
// Dmitry Vyukov's propasal at
// https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/299245
#pragma once

#include "base/macros.h"
#include "util/fibers/condition_variable.h"

namespace util {
namespace fibers_ext {

// This class is all about reducing the contention on the producer side (notifications).
// We want notifications to be as light as possible, while waits are less important
// since they on the path of being suspended anyway. However, we want to reduce number of
// spurious waits on the consumer side.
class EventCount {
  using spinlock_lock_t = ::boost::fibers::detail::spinlock_lock;

 public:
  EventCount() noexcept : val_(0) {}

  class Key {
    friend class EventCount;
    EventCount* me_;
    uint32_t epoch_;

    explicit Key(EventCount* me, uint32_t e) noexcept : me_(me), epoch_(e) {}

    Key(const Key&) = delete;

   public:
    Key(Key&&) noexcept = default;

    ~Key() {
      // memory_order_relaxed would suffice for correctness, but the faster
      // #waiters gets to 0, the less likely it is that we'll do spurious wakeups
      // (and thus system calls).
      me_->val_.fetch_sub(kAddWaiter, std::memory_order_seq_cst);
    }

    uint32_t epoch() const { return epoch_; }
  };

  // Return true if a notification was made, false if no notification was issued.
  bool notify() noexcept;

  bool notifyAll() noexcept;

  Key prepareWait() noexcept {
    uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
    return Key(this, prev >> kEpochShift);
  }

  void wait(uint32_t epoch) noexcept;

  /**
   * Wait for condition() to become true.  Will clean up appropriately if
   * condition() throws, and then rethrow.
   */
  template <typename Condition> void await(Condition condition);

 private:
  friend class Key;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // This requires 64-bit
  static_assert(sizeof(uint32_t) == 4, "bad platform");
  static_assert(sizeof(uint64_t) == 8, "bad platform");

  // val_ stores the epoch in the most significant 32 bits and the
  // waiter count in the least significant 32 bits.
  std::atomic<uint64_t> val_;

  ::boost::fibers::detail::spinlock splk_;
  condition_variable_any cnd_;

  static constexpr uint64_t kAddWaiter = uint64_t(1);

  static constexpr size_t kEpochShift = 32;
  static constexpr uint64_t kAddEpoch = uint64_t(1) << kEpochShift;
  static constexpr uint64_t kWaiterMask = kAddEpoch - 1;
};

inline bool EventCount::notify() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);

  if (UNLIKELY(prev & kWaiterMask)) {
    /*
    lk makes sure that when a waiting thread is entered the critical section in
    EventCount::wait, it atomically checks val_ when entering the WAIT state.
    We need it in order to make sure that cnd_.notify() is not called before the waiting
    thread enters WAIT state and thus the notification is missed.
    We could save this spinlock if we allow futex-like semantics in condition_variable_any
    or just implement what we need directly with fibers::context and its waitlist.
    See also https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/299245
    For more digging.
    */
    spinlock_lock_t lk{splk_};
    cnd_.notify_one();
    return true;
  }
  return false;
}

inline bool EventCount::notifyAll() noexcept {
  uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);

  if (UNLIKELY(prev & kWaiterMask)) {
    spinlock_lock_t lk{splk_};
    cnd_.notify_all();
    return true;
  }
  return false;
};

// Atomically checks for epoch and waits on cond_var.
inline void EventCount::wait(uint32_t epoch) noexcept {
  spinlock_lock_t lk{splk_};
  while ((val_.load(std::memory_order_acquire) >> kEpochShift) == epoch) {
    cnd_.wait(lk);
  }
}

template <typename Condition> void EventCount::await(Condition condition) {
  if (condition())
    return;  // fast path

  // condition() is the only thing that may throw, everything else is
  // noexcept, Key destructor makes sure to cancelWait state when exiting the function.
  while (true) {
    Key key = prepareWait();
    if (condition()) {
      break;
    }
    wait(key.epoch());
}
}

}  // namespace fibers_ext
}  // namespace util
