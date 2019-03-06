// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/macros.h"
#include "util/fibers/fibers_ext.h"

namespace util {
namespace fibers_ext {

class EventCount {
  using spinlock_lock_t = ::boost::fibers::detail::spinlock_lock;

 public:
  EventCount() noexcept : val_(0) {}

  class Key {
    friend class EventCount;
    explicit Key(uint32_t e) noexcept : epoch(e) {}

    uint32_t epoch;
  };

  // Return true if syscall notification was made, false if no call needed
  // (no threads were sleeping).
  bool notify() noexcept {
    uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);

    if (UNLIKELY(prev & kWaiterMask)) {
      // lk forces waiting threads to be before of the critical section in EventCount::wait or
      // suspended inside wait.
      // If they are suspended then notify_one will resume one thread, otherwise they will
      // check the epoch and skip waiting due to unsynced epoch number.
      spinlock_lock_t lk{splk_};
      cnd_.notify_one();
      return true;
    }
    return false;
  }

  bool notifyAll() noexcept {
    uint64_t prev = val_.fetch_add(kAddEpoch, std::memory_order_acq_rel);

    if (UNLIKELY(prev & kWaiterMask)) {
      // lk forces waiting threads to be before of the critical section in EventCount::wait or
      // suspended inside wait.
      // If they are suspended then notify_one will resume one thread, otherwise they will
      // check the epoch and skip waiting due to unsynced epoch number.
      spinlock_lock_t lk{splk_};
      cnd_.notify_all();
      return true;
    }
    return false;
  };

  Key prepareWait() noexcept {
    uint64_t prev = val_.fetch_add(kAddWaiter, std::memory_order_acq_rel);
    return Key(prev >> kEpochShift);
  }

  void cancelWait() noexcept {
    // memory_order_relaxed would suffice for correctness, but the faster
    // #waiters gets to 0, the less likely it is that we'll do spurious wakeups
    // (and thus system calls).
    val_.fetch_sub(kAddWaiter, std::memory_order_seq_cst);
  }

  void wait(Key key) noexcept;

  /**
   * Wait for condition() to become true.  Will clean up appropriately if
   * condition() throws, and then rethrow.
   */
  template <typename Condition> void await(Condition condition);

 private:
  bool doNotify(int n) noexcept;

  EventCount(const EventCount&) = delete;
  EventCount(EventCount&&) = delete;
  EventCount& operator=(const EventCount&) = delete;
  EventCount& operator=(EventCount&&) = delete;

  // This requires 64-bit
  static_assert(sizeof(int) == 4, "bad platform");
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

// Atomically checks for epoch
// and waits on cond_var.
inline void EventCount::wait(Key key) noexcept {
  spinlock_lock_t lk{splk_};
  while ((val_.load(std::memory_order_acquire) >> kEpochShift) == key.epoch) {
    cnd_.wait(lk);
  }
  lk.unlock();

  // memory_order_relaxed would suffice for correctness, but the faster
  // #waiters gets to 0, the less likely it is that we'll do spurious wakeups
  // (and thus system calls).
  val_.fetch_sub(kAddWaiter, std::memory_order_seq_cst);
}

template <typename Condition> void EventCount::await(Condition condition) {
  if (condition())
    return;  // fast path

  // condition() is the only thing that may throw, everything else is
  // noexcept, so we can hoist the try/catch block outside of the loop
  try {
    for (;;) {
      auto key = prepareWait();
      if (condition()) {
        cancelWait();
        break;
      } else {
        wait(key);
      }
    }
  } catch (...) {
    cancelWait();
    throw;
  }
}

}  // namespace fibers_ext
}  // namespace util
