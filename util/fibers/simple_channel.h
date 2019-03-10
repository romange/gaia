// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/ProducerConsumerQueue.h"

#include <boost/fiber/context.hpp>

#include "util/fibers/event_count.h"
#include "util/fibers/fibers_ext.h"

namespace util {
namespace fibers_ext {

/*
  This is single producer - single consumer thread-safe channel.
  It's fiber friendly, which means, multiple fibers at each end-point can use the channel:
  K fibers from producer thread can push and N fibers from consumer thread can pull the records.
  It has blocking interface that suspends blocked fibers upon empty/full conditions.
  It also can be used for single-thread/multiple-fibers scenario.
  This class designed to be pretty efficient by reducing the contention on its synchronization
  primitives to minimum.
*/
template <typename T> class SimpleChannel {
  typedef ::boost::fibers::context::wait_queue_t wait_queue_t;
  using spinlock_lock_t = ::boost::fibers::detail::spinlock_lock;

 public:
  SimpleChannel(size_t n) : q_(n) {}

  template <typename... Args> void Push(Args&&... recordArgs) noexcept;

  // Blocking call. Returns false if channel is closed, true otherwise with the popped value.
  bool Pop(T& dest);

  // Should be called only from the producer side. Signals the consumers that the channel
  // is going to be close. Consumers may still pop the existing items until Pop() return false.

  // This function does not block, only puts the channel into closing state.
  // It's responsibility of the caller to wait for the consumers to empty the remaining items
  // and stop using the channel.
  void StartClosing();

  // Non blocking
  template <typename... Args> bool TryPush(Args&&... args) noexcept {
    if (q_.write(std::forward<Args>(args)...)) {
      if (++quiet_pushes_ > q_.capacity() / 3) {
        pop_ec_.notify();
        quiet_pushes_ = 0;
      }
      return true;
    }
    pop_ec_.notify();
    quiet_pushes_ = 0;
    return false;
  }

  bool TryPop(T& val) {
    if (q_.read(val)) {
      return true;
    }
    push_ec_.notify();
    return false;
  }

 private:
  unsigned quiet_pushes_ = 0;

  folly::ProducerConsumerQueue<T> q_;
  std::atomic_bool is_closing_{false};

  // Event counts provide almost negligible contention during fast-path (a single atomic add).
  EventCount push_ec_, pop_ec_;
};

template <typename T>
template <typename... Args>
void SimpleChannel<T>::Push(Args&&... args) noexcept {
  while (true) {
    EventCount::Key key = push_ec_.prepareWait();
    if (TryPush(std::forward<Args>(args)...)) {
      break;
    }
    push_ec_.wait(key.epoch());
  }
}

template <typename T> bool SimpleChannel<T>::Pop(T& dest) {
  if (TryPop(dest))
    return true;

  while (true) {
    EventCount::Key key = pop_ec_.prepareWait();
    if (TryPop(dest)) {
      return true;
    }

    if (is_closing_.load(std::memory_order_acquire)) {
      return false;
    }

    pop_ec_.wait(key.epoch());
  }
}

template <typename T> void SimpleChannel<T>::StartClosing() {
  // Full barrier, StartClosing performance does not matter.
  is_closing_.store(true, std::memory_order_seq_cst);
  pop_ec_.notifyAll();
}

}  // namespace fibers_ext
}  // namespace util
