// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/ProducerConsumerQueue.h"

#include <boost/fiber/context.hpp>

namespace util {
namespace fibers_ext {

/*
  This is single producer/single consumer thread-safe channel.
  It's fiber friendly, which means, multiple fibers at each end-point can use the channel:
  K fibers from producer thread can push and N fibers from consumer thread can pull the records.
  It has blocking interface that suspends blocked fibers upon empty/full conditions.
*/
template <typename T> class SimpleChannel {
  typedef ::boost::fibers::context::wait_queue_t wait_queue_t;
  using spinlock_lock_t = ::boost::fibers::detail::spinlock_lock;

 public:
  SimpleChannel(size_t n) : q_(n) {}

  template <typename... Args> void Push(Args&&... recordArgs);

  // Blocking call. Returns false if channel is closed, true otherwise with the popped value.
  bool Pop(T& dest);

  // Should be called only from the producer side. Signals the consumers that the channel
  // is closed. Consumers may still pop the existing items.
  // This function does not block only puts the channel into closing state.
  // It's responsibility of the caller to wait for the consumers to empty the remaining items
  // and stop using the channel.
  void StartClosing();
 private:
  folly::ProducerConsumerQueue<T> q_;
  wait_queue_type                                     waiting_producers_;
  wait_queue_type                                     waiting_consumers_;
};

}  // namespace fibers_ext
}  // namespace util
