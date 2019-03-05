// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/fibers_ext.h"

namespace std {

ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op) {
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

using namespace boost;

// Not sure why we need compare_exchange_strong only in case of timed-wait but
// I am not gonna deep dive now.
inline bool should_switch(fibers::context* ctx, std::intptr_t expected) {
  return ctx->twstatus.compare_exchange_strong(expected, static_cast<std::intptr_t>(-1),
                                               std::memory_order_acq_rel) ||
         expected == 0;
}

void condition_variable_any::notify_one() noexcept {
  auto* active_ctx = fibers::context::active();

  // get one context' from wait-queue
  spinlock_lock_t lk{wait_queue_splk_};

  while (!wait_queue_.empty()) {
    auto* ctx = &wait_queue_.front();
    wait_queue_.pop_front();
    if (should_switch(ctx, reinterpret_cast<std::intptr_t>(this))) {
      // notify context
      lk.unlock();
      active_ctx->schedule(ctx);
      break;
    }
  }
}

void condition_variable_any::notify_all() noexcept {
  auto* active_ctx = fibers::context::active();

  // get all context' from wait-queue
  spinlock_lock_t lk{wait_queue_splk_};

  // notify all context'
  while (!wait_queue_.empty()) {
    auto* ctx = &wait_queue_.front();
    wait_queue_.pop_front();

    if (wait_queue_.empty()) {
      lk.unlock();
    }

    if (should_switch(ctx, reinterpret_cast<std::intptr_t>(this))) {
      // notify context
      active_ctx->schedule(ctx);
    }
  }
}

}  // namespace fibers_ext
}  // namespace util
