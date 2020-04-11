// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/uring_fiber_algo.h"

#include "base/logging.h"
#include "util/uring/proactor.h"

namespace util {
namespace uring {
using namespace boost;

UringFiberAlgo::UringFiberAlgo(Proactor* proactor) : proactor_(proactor) {
  main_cntx_ = fibers::context::active();
  CHECK(main_cntx_->is_context(fibers::type::main_context));
}

UringFiberAlgo::~UringFiberAlgo() {
}

void UringFiberAlgo::awakened(FiberContext* ctx, UringFiberProps& props) noexcept {
  DCHECK(!ctx->ready_is_linked());

  if (ctx->is_context(fibers::type::dispatcher_context)) {
  } else {
    ++ready_cnt_;  // increase the number of awakened/ready fibers.
  }

  ctx->ready_link(rqueue_); /*< fiber, enqueue on ready queue >*/
}

auto UringFiberAlgo::pick_next() noexcept -> FiberContext* {
  if (rqueue_.empty())
    return nullptr;
  FiberContext* ctx = &rqueue_.front();
  rqueue_.pop_front();

  if (!ctx->is_context(boost::fibers::type::dispatcher_context)) {
    --ready_cnt_;
  }
  return ctx;
}

void UringFiberAlgo::property_change(FiberContext* ctx, UringFiberProps& props) noexcept {
  if (!ctx->ready_is_linked()) {
    return;
  }

  // Found ctx: unlink it
  ctx->ready_unlink();
  if (!ctx->is_context(fibers::type::dispatcher_context)) {
    --ready_cnt_;
  }

  // Here we know that ctx was in our ready queue, but we've unlinked
  // it. We happen to have a method that will (re-)add a context* to the
  // right place in the ready queue.
  awakened(ctx, props);
}

bool UringFiberAlgo::has_ready_fibers() const noexcept {
  return ready_cnt_ > 0;
}

// suspend_until halts the thread in case there are no active fibers to run on it.
// This is done by dispatcher fiber.
void UringFiberAlgo::suspend_until(time_point const& abs_time) noexcept {
  auto* cur_cntx = fibers::context::active();

  DCHECK(cur_cntx->is_context(fibers::type::dispatcher_context));
  if (time_point::max() != abs_time) {
    LOG(FATAL) << "TBD";
  }
  main_cntx_->get_scheduler()->schedule(main_cntx_);
}

// This function is called from remote threads, to wake this thread in case it's sleeping.
// In our case, "sleeping" means - might stuck the wait function waiting for completion events.
// wait_for_cqe is the only place where the thread can be stalled.
void UringFiberAlgo::notify() noexcept {
  proactor_->WakeupIfNeeded();  // Trigger the loop in case we are stuck in wait_for_cqe.
}

}  // namespace uring
}  // namespace util
