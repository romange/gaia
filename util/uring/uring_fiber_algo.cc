// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <sys/types.h>

#include "util/uring/uring_fiber_algo.h"

#include "base/logging.h"
#include "util/uring/proactor.h"

// TODO: We should replace DVLOG macros with RAW_VLOG if we do glog sync integration.

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
    DVLOG(2) << "Awakened dispatch";
  } else {
    DVLOG(2) << "Awakened " << props.name();

    ++ready_cnt_;  // increase the number of awakened/ready fibers.
  }

  ctx->ready_link(rqueue_); /*< fiber, enqueue on ready queue >*/
}

auto UringFiberAlgo::pick_next() noexcept -> FiberContext* {
  DVLOG(2) << "pick_next: " << ready_cnt_ << "/" << rqueue_.size();

  if (rqueue_.empty())
    return nullptr;

  FiberContext* ctx = &rqueue_.front();
  rqueue_.pop_front();

  if (!ctx->is_context(boost::fibers::type::dispatcher_context)) {
    --ready_cnt_;
    UringFiberProps* props = (UringFiberProps*)ctx->get_properties();
    DVLOG(1) << "Switching to " << props->name();  // TODO: to switch to RAW_LOG.
  } else {
    DVLOG(1) << "Switching to dispatch";  // TODO: to switch to RAW_LOG.
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
  DVLOG(1) << "notify from " << syscall(SYS_gettid);

  // We send yield so that 1. Main context would awake and 
  // 2. it would yield to dispatch context that will put active fibers into 
  // ready queue.
  proactor_->AsyncBrief([] { this_fiber::yield(); });
}

}  // namespace uring
}  // namespace util
