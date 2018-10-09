// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "util/asio/io_context.h"

using namespace boost;
using namespace std;

namespace util {

namespace {

class AsioScheduler final : public fibers::algo::algorithm_with_properties<IoFiberProperties> {
 private:
  std::shared_ptr<asio::io_context> io_svc_;
  asio::steady_timer suspend_timer_;
  //]
  fibers::scheduler::ready_queue_type rqueue_arr_[IoFiberProperties::NUM_NICE_LEVELS];
  fibers::mutex mtx_;
  fibers::condition_variable cnd_;
  std::size_t counter_{0};

 public:
  //[asio_rr_ctor
  AsioScheduler(const std::shared_ptr<asio::io_context>& io_svc)
      : io_svc_(io_svc), suspend_timer_(*io_svc_) {
  }

  void awakened(fibers::context* ctx, IoFiberProperties& props) noexcept override {
    DCHECK(!ctx->ready_is_linked());

    unsigned nice = props.nice_level();
    DCHECK_LT(nice, IoFiberProperties::NUM_NICE_LEVELS);
    auto& q = rqueue_arr_[nice];

    ctx->ready_link(q); /*< fiber, enqueue on ready queue >*/
    if (!ctx->is_context(fibers::type::dispatcher_context)) {
      ++counter_;
    }
    // VLOG(1) << "awakened: " << ctx->get_id();
  }

  fibers::context* pick_next() noexcept override {
    fibers::context* ctx(nullptr);
    for (auto& q : rqueue_arr_) {
      if (q.empty())
        continue;

      // pop an item from the ready queue
      ctx = &q.front();
      q.pop_front();
      DCHECK(ctx && fibers::context::active() != ctx);
      if (!ctx->is_context(fibers::type::dispatcher_context)) {
        --counter_;
      }
      break;
    }

    //  VLOG_IF(1, ctx) << ctx->get_id();
    return ctx;
  }

  void property_change(boost::fibers::context* ctx, IoFiberProperties& props) noexcept final {
    // Although our priority_props class defines multiple properties, only
    // one of them (priority) actually calls notify() when changed. The
    // point of a property_change() override is to reshuffle the ready
    // queue according to the updated priority value.

    // 'ctx' might not be in our queue at all, if caller is changing the
    // priority of (say) the running fiber. If it's not there, no need to
    // move it: we'll handle it next time it hits awakened().
    if (!ctx->ready_is_linked()) {
      return;
    }

    // Found ctx: unlink it
    ctx->ready_unlink();

    // Here we know that ctx was in our ready queue, but we've unlinked
    // it. We happen to have a method that will (re-)add a context* to the
    // right place in the ready queue.
    awakened(ctx, props);
  }

  bool has_ready_fibers() const noexcept override {
    return 0 < counter_;
  }

  size_t active_fiber_count() const {
    return counter_;
  }

  void suspend_until(std::chrono::steady_clock::time_point const& abs_time) noexcept override {
    VLOG(2) << "suspend_until " << abs_time.time_since_epoch().count();

    // Set a timer so at least one handler will eventually fire, causing
    // run_one() to eventually return.
    if ((std::chrono::steady_clock::time_point::max)() != abs_time) {
      // Each expires_at(time_point) call cancels any previous pending
      // call. We could inadvertently spin like this:
      // dispatcher calls suspend_until() with earliest wake time
      // suspend_until() sets suspend_timer_,
      // loop() calls run_one()
      // some other asio handler runs before timer expires
      // run_one() returns to loop()
      // loop() yields to dispatcher
      // dispatcher finds no ready fibers
      // dispatcher calls suspend_until() with SAME wake time
      // suspend_until() sets suspend_timer_ to same time, canceling
      // previous async_wait()
      // loop() calls run_one()
      // asio calls suspend_timer_ handler with operation_aborted
      // run_one() returns to loop()... etc. etc.
      // So only actually set the timer when we're passed a DIFFERENT
      // abs_time value.
      suspend_timer_.expires_at(abs_time);
      suspend_timer_.async_wait([](system::error_code const&) { this_fiber::yield(); });
    }
    cnd_.notify_one();
  }
  //]

  void notify() noexcept override {
    // Something has happened that should wake one or more fibers BEFORE
    // suspend_timer_ expires. Reset the timer to cause it to fire
    // immediately, causing the run_one() call to return. In theory we
    // could use cancel() because we don't care whether suspend_timer_'s
    // handler is called with operation_aborted or success. However --
    // cancel() doesn't change the expiration time, and we use
    // suspend_timer_'s expiration time to decide whether it's already
    // set. If suspend_until() set some specific wake time, then notify()
    // canceled it, then suspend_until() was called again with the same
    // wake time, it would match suspend_timer_'s expiration time and we'd
    // refrain from setting the timer. So instead of simply calling
    // cancel(), reset the timer, which cancels the pending sleep AND sets
    // a new expiration time. This will cause us to spin the loop twice --
    // once for the operation_aborted handler, once for timer expiration
    // -- but that shouldn't be a big problem.
    suspend_timer_.async_wait([](system::error_code const&) { this_fiber::yield(); });
    suspend_timer_.expires_at(std::chrono::steady_clock::now());
  }

  void WaitTillFibersSuspend() {
    // block this fiber till all pending (ready) fibers are processed
    // == AsioScheduler::suspend_until() has been called.

    // TODO: We might want to limit the number of processed fibers per iterations
    // based on their priority and count to allow better responsiveness
    // for handling io_svc requests. For example, to process all HIGH, and then limit
    // all other fibers.
    std::unique_lock<fibers::mutex> lk(mtx_);
    cnd_.wait(lk);
  }

 private:
};

void MainLoop(asio::io_context* io_cntx, AsioScheduler* scheduler) {
  while (!io_cntx->stopped()) {
    if (scheduler->has_ready_fibers()) {
      while (io_cntx->poll())
        ;

      // Gives up control to allow other fibers to run in the thread.
      scheduler->WaitTillFibersSuspend();
    } else {
      // run one handler inside io_context
      // if no handler available, blocks this thread
      if (!io_cntx->run_one()) {
        break;
      }
    }
  }
  VLOG(1) << "MainLoop exited";
}

}  // namespace

void IoContext::StartLoop() {
  // I do not use use_scheduling_algorithm because I want to retain access to the scheduler.
  // fibers::use_scheduling_algorithm<AsioScheduler>(io_ptr);
  AsioScheduler* scheduler = new AsioScheduler(context_ptr_);
  fibers::context::active()->get_scheduler()->set_algo(scheduler);
  this_fiber::properties<IoFiberProperties>().set_name("io_loop");
  this_fiber::properties<IoFiberProperties>().set_nice_level(2);
  CHECK(fibers::context::active()->is_context(fibers::type::main_context));

  thread_id_ = this_thread::get_id();

  io_context& io_cntx = *context_ptr_;

  // We run the main loop inside the callback of io_context, blocking it until the loop exits.
  // The reason for this is that io_context::running_in_this_thread() is deduced based on the
  // call-stack. We might right our own utility function based on thread id since
  // we run single thread per io context.
  Post([scheduler, &io_cntx] { MainLoop(&io_cntx, scheduler); });

  // Bootstrap - launch the callback handler above.
  // It will block until MainLoop exits.
  io_cntx.run_one();

  // We stopped io_context. Lets spin it more until all ready handlers are run.
  // That should make sure that fibers are unblocked.
  io_cntx.restart();

  while (io_cntx.poll() || scheduler->has_ready_fibers()) {
    this_fiber::yield();  // while something happens, pass the ownership to other fiber.
  }
}

}  // namespace util
