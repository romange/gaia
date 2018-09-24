// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/io_context_pool.h"

#include <condition_variable>

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "base/pthread_utils.h"

using namespace boost;

namespace util {
namespace {

// TODO: to add priorities.
// See https://www.boost.org/doc/libs/1_68_0/libs/fiber/doc/html/fiber/custom.html#custom
// We need it in order to prioritize client rpc read path, loop() fibers - higher
// and connection acceptor fiber lower
// than normal operations. For this we need 3 predefined priorities: HIGH,NORMAL, NICED.
class AsioScheduler final : public fibers::algo::algorithm {
 private:
  std::shared_ptr<asio::io_context> io_svc_;
  asio::steady_timer suspend_timer_;
  //]
  fibers::scheduler::ready_queue_type rqueue_;
  fibers::mutex mtx_;
  fibers::condition_variable cnd_;
  std::size_t counter_{0};

 public:
  //[asio_rr_ctor
  AsioScheduler(const std::shared_ptr<asio::io_context>& io_svc)
      : io_svc_(io_svc), suspend_timer_(*io_svc_) {
  }

  void awakened(fibers::context* ctx) noexcept override {
    DCHECK(!DCHECK_NOTNULL(ctx)->ready_is_linked());
    ctx->ready_link(rqueue_); /*< fiber, enqueue on ready queue >*/
    if (!ctx->is_context(fibers::type::dispatcher_context)) {
      ++counter_;
    }
    // VLOG(1) << "awakened: " << ctx->get_id();
  }

  fibers::context* pick_next() noexcept override {
    fibers::context* ctx(nullptr);
    if (!rqueue_.empty()) {
      // pop an item from the ready queue
      ctx = &rqueue_.front();
      rqueue_.pop_front();
      DCHECK(ctx && fibers::context::active() != ctx);
      if (!ctx->is_context(fibers::type::dispatcher_context)) {
        --counter_;
      }
    }
    //  VLOG_IF(1, ctx) << ctx->get_id();
    return ctx;
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
      while (io_cntx->poll());

      scheduler->WaitTillFibersSuspend();
    } else {
      // run one handler inside io_context
      // if no handler available, block this thread
      if (!io_cntx->run_one()) {
        break;
      }
    }
  }
  VLOG(1) << "MainLoop exited";
}

}  // namespace

thread_local size_t IoContextPool::context_indx_ = 0;

IoContextPool::IoContextPool(std::size_t pool_size) {
  if (pool_size == 0)
    pool_size = std::thread::hardware_concurrency();
  context_arr_.resize(pool_size);
  thread_arr_.resize(pool_size);

  for (size_t i = 0; i < pool_size; ++i) {
    context_arr_[i] = std::make_shared<asio::io_context>();
  }
}

IoContextPool::~IoContextPool() {
  Stop();
}

void IoContextPool::ContextLoop(size_t index) {
  context_indx_ = index;

  auto& io_ptr = context_arr_[index];

  // I do not use use_scheduling_algorithm because I want to retain access to the scheduler.
  // fibers::use_scheduling_algorithm<AsioScheduler>(io_ptr);
  AsioScheduler* scheduler = new AsioScheduler(io_ptr);
  fibers::context::active()->get_scheduler()->set_algo(scheduler);

  VLOG(1) << "Started io thread " << index << " " << io_ptr.get();

  // We run the main loop inside the callback of io_context, blocking it until the loop exits.
  // The reason for this is that io_context::running_in_this_thread() is deduced based on the
  // call-stack. We might right our own utility function based on thread id since
  // we run single thread per io context.
  io_ptr->post([scheduler, ptr = io_ptr.get()] {
    MainLoop(ptr, scheduler);
  });

  // Bootstrap - launch the callback handler above.
  // It will block until MainLoop exits.
  io_ptr->run_one();

  // We stopped io_context. Lets spin it more until all ready handlers are run.
  // That should make sure that fibers are unblocked.
  io_ptr->restart();

  while (io_ptr->poll() || scheduler->has_ready_fibers()) {
    this_fiber::yield();  // while something happens, pass the ownership to other fiber.
  }

  VLOG(1) << "Finished io thread " << index;
}

void IoContextPool::Run() {
  CHECK_EQ(STOPPED, state_);

  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    thread_arr_[i].work.emplace(asio::make_work_guard(*context_arr_[i]));
    thread_arr_[i].tid = base::StartThread("IoPool", [this, i]() {
      ContextLoop(i);
    });
  }
  LOG(INFO) << "Running " << thread_arr_.size() << " io threads";
  state_ = RUN;
}

void IoContextPool::Stop() {
  if (state_ == STOPPED)
    return;

  for (size_t i = 0; i < context_arr_.size(); ++i) {
    context_arr_[i]->stop();
  }

  for (TInfo& tinfo : thread_arr_) {
    tinfo.work->reset();
  }

  for (TInfo& tinfo : thread_arr_) {
    pthread_join(tinfo.tid, nullptr);
  }
  state_ = STOPPED;
}

asio::io_context& IoContextPool::GetNextContext() {
  // Use a round-robin scheme to choose the next io_context to use.
  DCHECK_LT(next_io_context_, context_arr_.size());
  uint32_t index = next_io_context_.load();
  boost::asio::io_context& io_context = *context_arr_[index++];

  // Not-perfect round-robind since this function is non-transactional but it's valid.
  if (index == context_arr_.size())
    next_io_context_ = 0;
  else
    next_io_context_ = index;
  return io_context;
}

}  // namespace util
