// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "base/logging.h"
#include "base/walltime.h"

#include <glog/raw_logging.h>

#include "base/walltime.h"
#include "util/asio/io_context.h"

namespace util {

using fibers_ext::BlockingCounter;
using namespace boost;
using namespace std;

class IoFiberPropertiesMgr {
  IoFiberProperties& io_props;

 public:
  IoFiberPropertiesMgr(fibers::fiber_properties* props)
      : io_props(*static_cast<IoFiberProperties*>(props)) {}

  void set_resume_ts(uint64_t ts) { io_props.resume_ts_ = ts; }
  void set_awaken_ts(uint64_t ts) { io_props.awaken_ts_ = ts; }
};

namespace {
constexpr unsigned MAIN_NICE_LEVEL = 0;
constexpr unsigned DISPATCH_LEVEL = IoFiberProperties::NUM_NICE_LEVELS;

// Amount of fiber switches we make before bringing back the main IO loop.
constexpr unsigned MAIN_SWITCH_LIMIT = 100;
constexpr unsigned NOTIFY_GUARD_SHIFT = 16;
constexpr chrono::steady_clock::time_point STEADY_PT_MAX = chrono::steady_clock::time_point::max();

inline int64_t delta_micros(const chrono::steady_clock::time_point tp) {
  static_assert(8 == sizeof(chrono::steady_clock::time_point), "");
  return chrono::duration_cast<chrono::microseconds>(tp - chrono::steady_clock::now()).count();
}

class AsioScheduler final : public fibers::algo::algorithm_with_properties<IoFiberProperties> {
 private:
  using ready_queue_type = fibers::scheduler::ready_queue_type;
  std::shared_ptr<asio::io_context> io_context_;
  std::unique_ptr<asio::steady_timer> suspend_timer_;
  std::atomic_uint_fast64_t notify_cnt_{0};
  uint64_t main_loop_wakes_{0}, worker_pick_start_ts_{0}, main_suspend_ts_;

  std::atomic_uint_fast32_t notify_guard_{0};
  uint32_t last_nice_level_ = 0;
  ready_queue_type rqueue_arr_[IoFiberProperties::NUM_NICE_LEVELS + 1];
  std::size_t ready_cnt_{0};
  std::size_t switch_cnt_{0};

  fibers::context* main_loop_ctx_ = nullptr;
  chrono::steady_clock::time_point suspend_tp_ = STEADY_PT_MAX;

  enum : uint8_t { LOOP_RUN_ONE = 1, MAIN_LOOP_SUSPEND = 2, MAIN_LOOP_FINISHED = 4 };
  uint8_t mask_ = 0;

 public:
  //[asio_rr_ctor
  AsioScheduler(const std::shared_ptr<asio::io_context>& io_svc)
      : io_context_(io_svc), suspend_timer_(new asio::steady_timer(*io_svc)) {}

  ~AsioScheduler();

  void awakened(fibers::context* ctx, IoFiberProperties& props) noexcept override;

  fibers::context* pick_next() noexcept override;

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
    if (!ctx->is_context(fibers::type::dispatcher_context)) {
      DCHECK_GT(ready_cnt_, 0);
      --ready_cnt_;
    }

    // Here we know that ctx was in our ready queue, but we've unlinked
    // it. We happen to have a method that will (re-)add a context* to the
    // right place in the ready queue.
    awakened(ctx, props);
  }

  bool has_ready_fibers() const noexcept final { return 0 < ready_cnt_; }

  // suspend_until halts the thread in case there are no active fibers to run on it.
  // This is done by dispatcher fiber.
  void suspend_until(chrono::steady_clock::time_point const& abs_time) noexcept final {
    DVLOG(2) << "suspend_until " << abs_time.time_since_epoch().count();

    // Only dispatcher context stops the thread.
    // Which means that either the main loop in MAIN_LOOP_SUSPEND state or it finished and we
    // are in shutdown phase.
    // suspend_until is responsible for THREAD SUSPEND which is not what MAIN_LOOP_SUSPEND for.
    // In fact, we implement THREAD SUSPEND by deferring the control to asio::io_context::run_one.
    // For that to happen we need to resume the MAIN_LOOP_SUSPEND, and that's why we
    // schedule main_loop_ctx_ below.
    DCHECK(fibers::context::active()->is_context(fibers::type::dispatcher_context));

    // Set a timer so at least one handler will eventually fire, causing
    // run_one() to eventually return.
    if (abs_time != STEADY_PT_MAX && suspend_tp_ != abs_time) {
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
      suspend_tp_ = abs_time;
      suspend_timer_->expires_at(abs_time);

      suspend_timer_->async_wait([this, abs_time](const system::error_code& ec) {
        // Call Suspend handler.
        SuspendCb(ec, abs_time);
      });
      VLOG(1) << "Arm suspender at micros from now " << delta_micros(abs_time)
              << ", abstime: " << abs_time.time_since_epoch().count();
    }
    CHECK_EQ(0, mask_ & LOOP_RUN_ONE) << "Deadlock detected";

    // Awake main_loop_ctx_ in WaitTillFibersSuspend().
    main_loop_ctx_->get_scheduler()->schedule(main_loop_ctx_);
  }
  //]

  // This function is called from remote threads, to wake this thread in case it's sleeping.
  // In our case, "sleeping" means - might stuck inside run_one waiting for events.
  // We break from run_one() by scheduling a timer.
  void notify() noexcept final;

  void MainLoop();

 private:
  void SuspendCb(const system::error_code& ec, chrono::steady_clock::time_point tp) {
    VLOG(1) << "Fire suspender " << tp.time_since_epoch().count() << " " << ec;
    if (!ec) {
      // The timer has successfully finished.
      suspend_tp_ = STEADY_PT_MAX;

      // Switch to dispatch fiber to awaken fibers.
      this_fiber::yield();
    } else {
      CHECK_EQ(ec, asio::error::operation_aborted);
    }
  }
  void WaitTillFibersSuspend();
};

AsioScheduler::~AsioScheduler() {}

void AsioScheduler::MainLoop() {
  asio::io_context* io_cntx = io_context_.get();
  main_loop_ctx_ = fibers::context::active();

  while (!io_cntx->stopped()) {
    if (has_ready_fibers()) {
      while (io_cntx->poll())
        ;

      auto start = base::GetMonotonicMicrosFast();
      // Gives up control to allow other fibers to run in the thread.
      WaitTillFibersSuspend();
      auto delta = base::GetMonotonicMicrosFast() - start;
      LOG_IF(INFO, delta > 100000) << "Scheduler: Took " << delta / 1000 << " ms to resume";

      continue;
    }

    // run one handler inside io_context
    // if no handler available, blocks this thread
    DVLOG(2) << "MainLoop::RunOneStart";
    mask_ |= LOOP_RUN_ONE;
    if (!io_cntx->run_one()) {
      mask_ &= ~LOOP_RUN_ONE;
      break;
    }
    DVLOG(2) << "MainLoop::RunOneEnd";
    mask_ &= ~LOOP_RUN_ONE;
  }

  VLOG(1) << "MainLoop exited";
  mask_ |= MAIN_LOOP_FINISHED;

  // We won't run "run_one" anymore therefore we can remove our dependence on suspend_timer_ and
  // asio:context. In fact, we can not use timed-waits from now on using fibers code because we've
  // relied on asio for that.
  // We guard suspend_timer_ using notify_guard_ with no locks in notify().
  // However we must block here.
  // It's a shutdown phase, so we do not care. We can not rely on fiber scheduling methods since
  // they rely on the loop above. Therefore we just use thread yield.

  // Signal that we shutdown and check if we need to wait for current notify calls to exit.
  constexpr uint32_t NOTIFY_BIT = 1U << NOTIFY_GUARD_SHIFT;
  uint32_t seq = notify_guard_.fetch_or(NOTIFY_BIT);
  while (seq) {
    pthread_yield();                                // almost like a sleep
    seq = notify_guard_.load() & (NOTIFY_BIT - 1);  // Block untill all notify calls exited.
  }
  suspend_timer_.reset();  // now we can free suspend_timer_.

  VLOG(1) << "MainLoopWakes/NotifyCnt: " << main_loop_wakes_ << "/" << notify_cnt_;
}

void AsioScheduler::WaitTillFibersSuspend() {
  // block this fiber till all (ready) fibers are processed
  // or when  AsioScheduler::suspend_until() has been called or awaken() decided to resume it.
  mask_ |= MAIN_LOOP_SUSPEND;

  DVLOG(2) << "WaitTillFibersSuspend:Start";
  main_suspend_ts_ = base::GetMonotonicMicrosFast();

  main_loop_ctx_->suspend();
  mask_ &= ~MAIN_LOOP_SUSPEND;
  switch_cnt_ = 0;
  DVLOG(2) << "WaitTillFibersSuspend:End";
}

// Thread-local function
void AsioScheduler::awakened(fibers::context* ctx, IoFiberProperties& props) noexcept {
  DCHECK(!ctx->ready_is_linked());

  ready_queue_type* rq;
  if (ctx->is_context(fibers::type::dispatcher_context)) {
    rq = rqueue_arr_ + DISPATCH_LEVEL;
    DVLOG(2) << "Ready: " << fibers_ext::short_id(ctx) << " dispatch"
             << ", ready_cnt: " << ready_cnt_;
  } else {
    unsigned nice = props.nice_level();
    DCHECK_LT(nice, IoFiberProperties::NUM_NICE_LEVELS);
    rq = rqueue_arr_ + nice;
    ++ready_cnt_;  // increase the number of awakened/ready fibers.
    if (last_nice_level_ > nice)
      last_nice_level_ = nice;

    uint64_t now = base::GetMonotonicMicrosFast();
    IoFiberPropertiesMgr{&props}.set_awaken_ts(now);

    // In addition, we wake main_loop_ctx_ is too many switches ocurred
    // while it was suspended.
    // It's a convenient place to wake because we are sure there is a least
    // one ready worker in addition to main_loop_ctx_ and it won't stuck in
    // run_one().
    // * main_loop_ctx_->ready_is_linked() could be linked already in the previous invocations
    // of awakened before pick_next resumed it.
    if (nice > MAIN_NICE_LEVEL && (mask_ & MAIN_LOOP_SUSPEND) && switch_cnt_ > 0 &&
        !main_loop_ctx_->ready_is_linked()) {
      if (now - main_suspend_ts_ > 5000) {  // 5ms {
        DVLOG(2) << "Wake MAIN_LOOP_SUSPEND " << fibers_ext::short_id(main_loop_ctx_)
                 << ", r/s: " << ready_cnt_ << "/" << switch_cnt_;

        switch_cnt_ = 0;
        ++ready_cnt_;
        main_loop_ctx_->ready_link(rqueue_arr_[MAIN_NICE_LEVEL]);
        last_nice_level_ = MAIN_NICE_LEVEL;
        ++main_loop_wakes_;
      }
    }

    DVLOG(2) << "Ready: " << fibers_ext::short_id(ctx) << "/" << props.name()
             << ", nice/rdc: " << nice << "/" << ready_cnt_;
  }

  ctx->ready_link(*rq); /*< fiber, enqueue on ready queue >*/
}

fibers::context* AsioScheduler::pick_next() noexcept {
  fibers::context* ctx(nullptr);
  using fibers_ext::short_id;

  auto now = base::GetMonotonicMicrosFast();
  auto delta = now - worker_pick_start_ts_;
  worker_pick_start_ts_ = now;

  for (; last_nice_level_ < IoFiberProperties::NUM_NICE_LEVELS; ++last_nice_level_) {
    auto& q = rqueue_arr_[last_nice_level_];
    if (q.empty())
      continue;

    // pop an item from the ready queue
    ctx = &q.front();
    q.pop_front();

    DCHECK(!ctx->is_context(fibers::type::dispatcher_context));
    DCHECK_GT(ready_cnt_, 0);
    --ready_cnt_;

    RAW_VLOG(2, "Switching from %x to %x switch_cnt(%d)", short_id(), short_id(ctx), switch_cnt_);
    DCHECK(ctx != fibers::context::active());

    IoFiberPropertiesMgr{ctx->get_properties()}.set_resume_ts(now);

    // Checking if we want to resume to main loop prematurely to preserve responsiveness
    // of IO loop. MAIN_NICE_LEVEL is reserved for the main loop so we count only
    // when we switch to other fibers and the loop is in MAIN_LOOP_SUSPEND state.
    if ((mask_ & MAIN_LOOP_SUSPEND) && last_nice_level_ > MAIN_NICE_LEVEL) {
      ++switch_cnt_;

      if (delta > 30000) {
        auto* active = fibers::context::active();
        if (!active->is_context(fibers::type::main_context)) {
          auto& props = static_cast<IoFiberProperties&>(*active->get_properties());

          LOG(INFO) << props.name() << " took " << delta / 1000 << " ms";
        }
      }

      // We can not prematurely wake MAIN_LOOP_SUSPEND if ready_cnt_ == 0.
      // The reason for this is that in that case the main loop will call "run_one" during the
      // next iteration and might get stuck there because we never reached suspend_until
      // that might configure asio loop to break early.
      // This is why we break from MAIN_LOOP_SUSPEND inside waken call, where
      // we are sure there is at least one worker fiber, and the main loop won't stuck in run_one.
    }

    RAW_VLOG(3, "pick_next: %x", short_id(ctx));

    return ctx;
  }

  DCHECK_EQ(0, ready_cnt_);

  auto& dispatch_q = rqueue_arr_[DISPATCH_LEVEL];
  if (!dispatch_q.empty()) {
    fibers::context* ctx = &dispatch_q.front();
    dispatch_q.pop_front();

    RAW_VLOG(2, "Switching from ", short_id(), " to dispatch ", short_id(ctx),
             ", mask: ", unsigned(mask_));
    return ctx;
  }

  RAW_VLOG(2, "pick_next: null");

  return nullptr;
}

void AsioScheduler::notify() noexcept {
  uint32_t seq = notify_guard_.fetch_add(1, std::memory_order_acq_rel);

  if ((seq >> 16) == 0) {  // Test whether are not shuttind down.

    // We need to break from run_one asap.
    // If we've already armed suspend_timer_ via suspend_until then cancelling it would suffice.
    // However, in case suspend_until has not been called or called STEADY_PT_MAX,
    // then there is nothing to cancel and run_one won't break.
    // In addition, AsioScheduler::notify is called from remote thread so may only access
    // thread-safe data. Therefore, the simplest solution would be just post a callback.
    RAW_VLOG(1, "AsioScheduler::notify");

    /*suspend_timer_->expires_at(chrono::steady_clock::now());
    suspend_timer_->async_wait([this](const system::error_code& ec) {
      VLOG(1) << "Awake suspender, tp: " << suspend_tp_.time_since_epoch().count() << " " << ec;
      this_fiber::yield();
    });*/
    asio::post(*io_context_, [] { this_fiber::yield(); });
    notify_cnt_.fetch_add(1, std::memory_order_relaxed);
  } else {
    RAW_VLOG(1, "Called during shutdown phase");
  }
  notify_guard_.fetch_sub(1, std::memory_order_acq_rel);
}

}  // namespace

constexpr unsigned IoFiberProperties::MAX_NICE_LEVEL;
constexpr unsigned IoFiberProperties::NUM_NICE_LEVELS;

void IoFiberProperties::SetNiceLevel(unsigned p) {
  // Of course, it's only worth reshuffling the queue and all if we're
  // actually changing the nice.
  p = std::min(p, MAX_NICE_LEVEL);
  if (p != nice_) {
    nice_ = p;
    notify();
  }
}

void IoContext::StartLoop(BlockingCounter* bc) {
  // I do not use use_scheduling_algorithm because I want to retain access to the scheduler.
  // fibers::use_scheduling_algorithm<AsioScheduler>(io_ptr);
  AsioScheduler* scheduler = new AsioScheduler(context_ptr_);
  fibers::context::active()->get_scheduler()->set_algo(scheduler);
  this_fiber::properties<IoFiberProperties>().set_name("io_loop");
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(MAIN_NICE_LEVEL);
  CHECK(fibers::context::active()->is_context(fibers::type::main_context));

  thread_id_ = this_thread::get_id();

  io_context& io_cntx = *context_ptr_;

  // We run the main loop inside the callback of io_context, blocking it until the loop exits.
  // The reason for this is that io_context::running_in_this_thread() is deduced based on the
  // call-stack. GAIA code should use InContextThread() to check whether the code runs in the
  // context's thread.
  Async([scheduler, bc] {
    bc->Dec();
    scheduler->MainLoop();
  });

  // Bootstrap - launch the callback handler above.
  // It will block until MainLoop exits. See comment above.
  io_cntx.run_one();

  // Shutdown phase.
  for (unsigned i = 0; i < 2; ++i) {
    DVLOG(1) << "Cleanup Loop " << i;
    while (io_cntx.poll() || scheduler->has_ready_fibers()) {
      this_fiber::yield();  // while something happens, pass the ownership to other fiber.
    }
    io_cntx.restart();
  }
}

void IoContext::Stop() {
  if (cancellable_arr_.size() > 0) {
    fibers_ext::BlockingCounter cancel_bc(cancellable_arr_.size());

    VLOG(1) << "Cancelling " << cancellable_arr_.size() << " cancellables";
    // Shutdown sequence and cleanup.
    for (auto& k_v : cancellable_arr_) {
      AsyncFiber([&] {
        k_v.first->Cancel();
        cancel_bc.Dec();
      });
    }
    cancel_bc.Wait();
    for (auto& k_v : cancellable_arr_) {
      k_v.second.join();
    }
    cancellable_arr_.clear();
  }

  context_ptr_->stop();
  VLOG(1) << "AsioIoContext stopped";
}

}  // namespace util
