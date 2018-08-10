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

class round_robin : public fibers::algo::algorithm {
 private:
  std::shared_ptr< asio::io_service>      io_svc_;
  asio::steady_timer                      suspend_timer_;
//]
  fibers::scheduler::ready_queue_type     rqueue_;
  fibers::mutex                           mtx_;
  fibers::condition_variable              cnd_;
  std::size_t                             counter_{ 0 };

 public:
  // [asio_rr_service_top
  struct rr_service : public asio::io_context::service {

    // Required by add_service.
    static asio::execution_context::id  id;

    // std::unique_ptr< asio::io_service::work >    work_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;

    rr_service(asio::io_context& io_svc) : asio::io_context::service(io_svc),
      work_{asio::make_work_guard(io_svc)} {
    }

    virtual ~rr_service() {}

    rr_service(rr_service const&) = delete;
    rr_service& operator=(rr_service const&) = delete;

    void shutdown_service() final {
      work_.reset();
      VLOG(1) << "Work reset";
    }
  };
//]

//[asio_rr_ctor
  round_robin(const std::shared_ptr< asio::io_context >& io_svc) :
      io_svc_(io_svc), suspend_timer_( * io_svc_) {
    // We use add_service() very deliberately. This will throw
    // service_already_exists if you pass the same io_context instance to
    // more than one round_robin instance.
    asio::add_service(*io_svc_, new rr_service(*io_svc_));

    asio::post(*io_svc_, [this] () mutable { this->loop();});
  }

  void awakened(fibers::context * ctx) noexcept {
      BOOST_ASSERT( nullptr != ctx);
      BOOST_ASSERT( ! ctx->ready_is_linked() );
      ctx->ready_link( rqueue_); /*< fiber, enqueue on ready queue >*/
      if ( ! ctx->is_context( fibers::type::dispatcher_context) ) {
          ++counter_;
      }
      // VLOG(1) << "awakened: " << ctx->get_id();
  }

  fibers::context* pick_next() noexcept {
    fibers::context * ctx( nullptr);
    if (!rqueue_.empty() ) { /*<
        pop an item from the ready queue
    >*/
        ctx = & rqueue_.front();
        rqueue_.pop_front();
        BOOST_ASSERT( nullptr != ctx);
        BOOST_ASSERT( fibers::context::active() != ctx);
        if ( ! ctx->is_context(fibers::type::dispatcher_context) ) {
            --counter_;
        }
    }
    return ctx;
  }

  bool has_ready_fibers() const noexcept {
      return 0 < counter_;
  }

  void suspend_until(std::chrono::steady_clock::time_point const& abs_time) noexcept final {
    VLOG(1) << "suspend_until " << abs_time.time_since_epoch().count();

      // Set a timer so at least one handler will eventually fire, causing
      // run_one() to eventually return.
    if ( (std::chrono::steady_clock::time_point::max)() != abs_time) {
    // Each expires_at(time_point) call cancels any previous pending
    // call. We could inadvertently spin like this:
    // dispatcher calls suspend_until() with earliest wake time
    // suspend_until() sets suspend_timer_
    // lambda loop calls run_one()
    // some other asio handler runs before timer expires
    // run_one() returns to lambda loop
    // lambda loop yields to dispatcher
    // dispatcher finds no ready fibers
    // dispatcher calls suspend_until() with SAME wake time
    // suspend_until() sets suspend_timer_ to same time, canceling
    // previous async_wait()
    // lambda loop calls run_one()
    // asio calls suspend_timer_ handler with operation_aborted
    // run_one() returns to lambda loop... etc. etc.
    // So only actually set the timer when we're passed a DIFFERENT
    // abs_time value.
        suspend_timer_.expires_at( abs_time);
        suspend_timer_.async_wait([](system::error_code const&){
                                    this_fiber::yield();
                                  });
    }
    cnd_.notify_one();
  }
//]

  void notify() noexcept {
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
    suspend_timer_.async_wait([](system::error_code const&){
                                this_fiber::yield();
                              });
    suspend_timer_.expires_at( std::chrono::steady_clock::now() );
  }

//]

 private:
    void loop() {
      while (!io_svc_->stopped()) {
        if (has_ready_fibers() ) {
          while (io_svc_->poll());

          // block this fiber till all pending (ready) fibers are processed
          // == round_robin::suspend_until() has been called
          std::unique_lock<fibers::mutex > lk(mtx_);
          cnd_.wait(lk);
        } else {
            // run one handler inside io_context
            // if no handler available, block this thread
            if (!io_svc_->run_one() ) {
                break;
            }
        }
      }
    }
};

asio::io_context::id round_robin::rr_service::id;

}

thread_local size_t IoContextPool::context_indx_ = 0;

IoContextPool::IoContextPool(std::size_t pool_size)
  : context_arr_(pool_size), thread_arr_(pool_size) {
  if (pool_size == 0)
    pool_size = std::thread::hardware_concurrency();
  for (size_t i = 0; i < pool_size; ++i) {
    context_arr_[i] = std::make_shared<asio::io_context>();
  }
}

void IoContextPool::Run() {
  for (size_t i = 0; i < thread_arr_.size(); ++i) {
    thread_arr_[i].tid = base::StartThread("IoPool",
      [this, i]() {
        context_indx_ = i;
        fibers::use_scheduling_algorithm<round_robin>(context_arr_[i]);

        context_arr_[i]->run();
      });
  }
}

void IoContextPool::Stop() {
  for (size_t i = 0; i < context_arr_.size(); ++i) {
    context_arr_[i]->stop();
  }
}

void IoContextPool::Join() {
  for (const TInfo& tinfo : thread_arr_) {
    pthread_join(tinfo.tid, nullptr);
  }
}

asio::io_context& IoContextPool::GetNextContext() {
  // Use a round-robin scheme to choose the next io_context to use.
  boost::asio::io_context& io_context = *context_arr_[next_io_context_];
  ++next_io_context_;
  if (next_io_context_ == context_arr_.size())
    next_io_context_ = 0;
  return io_context;
}

}  // namespace util
