// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <thread>
#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/condition_variable.hpp>

namespace util {

// Single threaded but fiber friendly PeriodicTask. Runs directly from IO fiber therefore
// should run only cpu, non-blocking tasks which should
// not block the calling fiber.
// 'Cancel' blocks the calling fiber until the scheduled callback finished running.
class PeriodicTask {
  enum {ALARMED = 0x1, SHUTDOWN = 0x2};

 public:
  using timer_t = ::boost::asio::steady_timer;
  using duration_t = timer_t::duration;
  using error_code = boost::system::error_code;

  PeriodicTask(::boost::asio::io_context& cntx, duration_t d) : timer_(cntx), d_(d), state_(0) {}
  PeriodicTask(PeriodicTask&&) = default;

  ~PeriodicTask() { Cancel(); }

  // f must be non-blocking function because it runs directly in IO fiber.
  template<typename Func> void Start(Func&& f) {
    timer_.expires_after(d_);
    state_ |= ALARMED;

    RunInternal(std::forward<Func>(f));
  }

  // Cancels the task and blocks until all the callbacks finished to run.
  // Since it blocks - it should not run in IO fiber.
  void Cancel();

 private:
   template<typename Func> void RunInternal(Func&& f) {
    timer_.async_wait([this, f = std::forward<Func>(f)] (const error_code& ec) mutable {
      if (ec == boost::asio::error::operation_aborted || (state_ & SHUTDOWN)) {
        Disalarm();
        return;
      }

      f();
      Start(std::forward<Func>(f));
    });
  }

  void Disalarm() {
    state_ &= ~uint8_t(ALARMED);
  }

  timer_t timer_;
  duration_t d_;
  uint8_t state_ ;
};


// Each tasks runs in a new thread, thus not blocking the IO fiber. The next invocation of the
// task will skip the run if the previous has finished.
class PeriodicWorkerTask {
 public:
  PeriodicWorkerTask(::boost::asio::io_context& cntx, PeriodicTask::duration_t d) : pt_(cntx, d) {}

  template<typename Func> void Start(Func&& f) {
    pt_.Start([this, f = PackagedTask(std::forward<Func>(f))] () {
      if (AllowRunning()) {
        std::thread(f).detach();
      };
    });
  }

  void Cancel();
 private:
  template<typename Func> auto PackagedTask(Func&& f) {
    return [this, f = std::forward<Func>(f)]() {
      f();
      Epilog();
    };
  }

  bool AllowRunning() {
    bool val = false;
    return is_running_.compare_exchange_strong(val, true);
  }

  void Epilog();

  std::atomic_bool is_running_{false};

  ::boost::fibers::mutex m_;
  ::boost::fibers::condition_variable   cond_;
  PeriodicTask pt_;
};

}  // namespace util

