// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <thread>
#include <boost/asio/steady_timer.hpp>
#include <boost/fiber/condition_variable.hpp>
#include "util/asio/io_context.h"

namespace util {

// Single threaded but fiber friendly PeriodicTask. Runs directly from IO fiber therefore
// should run only cpu, non-blocking tasks which should not block the calling fiber.
// 'Cancel' may block the calling fiber until the scheduled callback finished running.
class PeriodicTask {
  enum {ALARMED = 0x1, SHUTDOWN = 0x2};

 public:
  using timer_t = ::boost::asio::steady_timer;
  using duration_t = timer_t::duration;
  using error_code = boost::system::error_code;

  PeriodicTask(IoContext& cntx, duration_t d) : timer_(cntx.raw_context()), d_(d), state_(0) {}
  PeriodicTask(PeriodicTask&&) = default;

  ~PeriodicTask() { Cancel(); }

  // f must be non-blocking function because it runs directly in IO fiber.
  // f should accept 'ticks' argument that says by how many duration cycles we are late.
  // Usually ticks=1.
  // For example if we started at time 0, with duration=1s and on n-th invocation we measured
  // "n+1" seconds, then f(2) will be called. The system is self-balancing so for next invocation
  // at (n+2) seconds, we will pass f(1) again.
  template<typename Func> void Start(Func&& f) {
    Alarm();
    RunInternal(std::forward<Func>(f));
  }

  // Cancels the task and blocks until all the callbacks finished to run.
  // Since it blocks - it should not run from IO fiber.
  void Cancel();

 private:
  void Alarm();

  template<typename Func> void RunInternal(Func&& f) {
    timer_.async_wait([this, f = std::forward<Func>(f)] (const error_code& ec) mutable {
      if (ec == boost::asio::error::operation_aborted || (state_ & SHUTDOWN)) {
        Disalarm();
        return;
      }
      duration_t real_d = timer_t::clock_type::now() - last_;

      // for each function invocation we pass at least 1 tick.
      int ticks = std::max<int>(1, real_d / d_);
      f(ticks);
      last_ += d_ * ticks;

      // due to max() rounding, last_ will self balance itself to be close to clock_type::now().
      timer_.expires_at(last_ + d_);
      RunInternal(std::forward<Func>(f));
    });
  }

  void Disalarm() {
    state_ &= ~uint8_t(ALARMED);
  }

  timer_t timer_;
  duration_t d_;
  timer_t::time_point last_;
  uint8_t state_ ;
};


// Each tasks runs in a new thread, thus not blocking the IO fiber. The next invocation of the
// task will skip the run if the previous has finished.
class PeriodicWorkerTask {
 public:
  struct Options {
    std::string name;

    // how many times this task can be skipped before reaching error state.
    // Provide kuint32max number for allowing infinite number of skips.
    // By default we do not allow skipping tasks.
    uint32_t skip_run_margin;

    Options() : skip_run_margin(0) {}
  };

  PeriodicWorkerTask(IoContext& cntx, PeriodicTask::duration_t d,
      const Options& opts = Options{}) : pt_(cntx, d), opts_(opts) {}

  ~PeriodicWorkerTask() { Cancel(); }

  template<typename Func> void Start(Func&& f) {
    pt_.Start([this, f = PackagedTask(std::forward<Func>(f))] (int ticks) {
      if (AllowRunning()) {
        std::thread(f).detach();
      } else {
        HandleSkipRun();
      }
    });
  }

  void Cancel();
  bool IsHanging() const { return is_hanging_; }

 private:
  template<typename Func> auto PackagedTask(Func&& f) {
    return [this, f = std::forward<Func>(f)]() {
      ResetErrorState();
      f();
      Epilog();
    };
  }

  // Enters running state if possible, returns if succeeded.
  bool AllowRunning() {
    bool val = false;
    return is_running_.compare_exchange_strong(val, true);
  }

  void ResetErrorState();
  void Epilog();
  void HandleSkipRun();

  std::atomic_bool is_running_{false};

  ::boost::fibers::mutex m_;
  ::boost::fibers::condition_variable   cond_;
  PeriodicTask pt_;
  Options opts_;

  bool is_hanging_ = false;
  unsigned number_skips_ = 0;
};

}  // namespace util

