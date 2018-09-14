// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/steady_timer.hpp>

namespace util {

// Single threaded but fiber friendly PeriodicTask.
// Cancel blocks the calling fiber until the scheduled callback finished running.
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
  template<typename Func> void Run(Func&& f) {
    timer_.expires_after(d_);
    state_ |= ALARMED;

    RunInternal(std::forward<Func>(f));
  }

  // Cancels the task and blocks until all the callbacks finished to run.
  // Since it blocks - it should not run in IO fiber.
  void Cancel();

 private:
   template<typename Func> void RunInternal(Func&& f) {
    timer_.async_wait([this, f = std::move(f)](const error_code& ec) {
      if (ec == boost::asio::error::operation_aborted || (state_ & SHUTDOWN)) {
        Disalarm();
        return;
      }

      f();
      Run(std::move(f));
    });
  }

  void Disalarm();

  timer_t timer_;
  duration_t d_;
  uint8_t state_ ;
};
}  // namespace util

