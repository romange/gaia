// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"

#include "base/logging.h"
#include "util/asio/yield.h"

namespace util {

void PeriodicTask::Disalarm() {
  state_ &= ~uint8_t(ALARMED);
  VLOG(1) << "Disalarmed " << int(state_);
}

void PeriodicTask::Cancel() {
  if ((state_ & ALARMED) == 0)
    return;

  state_ |= SHUTDOWN;
  timer_.expires_after(duration_t(0));
  error_code ec;
  while (state_ & ALARMED) {
    timer_.async_wait(fibers_ext::yield[ec]);
  }
  state_ &= ~uint8_t(SHUTDOWN);
}


std::function<void()> PeriodicThreadTask::WrappedFunc(std::function<void()> f) {
  return [this, f = std::move(f)]() {
      f();
      VLOG(1) << "is_running_ = false";

      // We need to lock m_ because otherwise the object could be destroyed right after
      // is_running_ was reset and before cond_ notified. Then this function would have a data race.
      std::lock_guard<::boost::fibers::mutex> lock(m_);
      is_running_.store(false);
      cond_.notify_all();
  };
}

void PeriodicThreadTask::Cancel() {
  pt_.Cancel();

  std::unique_lock<::boost::fibers::mutex> lock(m_);
  cond_.wait(lock, [this]() { return !is_running_; });
  VLOG(1) << "PeriodicThreadTask::Cancel end";
}

}  // namespace util
