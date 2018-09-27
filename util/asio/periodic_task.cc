// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"

#include "base/logging.h"
#include "util/asio/yield.h"
#include "util/stats/varz_stats.h"


namespace util {

DEFINE_VARZ(VarzCount, task_hang_times);

void PeriodicTask::Cancel() {
  if ((state_ & ALARMED) == 0)
    return;
  VLOG(1) << "Cancel";
  state_ |= SHUTDOWN;
  timer_.expires_after(duration_t(0));
  error_code ec;
  while (state_ & ALARMED) {
    timer_.async_wait(fibers_ext::yield[ec]);
  }
  state_ &= ~uint8_t(SHUTDOWN);
  VLOG(1) << "Cancel Finish";
}

void PeriodicTask::Alarm() {
  CHECK_EQ(0, state_ & ALARMED) << "Can not Start on already alarmed timer, run Cancel first";
  timer_.expires_after(d_);
  state_ |= ALARMED;
}

void PeriodicWorkerTask::ResetErrorState() {
  if (IsHanging()) {
    task_hang_times.IncBy(-1);
    is_hanging_ = false;
  }

  number_skips_ = 0;
}

void PeriodicWorkerTask::Epilog() {
  // We need to lock m_ because otherwise the object could be destroyed right after
  // is_running_ was reset and before cond_ notified. Then this function would have a data race.
  std::lock_guard<::boost::fibers::mutex> lock(m_);
  is_running_.store(false);
  cond_.notify_all();
}

void PeriodicWorkerTask::HandleSkipRun() {
  ++number_skips_;
  if (!is_hanging_ && number_skips_ > opts_.skip_run_margin) {
    is_hanging_ = true;
    task_hang_times.Inc();
  }
  if (IsHanging()) {
    LOG(ERROR) << "Task " << opts_.name << " hands for " << number_skips_ << " times";
  }
}

void PeriodicWorkerTask::Cancel() {
  pt_.Cancel();

  std::unique_lock<::boost::fibers::mutex> lock(m_);
  cond_.wait(lock, [this]() { return !is_running_; });

  ResetErrorState();
  VLOG(1) << "PeriodicWorkerTask::Cancel end";
}

}  // namespace util
