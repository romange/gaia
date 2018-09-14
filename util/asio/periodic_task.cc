// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"
#include "base/logging.h"

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

}  // namespace util
