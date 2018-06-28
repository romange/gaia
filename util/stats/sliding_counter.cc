// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/stats/sliding_counter.h"

#include "base/walltime.h"
#include "base/pthread_utils.h"

#include <time.h>

namespace util {

static pthread_once_t g_init_once = PTHREAD_ONCE_INIT;
static pthread_t g_time_update_thread = 0;
static bool g_test_used = false;

std::atomic<uint32> SlidingSecondBase::current_time_global_ = ATOMIC_VAR_INIT(0);

SlidingSecondBase::SlidingSecondBase() {
  pthread_once(&g_init_once, &SlidingSecondBase::InitTimeGlobal);
}

void SlidingSecondBase::InitTimeGlobal() {
  if (!g_test_used) current_time_global_ = time(NULL);
  g_time_update_thread = base::StartThread("UpdateTimeTh", &SlidingSecondBase::UpdateTimeGlobal,
                                          nullptr);
}

void* SlidingSecondBase::UpdateTimeGlobal(void*) {
  uint32 t = current_time_global_;
  while(true) {
    // To support unit testing - if current_time_global_ was out of sync from what to expect
    uint32 new_val = time(NULL);
    if (g_test_used ||
        !current_time_global_.compare_exchange_strong(t, new_val, std::memory_order_acq_rel))
      break;
    t = new_val;
    SleepForMilliseconds(100);
  }
  return nullptr;
}

void SlidingSecondBase::SetCurrentTime_Test(uint32 time_val) {
  g_test_used = true;
  current_time_global_.store(time_val, std::memory_order_release);
}

uint32 QPSCount::Get() const {
  constexpr unsigned kWinSize = decltype(window_)::SIZE - 1;

  return window_.SumLast(1, kWinSize) / kWinSize; // Average over kWinSize values.
}


}  // namespace util