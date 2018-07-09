// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/stats/sliding_counter.h"

#include "base/walltime.h"
#include "base/pthread_utils.h"

#include <time.h>
#include "base/init.h"
#include "base/port.h"

namespace util {

static uint32 g_test_used = kuint32max;

SlidingSecondBase::SlidingSecondBase() {
}

uint32 SlidingSecondBase::CurrentTime() {
  if (PREDICT_TRUE(g_test_used == kuint32max)) {
    return base::GetMonotonicMicrosFast() / base::kNumMicrosPerSecond;
  }
  return g_test_used;
}


void SlidingSecondBase::SetCurrentTime_Test(uint32 time_val) {
  g_test_used = time_val;
}

uint32 QPSCount::Get() const {
  constexpr unsigned kWinSize = decltype(window_)::SIZE - 1;

  return window_.SumLast(1, kWinSize) / kWinSize; // Average over kWinSize values.
}


}  // namespace util
