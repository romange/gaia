// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _UTIL_SLIDING_COUNTER_H
#define _UTIL_SLIDING_COUNTER_H

#include <atomic>
#include <mutex>
#include "base/atomic_wrapper.h"
#include "base/integral_types.h"
#include "base/logging.h"

namespace util {

class SlidingSecondBase {
public:
  static void SetCurrentTime_Test(uint32 time);

protected:
  SlidingSecondBase();

  static uint32 CurrentTime() { return current_time_global_.load(std::memory_order_relaxed); }


  mutable base::atomic_wrapper<uint32> last_ts_;
private:
  static void InitTimeGlobal();
  static void* UpdateTimeGlobal(void*);

  static std::atomic<uint32> current_time_global_;
};

template<typename T, unsigned NUM, unsigned PRECISION> class SlidingSecondCounterT
    : public SlidingSecondBase {
  mutable base::atomic_wrapper<T> count_[NUM];

  // updates last_ts_ according to current_time_global_ and returns the latest bin.
  // has const semantics even though it updates last_ts_ and count_ because
  // it hides data updates behind it.
  int32 MoveTsIfNeeded() const;

public:
  static constexpr unsigned SIZE = NUM;
  static constexpr unsigned SPAN = PRECISION*NUM;

  SlidingSecondCounterT() {
    static_assert(NUM > 1, "Invalid window size");
    Reset();
  }

  void Inc() { IncBy(1); }

  T IncBy(int32 delta) {
    int32 bin = MoveTsIfNeeded();
    T tmp = count_[bin].fetch_add(delta, std::memory_order_acq_rel);
    return tmp;
  }

  // Compares whether the value in the current bucket is greater or equal than delta and decrements
  // by delta. This operation is atomic.
  // Returns the value from the bin after the update...
  T DecIfNotLess(int32 delta) {
    int32 bin = MoveTsIfNeeded();
    T val;
    do {
      val = count_[bin].load(std::memory_order_acquire);
      if (val < delta) {
        return val;
      }
    } while(!count_[bin].compare_exchange_weak(val, val - delta,  std::memory_order_acq_rel));
    return val - delta;
  }

  // Sums last cnt counts, starting from (last_ts_ - offset) and going back.
  // offset + cnt must be less or equal to NUM.
  T SumLast(unsigned offset, unsigned count = unsigned(-1)) const;

  T Sum() const {
    MoveTsIfNeeded();
    T sum = 0;
    for (unsigned i = 0; i < NUM; ++i)
      sum += count_[i].load(std::memory_order_acquire);
    return sum;
  }

  void Reset() {
    for (unsigned i = 0; i < NUM; ++i)
      count_[i].store(0, std::memory_order_release);
  }

  uint32 last_ts() const { return last_ts_; }
  static unsigned span() { return SPAN; }
  static unsigned bin_span() {return PRECISION; }
};

template<unsigned NUM, unsigned PRECISION> using SlidingSecondCounter =
    SlidingSecondCounterT<uint32, NUM, PRECISION>;

class QPSCount {
  // We store 1s resolution in 10 cells window.
  // This way we can reliable read 8 already finished counts when we have another 2
  // to be filled up.
  SlidingSecondCounter<10, 1> window_;

public:
  void Reset() { window_.Reset(); }

  void Inc() {
    window_.Inc();
  }

  uint32 Get() const;
};



/*********************************************
 Implementation section.
**********************************************/

template<typename T, unsigned NUM, unsigned PRECISION>
    int32 SlidingSecondCounterT<T, NUM, PRECISION>::MoveTsIfNeeded() const {
  uint32 current_time = CurrentTime() / PRECISION;
  uint32 last_ts = last_ts_.load(std::memory_order_acquire);
  if (last_ts >= current_time) {
    // last_ts > current_time can happen if the thread was preempted before last_ts_.load,
    // and another thread already updated last_ts_ before this load.
    return last_ts % NUM;
  }
  if (last_ts + NUM <= current_time) {
    // Reset everything.
    for (uint32 i = 0; i < NUM; ++i) {
      count_[i].store(0, std::memory_order_release);
    }
  } else {
    // Reset delta upto current_time including.
    for (uint32 i = last_ts + 1; i <= current_time; ++i) {
      count_[i % NUM].store(0, std::memory_order_release);
    }
  }
  if (last_ts_.compare_exchange_strong(last_ts, current_time, std::memory_order_acq_rel)) {
    return current_time % NUM;
  } else {
    return last_ts % NUM;
  }
}

template<typename T, unsigned NUM, unsigned PRECISION>
    T SlidingSecondCounterT<T, NUM, PRECISION>::SumLast(unsigned offset, unsigned count) const {
  DCHECK_LT(offset, NUM);
  if (count > NUM - offset) {
    count = NUM - offset;
  }
  T sum = 0;
  int32 start = MoveTsIfNeeded() - offset - count + 1;
  if (start < 0) start += NUM;
  for (unsigned i = 0; i < count; ++i) {
    sum += count_[ (start + i) % NUM ].load(std::memory_order_acquire);
  }
  return sum;
}

}  // namespace util

#endif  // _UTIL_SLIDING_COUNTER_H