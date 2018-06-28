// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#define __STDC_FORMAT_MACROS 1

#include <atomic>
#include <csignal>
#include "base/walltime.h"
#include "base/logging.h"
#include "base/pthread_utils.h"

#include <sys/timerfd.h>
#include <cstdio>

using std::string;

void SleepForMilliseconds(uint32 milliseconds) {
 // Sleep for a few milliseconds
 struct timespec sleep_time;
 sleep_time.tv_sec = milliseconds / 1000;
 sleep_time.tv_nsec = (milliseconds % 1000) * 1000000;
 while (nanosleep(&sleep_time, &sleep_time) != 0 && errno == EINTR)
   ;  // Ignore signals and wait for the full interval to elapse.
}


namespace base {

static std::atomic<uint64_t> ms_long_counter = ATOMIC_VAR_INIT(0);
static std::atomic_int timer_fd = ATOMIC_VAR_INIT(-1);

static pthread_t timer_thread_id = 0;

static void* UpdateMsCounter(void* arg) {
  CHECK_EQ(0, pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, nullptr));
  int fd;
  uint64 missed;

  while ((fd = timer_fd.load(std::memory_order_relaxed)) > 0) {
    int ret = read(fd, &missed, sizeof missed);
    DCHECK_EQ(8, ret);
    ms_long_counter.fetch_add(missed, std::memory_order_release);
  }
  return nullptr;
}

void SetupJiffiesTimer() {
  if (timer_thread_id)
    return;

  timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
  CHECK_GT(timer_fd, 0);
  struct itimerspec its;

  its.it_value.tv_sec = 0;
  its.it_value.tv_nsec = 100000;  // The first expiration in 0.1ms.

  // Setup periodic timer of the same interval.
  its.it_interval.tv_sec = its.it_value.tv_sec;
  its.it_interval.tv_nsec = its.it_value.tv_nsec;

  CHECK_EQ(0, timerfd_settime(timer_fd, 0, &its, NULL));
  timer_thread_id = base::StartThread("MsTimer", &UpdateMsCounter, nullptr);
  struct sched_param sparam;
  sparam.sched_priority = 1;

  pthread_setschedparam(timer_thread_id, SCHED_FIFO, &sparam);
}

void SleepMicros(uint32 usec) {
  struct timespec sleep_time;
  sleep_time.tv_sec = usec / kNumMicrosPerSecond;
  sleep_time.tv_nsec = (usec % kNumMicrosPerSecond) * 1000;
  while (nanosleep(&sleep_time, &sleep_time) != 0 && errno == EINTR) {}
}

#if 0
void SetupMillisecondTimer() {
  if (millis_timer)
    return;
  static_assert(std::is_pointer<timer_t>::value, "");

#if !defined(ATOMIC_LLONG_LOCK_FREE) || ATOMIC_LLONG_LOCK_FREE != 2
  #error ATOMIC_LLONG must be lock free
#endif

  CHECK_LT(SIGRTMIN, SIGRTMAX);
  CHECK(ms_long_counter.is_lock_free());

  ms_long_counter = 0;
  struct sigaction sa;

  // SA_RESTART is used in order to tell that this signal should not interrupt system calls
  // like read(). In fact they are interrupted but resume themselves without returning -1
  // in the middle (i.e. transparent to caller).
  // SA_SIGINFO tells that we pass sa_sigaction and not sa_handler.
  sa.sa_flags = SA_RESTART | SA_SIGINFO;
  sa.sa_sigaction = &MonotonicMsHandler;
  sigemptyset(&sa.sa_mask);
  CHECK_EQ(0, sigaction(SIGRTMIN, &sa, NULL));

#if 0
  // Do not see the reason to do it.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGRTMIN);
  CHECK_EQ(0, sigprocmask(SIG_SETMASK, &mask, NULL));
#endif

  struct sigevent sev;
  sev.sigev_notify = SIGEV_SIGNAL;
  sev.sigev_signo = SIGRTMIN;
  // sev.sigev_value.sival_ptr = &millis_timer;

  CHECK_EQ(0, timer_create(CLOCK_MONOTONIC, &sev, &millis_timer));

  struct itimerspec its;
  its.it_value.tv_sec = 0;
  its.it_value.tv_nsec = 1000000;  // The first expiration in 1ms.

  // Setup periodic timer of the same interval.
  its.it_interval.tv_sec = its.it_value.tv_sec;
  its.it_interval.tv_nsec = its.it_value.tv_nsec;

  CHECK_EQ(0, timer_settime(millis_timer, 0, &its, NULL));
  // CHECK_EQ(0, sigprocmask(SIG_UNBLOCK, &mask, NULL));
}


void DestroyMillisecondTimer() {
  if (!millis_timer) return;

  struct sigaction sa;
  sa.sa_flags = SA_RESTART | SA_SIGINFO;
  sa.sa_sigaction = nullptr;
  sigemptyset(&sa.sa_mask);

  CHECK_EQ(0, sigaction(SIGRTMIN, &sa, NULL));

  CHECK_EQ(0, timer_delete(millis_timer));
  millis_timer = nullptr;
}

#endif

void DestroyJiffiesTimer() {
  if (!timer_thread_id) return;

  int fd = timer_fd.exchange(0);
  pthread_join(timer_thread_id, nullptr);

  timer_thread_id = 0;
  CHECK_EQ(0, close(fd));
}

uint64 GetMonotonicJiffies() {
  return ms_long_counter.load(std::memory_order_acquire);
}

uint64 GetMonotonicMicrosFast() {
  return ms_long_counter.load(std::memory_order_acquire) * 100;
}

void StringAppendStrftime(string* dst,
                          const char* format,
                          time_t when,
                          bool local) {
  struct tm tm;
  bool conversion_error;
  if (local) {
    conversion_error = (localtime_r(&when, &tm) == NULL);
  } else {
    conversion_error = (gmtime_r(&when, &tm) == NULL);
  }
  if (conversion_error) {
    // If we couldn't convert the time, don't append anything.
    return;
  }

  char space[512];

  size_t result = strftime(space, sizeof(space), format, &tm);

  if (result < sizeof(space)) {
    // It fit
    dst->append(space, result);
    return;
  }
}

}  // namespace base
