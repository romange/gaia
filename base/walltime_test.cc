// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/walltime.h"

#include <sys/timerfd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <csignal>
#include <time.h>
#include <functional>
#include <thread>
#include <benchmark/benchmark.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"

DEFINE_int32(test_sleep_delay_sec, 1, "");

namespace base {

class WalltimeTest : public testing::Test {
};

TEST_F(WalltimeTest, BasicTimer) {
  LOG(INFO) << "Resolution in ms: " << Timer::ResolutionUsec() / 1000;

  Timer timer;
  EXPECT_EQ(0, timer.EvalUsec());
  SleepForMilliseconds(4);
  EXPECT_LE(timer.EvalUsec(), 8000);
}

class Dummy {
  char buf[1000];
public:
  void __attribute__ ((noinline)) f() noexcept {
  }

  static void  __attribute__ ((noinline)) CallMethod(void* cntx) {
    Dummy* me = (Dummy*)cntx;
    me->f();
  }
};


void __attribute__ ((noinline)) MyFunction(void* foo, void* bar) {
  (void)foo;
  (void)bar;
}


TEST_F(WalltimeTest, ClockRes) {
  timespec ts;
  ASSERT_EQ(0, clock_getres(CLOCK_REALTIME_COARSE, &ts));
  ASSERT_EQ(0, ts.tv_sec);
  EXPECT_LE(ts.tv_nsec, 4000000);
  LOG(INFO) << "CLOCK_REALTIME_COARSE res: " << ts.tv_nsec;

  ASSERT_EQ(0, clock_getres(CLOCK_MONOTONIC_COARSE, &ts));
  ASSERT_EQ(0, ts.tv_sec);
  EXPECT_LE(ts.tv_nsec, 4000000);
  LOG(INFO) << "CLOCK_MONOTONIC_COARSE res: " << ts.tv_nsec;

  ASSERT_EQ(0, clock_getres(CLOCK_PROCESS_CPUTIME_ID, &ts));
  EXPECT_LE(ts.tv_nsec, 4000000);

  ASSERT_EQ(0, clock_getres(CLOCK_MONOTONIC, &ts));
  EXPECT_LE(ts.tv_nsec, 1000000);
}

TEST_F(WalltimeTest, CoarseMonotonic) {
  for (int i = 0; i < 10; ++i) {
    MicrosecondsInt64 coarse = Timer::Usec();
    MicrosecondsInt64 exact = GetMonotonicMicros();
    EXPECT_LE(coarse, exact);
    EXPECT_LE(exact - coarse, 8000);
    SleepMicros(1000);
  }
}

TEST_F(WalltimeTest, TimerMonotonic) {
  MicrosecondsInt64 start = GetMonotonicMicros();
  if (FLAGS_test_sleep_delay_sec >= 0) {
    // cpu free.
    SleepForMilliseconds(FLAGS_test_sleep_delay_sec*kNumMillisPerSecond);
  } else {
    // cpu intensive.
    while (GetMonotonicMicros() < start - FLAGS_test_sleep_delay_sec*kNumMicrosPerSecond);
  }

  unsigned millis = GetMonotonicJiffies();
  MicrosecondsInt64 delta = (GetMonotonicMicros() - start) / 100;
  ASSERT_LE(millis/10, delta + 1);
  ASSERT_LE(delta - millis, 1) << " " << millis;

}

TEST_F(WalltimeTest, TimerMonotonicNoInterrupt) {
  siginfo_t info;
  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = 10000000;
  errno = 0;

  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGALRM);
  ASSERT_EQ(-1, sigtimedwait(&mask, &info, &timeout));
  ASSERT_EQ(EAGAIN, errno);
}

TEST_F(WalltimeTest, ThreadTime) {
  std::thread t1([]() {
    MicrosecondsInt64 thread_start = GetClockMicros<CLOCK_THREAD_CPUTIME_ID>();
    // MicrosecondsInt64 proc_start = GetClockMicros<CLOCK_PROCESS_CPUTIME_ID>();
    MicrosecondsInt64 wall_start = GetMonotonicMicros();
    SleepForMilliseconds(100);

    EXPECT_GT(GetMonotonicMicros(), wall_start + 99*kNumMicrosPerMilli);

    // EXPECT_LT(GetClockMicros<CLOCK_PROCESS_CPUTIME_ID>() - proc_start,10*kNumMicrosPerMilli);
    EXPECT_LT(GetClockMicros<CLOCK_THREAD_CPUTIME_ID>() - thread_start, 1*kNumMicrosPerMilli);
  });

  t1.join();
}

using benchmark::DoNotOptimize;

static void BM_TimeX4(benchmark::State& state) {
  while (state.KeepRunning()) {
    DoNotOptimize(time(NULL));
    DoNotOptimize(time(NULL));
    DoNotOptimize(time(NULL));
    DoNotOptimize(time(NULL));
  }
}
BENCHMARK(BM_TimeX4);

static void BM_GetTimeOfDay(benchmark::State& state) {
  struct timeval tv;
  while (state.KeepRunning()) {
    DoNotOptimize(gettimeofday(&tv, NULL));
  }
}
BENCHMARK(BM_GetTimeOfDay);

template<clockid_t cid> void BM_ClockType(benchmark::State& state) {
  timespec ts; \
  while (state.KeepRunning()) {
    DoNotOptimize(clock_gettime(cid, &ts));
  }
}
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_PROCESS_CPUTIME_ID);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_THREAD_CPUTIME_ID);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME_ALARM);

static void BM_TimerGetTime(benchmark::State& state) {
  struct sigevent sev;
  timer_t timerid;
  memset(&sev, 0, sizeof(sev));
  sev.sigev_notify = SIGEV_NONE;
  sev.sigev_signo = SIGALRM;
  CHECK_EQ(0, timer_create(CLOCK_MONOTONIC, &sev, &timerid));

  struct itimerspec cur_val;
  while (state.KeepRunning()) {
    CHECK_EQ(0, timer_gettime(timerid, &cur_val));
  }
  CHECK_EQ(0, timer_delete(timerid));
}
BENCHMARK(BM_TimerGetTime);

static void BM_MonotonicJiffies(benchmark::State& state) {
  while (state.KeepRunning()) {
    DoNotOptimize(GetMonotonicJiffies());
  }
}
BENCHMARK(BM_MonotonicJiffies)->ThreadRange(1, 16);

static void BM_MonotonicMicrosFast(benchmark::State& state) {
  while (state.KeepRunning()) {
    DoNotOptimize(GetMonotonicMicrosFast());
  }
}
BENCHMARK(BM_MonotonicMicrosFast)->ThreadRange(1, 8);


static void BM_ReadLock(benchmark::State& state) {
  pthread_rwlock_t lock;
  CHECK_EQ(0, pthread_rwlock_init(&lock, nullptr));
  CHECK_EQ(0, pthread_rwlock_rdlock(&lock));
  while (state.KeepRunning()) {
    CHECK_EQ(0, pthread_rwlock_rdlock(&lock));
    CHECK_EQ(0, pthread_rwlock_unlock(&lock));
  }
  CHECK_EQ(0, pthread_rwlock_unlock(&lock));
  pthread_rwlock_destroy(&lock);
}
BENCHMARK(BM_ReadLock);

static void BM_WriteLock(benchmark::State& state) {
  pthread_rwlock_t lock;
  CHECK_EQ(0, pthread_rwlock_init(&lock, nullptr));
  while (state.KeepRunning()) {
    CHECK_EQ(0, pthread_rwlock_wrlock(&lock));
    CHECK_EQ(0, pthread_rwlock_unlock(&lock));
  }
  pthread_rwlock_destroy(&lock);
}
BENCHMARK(BM_WriteLock);

static std::mutex m1;
static void BM_StlMutexGuardLock(benchmark::State& state) {
  while (state.KeepRunning()) {
    std::lock_guard<std::mutex> g(m1);
  }
}
BENCHMARK(BM_StlMutexGuardLock)->ThreadRange(1,16);

static std::mutex m2;
static void BM_StlMutexUniqueLock(benchmark::State& state) {
  while (state.KeepRunning()) {
    std::unique_lock<std::mutex> g(m2);
  }
}
BENCHMARK(BM_StlMutexUniqueLock)->ThreadRange(1,16);


static void BM_CallFuncPtr(benchmark::State& state) {
  void (*func_ptr)(void*, void* ) = &MyFunction;

  while (state.KeepRunning()) {
     func_ptr(nullptr, nullptr);
  }
}
BENCHMARK(BM_CallFuncPtr);

static void BM_CallFuncObj(benchmark::State& state) {
  std::function<void()> f = std::bind(MyFunction, nullptr, nullptr);
  while (state.KeepRunning()) {
    f();
  }
}
BENCHMARK(BM_CallFuncObj);

static void BM_CallMemberFunc(benchmark::State& state) {
  Dummy dummy;
  void (*MyFunc)(void* cntx) = &Dummy::CallMethod;

  while (state.KeepRunning()) {
    MyFunc(&dummy);
  }
}
BENCHMARK(BM_CallMemberFunc);

static void BM_CallFuncMembFunc(benchmark::State& state) {
  Dummy dummy;
  std::function<void()> f = std::bind(&Dummy::f, &dummy);

  while (state.KeepRunning()) {
    f();
  }
}
BENCHMARK(BM_CallFuncMembFunc);


static std::mutex m9;
static std::condition_variable cv9;
static bool worker_thread_run = true;

void worker_thread() {
  std::unique_lock<std::mutex> lk(m9);

  while (worker_thread_run) {
    cv9.wait(lk);
  }
}

static void BM_ConditionNotifyOne(benchmark::State& state) {
  std::thread t9(worker_thread);

  while (state.KeepRunning()) {
    std::unique_lock<std::mutex> lk(m9);
    cv9.notify_one();
  }

  {
    std::unique_lock<std::mutex> lk(m9);
    worker_thread_run = false;
    cv9.notify_one();
  }
  t9.join();
}
BENCHMARK(BM_ConditionNotifyOne);

static void BM_PthreadNotifyOne(benchmark::State& state) {
  pthread_cond_t cond_var;
  pthread_cond_init(&cond_var, nullptr);
  pthread_mutex_t my_mutex = PTHREAD_MUTEX_INITIALIZER;

  while (state.KeepRunning()) {
    pthread_mutex_lock(&my_mutex);
    CHECK_EQ(0, pthread_cond_signal(&cond_var));
    pthread_mutex_unlock(&my_mutex);
  }
}
BENCHMARK(BM_PthreadNotifyOne);

static void BM_ReadTimerFd(benchmark::State& state) {
  int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  uint64 missed;
  while (state.KeepRunning()) {
    read(timer_fd, &missed, sizeof (missed));
  }
  close(timer_fd);
}
BENCHMARK(BM_ReadTimerFd);

volatile std::atomic_int val1;
static void BM_IncrementCounterRelaxed(benchmark::State& state) {
  while (state.KeepRunning()) {
    for (int i = 0; i < 10; ++i) {
      val1.fetch_add(1, std::memory_order_relaxed);
    }
  }
}
BENCHMARK(BM_IncrementCounterRelaxed)->ThreadRange(1, 8);

static void BM_IncrementCounterLocal(benchmark::State& state) {
  volatile std::atomic_int val2;

  while (state.KeepRunning()) {
    for (int i = 0; i < 10; ++i) {
      DoNotOptimize(val2.fetch_add(1, std::memory_order_relaxed));
    }
  }
}
BENCHMARK(BM_IncrementCounterLocal)->ThreadRange(1, 8);

static void BM_IncrementCounterLocalInt(benchmark::State& state) {
  volatile int val3 = 0;

  while (state.KeepRunning()) {
    for (int i = 0; i < 10; ++i) {
      DoNotOptimize(++val3);
    }
  }
}
BENCHMARK(BM_IncrementCounterLocalInt)->ThreadRange(1, 8);

// Demonstrate the power of cache.
// Arg(1): runs sz iterations and Arg(2) runs sz/2 iterations but both take the same time.
// This is due to the fact that std::fill fills the cpu cache with garbage and
// Reading from main memory is equally long both for sz and sz/2 numbers as long as it's the same
// amount of cache lines that are loaded.
static void BM_LoopMillionInts(benchmark::State& state) {
  constexpr int sz = 10000000;

  std::unique_ptr<uint32[]> data(new uint32[sz]);
  int divisor = state.range(0);
  int iter_num = sz / divisor;
  std::unique_ptr<uint32[]> cold_cache(new uint32[sz]);
  while (state.KeepRunning()) {
    state.PauseTiming();
    std::fill(cold_cache.get(), cold_cache.get() + sz, 0);
    state.ResumeTiming();

    for (int i = 0; i < iter_num; ++i) {
      data[i*divisor] = i;
    }
  }
}
BENCHMARK(BM_LoopMillionInts)->Arg(1)->Arg(2);


}  // namespace base
