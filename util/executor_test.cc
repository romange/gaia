// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/executor.h"

#include <atomic>
#include <thread>

#include <event2/event.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace util {
using namespace std;
using chrono::milliseconds;

class ExecutorTest : public testing::Test {
protected:
};

TEST_F(ExecutorTest, Basic) {
  Executor executor("b1", false);
  atomic_long val(0);
  for (int i = 0; i < 10; ++i) {
    // executor.Add([&val]() { val.fetch_add(1); });
  }
  this_thread::sleep_for(milliseconds(30));
  executor.Shutdown();
  executor.WaitForLoopToExit();
  // EXPECT_EQ(10, val);

  // executor.Add([&val]() { val.fetch_add(20); });  // no op.
  executor.WaitForLoopToExit(); // should be no op.
  this_thread::sleep_for(milliseconds(30));
  // EXPECT_EQ(10, val);
}

TEST_F(ExecutorTest, ManyExecutors) {
  for (int i = 0; i < 1300; ++i) {
    char buf[100];
    sprintf(buf, "b%d", i);
    Executor* executor = new Executor(buf, false);
    executor->Shutdown();
    delete executor;
  }
}

TEST_F(ExecutorTest, Remove) {
  Executor executor("b1");
  atomic_long val(0);
  auto handler = executor.AddPeriodicEvent([&val]() { val.fetch_add(1); }, 1);
  this_thread::sleep_for(milliseconds(50));

  EXPECT_GE(val, 12);
  executor.RemoveHandler(handler);
  long save = val;
  this_thread::sleep_for(milliseconds(20));
  EXPECT_EQ(save, val);
}

static void PeriodicCb(evutil_socket_t fd, short what, void *ptr) {
  CHECK_EQ(EV_TIMEOUT, what);

  unsigned* val = (unsigned*)ptr;
  (*val)++;

  LOG(INFO) << "PeriodicCb";
  this_thread::sleep_for(milliseconds(50));
}

TEST_F(ExecutorTest, EbasePeriodic) {
  Executor executor("b2");
  unsigned val = 0;
  event* timeout = event_new(executor.ebase(), -1, EV_PERSIST | EV_TIMEOUT, &PeriodicCb, &val);
  timeval tv = {0, 10000};  // 10ms
  CHECK_EQ(0, evtimer_add(timeout, &tv));

  LOG(INFO) << "Added";
  this_thread::sleep_for(milliseconds(130));
  LOG(INFO) << "After Sleep";
  event_free(timeout);
  EXPECT_GE(val, 2);
  EXPECT_LT(val, 5);

  executor.Shutdown();
  executor.WaitForLoopToExit();
}


}  // namespace util