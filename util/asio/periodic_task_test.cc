// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"
#include "util/asio/io_context_pool.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

using namespace std::chrono;

namespace util {
class PeriodicTest : public testing::Test {};

TEST_F(PeriodicTest, BasicTimer) {
  IoContextPool pool;
  auto& cntx = pool.GetNextContext();
  pool.Run();
  unsigned int count = 0;
  {
    std::unique_ptr<PeriodicTask> task(new PeriodicTask(cntx, milliseconds(1)));
    task->Run([&count] { ++count; });
    SleepForMilliseconds(20);
    EXPECT_GT(count, 10);
  }
  pool.Stop();
}

}  // namespace util
