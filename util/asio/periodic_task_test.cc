// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "util/asio/io_context_pool.h"

using namespace std::chrono;

namespace util {

class PeriodicTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_.Run();
  }

  void TearDown() {
    pool_.Stop();
  }

  IoContextPool pool_;
};

TEST_F(PeriodicTest, BasicTimer) {
  auto& cntx = pool_.GetNextContext();
  unsigned int count = 0;
  {
    std::unique_ptr<PeriodicTask> task(new PeriodicTask(cntx, milliseconds(1)));
    task->Start([&count] { ++count; });
    SleepForMilliseconds(20);
    EXPECT_GT(count, 10);
  }
}

class BlockCheck {
  std::condition_variable cv_;
  bool val_ = false;

 public:

  void Wait() {
    std::mutex m;
    std::unique_lock<std::mutex> l(m);
    cv_.wait(l, [this] { return val_;});
  }

  void Set(bool b) {
    val_ = b;
    if (b) {
      cv_.notify_one();
    }
  }
};

TEST_F(PeriodicTest, Thread) {
  auto& cntx = pool_.GetNextContext();
  std::unique_ptr<PeriodicThreadTask> task(new PeriodicThreadTask(cntx, milliseconds(1)));

  unsigned count = 0;
  BlockCheck bc;

  std::function<void()> f = [&] { ++count; bc.Wait(); };

  for (unsigned i = 0; i < 2; ++i) {
    task->Start(f);
    SleepForMilliseconds(5);
    bc.Set(true);
    task->Cancel();
    bc.Set(false);
    EXPECT_EQ(i+1, count);
  }

}

}  // namespace util
