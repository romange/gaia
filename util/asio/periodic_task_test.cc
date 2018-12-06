// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/periodic_task.h"

#include <thread>
#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "util/asio/io_context_pool.h"

using namespace std;
using namespace chrono;
using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;

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
  IoContext& cntx = pool_.GetNextContext();
  unsigned int count = 0;
  {
    std::unique_ptr<PeriodicTask> task(new PeriodicTask(cntx, milliseconds(1)));
    task->Start([&count](int) { ++count; });
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

class NonMoveable {
public:
 NonMoveable() {}
 NonMoveable(const NonMoveable&) noexcept = default;
 NonMoveable(NonMoveable&& ) = delete;
 void operator=(NonMoveable&& ) = delete;
};

TEST_F(PeriodicTest, Cpp) {
  auto& cntx = pool_.GetNextContext();
  thread::id main_id = this_thread::get_id();
  std::unique_ptr<PeriodicTask> task(new PeriodicTask(cntx, milliseconds(1)));

  BlockCheck bc;
  NonMoveable nm;
  thread::id task_thread_id;

  // non-moveable
  task->Start([nm, &bc, &task_thread_id] (int) {
    task_thread_id = std::this_thread::get_id();
    bc.Set(true);
  });
  bc.Wait();

  EXPECT_NE(thread::id{}, task_thread_id);
  EXPECT_NE(main_id, task_thread_id);

  bc.Set(false);
  task->Cancel();

  LOG(INFO) << "Moveable";
  std::unique_ptr<int> pi;
  task->Start([pi = std::move(pi), &bc, &task_thread_id] (int) {
    task_thread_id = std::this_thread::get_id();
    bc.Set(true);
    VLOG(1) << "Cb run";
  });
  bc.Wait();
  task.reset();
}

TEST_F(PeriodicTest, Thread) {
  auto& cntx = pool_.GetNextContext();
  std::unique_ptr<PeriodicWorkerTask> task(new PeriodicWorkerTask(cntx, milliseconds(1)));

  unsigned count = 0;
  BlockCheck bc;

  std::function<void()> f = [&] { ++count; bc.Wait(); };

  CaptureStderr();
  for (unsigned i = 0; i < 2; ++i) {
    task->Start(f);
    SleepForMilliseconds(25);
    EXPECT_TRUE(task->IsHanging());
    bc.Set(true);
    task->Cancel();
    bc.Set(false);
    EXPECT_EQ(i+1, count);
    EXPECT_FALSE(task->IsHanging());
  }
  string err = GetCapturedStderr();
  EXPECT_FALSE(err.empty());
}

}  // namespace util
