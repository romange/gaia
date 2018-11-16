// Copyright 2015, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <gmock/gmock.h>

#include "base/logging.h"

#include "util/sp_task_pool.h"
#include "base/gtest.h"

using testing::ElementsAre;
using std::vector;
using std::string;

namespace util {

namespace {

enum Op {MOVE_CTOR,CPY_CTOR, MOVE_ARG,
          COPY_ASSIGN,
          MOVE_ASSIGN,
          CONST_ARG};

struct CheckCtorRules {
  std::vector<Op> ops;

  CheckCtorRules() {}
  CheckCtorRules(const CheckCtorRules& c) : ops(c.ops)  {
    ops.push_back(CPY_CTOR);
  }

  CheckCtorRules(CheckCtorRules&& c) : ops(c.ops) {
    ops.push_back(MOVE_CTOR);
  }

  CheckCtorRules(int&& c) {
    ops.push_back(MOVE_ARG);
  }

  CheckCtorRules(const int& c) {
    ops.push_back(CONST_ARG);
  }


  CheckCtorRules& operator=(const CheckCtorRules& o) {
    ops = o.ops;
    ops.push_back(COPY_ASSIGN);
    return *this;
  }

  CheckCtorRules& operator=(CheckCtorRules&& o) {
    ops = o.ops;
    ops.push_back(MOVE_ASSIGN);
    return *this;
  }
};

struct Task {
  typedef std::vector<Op>* SharedData;
  SharedData shared = nullptr;

  void operator()(const CheckCtorRules& r) {
    *shared = r.ops;
  }

  void InitShared(const SharedData& s) {
    shared = s;
  }
};

class SPTaskPoolTest : public testing::Test {
protected:
  static void SetUpTestCase() {
  }
};

}  // namespace


TEST_F(SPTaskPoolTest, Queue) {
  folly::ProducerConsumerQueue<CheckCtorRules> q(3);
  CheckCtorRules obj;
  ASSERT_TRUE(q.write(obj));
  ASSERT_FALSE(q.isEmpty());

  CheckCtorRules* p1 = q.frontPtr();

  EXPECT_THAT(p1->ops, ElementsAre(CPY_CTOR));

  CheckCtorRules dest;
  ASSERT_TRUE(q.read(dest));
  EXPECT_THAT(dest.ops, ElementsAre(CPY_CTOR, MOVE_ASSIGN));


  ASSERT_TRUE(q.write(CheckCtorRules()));
  p1 = q.frontPtr();
  EXPECT_THAT(p1->ops, ElementsAre(MOVE_CTOR));

  ASSERT_TRUE(q.read(dest));
  EXPECT_THAT(dest.ops, ElementsAre(MOVE_CTOR, MOVE_ASSIGN));


  ASSERT_TRUE(q.write(5));
  p1 = q.frontPtr();
  EXPECT_THAT(p1->ops, ElementsAre(MOVE_ARG));
  ASSERT_TRUE(q.read(dest));

  int i = 5;
  ASSERT_TRUE(q.write(i));
  p1 = q.frontPtr();
  EXPECT_THAT(p1->ops, ElementsAre(CONST_ARG));

  ASSERT_TRUE(q.read(dest));
  EXPECT_THAT(dest.ops, ElementsAre(CONST_ARG, MOVE_ASSIGN));
}

TEST_F(SPTaskPoolTest, Basic) {
  SingleProducerTaskPool<Task, CheckCtorRules> pool("test", 2, 1);
  vector<Op> result;
  pool.SetSharedData(&result);
  pool.Launch();
  pool.RunInline(5);
  EXPECT_THAT(result, ElementsAre(MOVE_ARG));

  pool.RunTask(CheckCtorRules());
  pool.WaitForTasksToComplete();
  EXPECT_THAT(result, ElementsAre(MOVE_CTOR, MOVE_ASSIGN));
}

struct TaskNoShared {
  unsigned sleep;


  void operator()(std::pair<int, string> p) {
    if (sleep) base::SleepMicros(sleep);
  }
  TaskNoShared(unsigned s = 0) : sleep(s) {}
};

typedef SingleProducerTaskPool<TaskNoShared, std::pair<int, string>> TaskNoSharedPool;

TEST_F(SPTaskPoolTest, NoShared) {
  TaskNoSharedPool pool("test", 2);
  pool.Launch(1000);

  EXPECT_EQ(0, pool.QueueSize());

  for (int i = 0; i < 16; ++i)
    pool.RunTask(5, "bar");

  // Assuming that the thread was not preempted and waited for thread pool to run all its tasks.
  // EXPECT_GT(pool.QueueSize(), 0);
  pool.WaitForTasksToComplete();
}

TEST_F(SPTaskPoolTest, JoinThreads) {
  for (int i = 0; i < 2500; ++i) {
    TaskNoSharedPool pool("tstjoin", 2);
    pool.Launch();
  }
}

struct TaskFinalize {
  TaskFinalize(int* cnt) : cnt_(cnt) {}

  void operator()(int work) {
  }

  void Finalize() {
    (*cnt_)++;
  }

  int* cnt_;
};

TEST_F(SPTaskPoolTest, Finalize) {
  SingleProducerTaskPool<TaskFinalize, int> pool("finalize", 2);
  int count = 0;

  pool.Launch(&count);
  pool.WaitForTasksToComplete();
  pool.Finalize();
  EXPECT_GT(count, 0);
}

struct NoOpTask {
  void operator()(int ) {
  }
};

typedef SingleProducerTaskPool<NoOpTask, int> NoOpPool;

static void BM_PoolRun(benchmark::State& state) {
  NoOpPool pool("test", 40, state.range(0));
  pool.Launch();
  volatile int val = 10;

  while (state.KeepRunning()) {
    pool.RunTask(val);
  }

  pool.WaitForTasksToComplete();
}
BENCHMARK(BM_PoolRun)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16);


static void BM_PoolTryRun(benchmark::State& state) {
  TaskNoSharedPool pool("test", 40);
  pool.Launch();

  while (state.KeepRunning()) {
    if (!pool.TryRunTask(10, "foo")) {
      pool.WaitForTasksToComplete();
    }
  }
}
BENCHMARK(BM_PoolTryRun);

}  // namespace util
