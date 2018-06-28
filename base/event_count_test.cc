/*
 * Copyright 2015 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/event_count.h"

#include <algorithm>
#include <atomic>
#include <random>
#include <thread>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

using namespace folly;

namespace {

class Semaphore {
 public:
  explicit Semaphore(int v=0) : value_(v) { }

  void down() {
    ec_.await([this] { return tryDown(); });
  }

  void up() {
    ++value_;
    ec_.notifyAll();
  }

  int value() const {
    return value_;
  }

 private:
  bool tryDown() {
    for (int v = value_; v != 0;) {
      if (value_.compare_exchange_weak(v, v-1)) {
        return true;
      }
    }
    return false;
  }

  std::atomic<int> value_;
  EventCount ec_;
};

template <class T, class Random>
void randomPartition(Random& random, T key, int n,
                     std::vector<std::pair<T, int>>& out) {
  while (n != 0) {
    int m = std::min(n, 1000);
    std::uniform_int_distribution<uint32_t> u(1, m);
    int cut = u(random);
    out.emplace_back(key, cut);
    n -= cut;
  }
}

class EventCountTest : public testing::Test {
  virtual void SetUp() override {
  }

  virtual void TearDown() override {
    if (t1_ && t1_->joinable()) {
      t1_->join();
    }
  }
protected:
  std::unique_ptr<std::thread> t1_;
};

}  // namespace


TEST_F(EventCountTest, Simple) {
  // We're basically testing for no deadlock.
  static constexpr size_t count = 300000;

  enum class Op {
    UP,
    DOWN
  };
  std::vector<std::pair<Op, int>> ops;
  std::mt19937 rnd(10);
  randomPartition(rnd, Op::UP, count, ops);
  size_t uppers = ops.size();
  randomPartition(rnd, Op::DOWN, count, ops);
  size_t downers = ops.size() - uppers;
  VLOG(1) << "Using " << ops.size() << " threads: uppers=" << uppers
          << " downers=" << downers << " sem_count=" << count;

  std::random_shuffle(ops.begin(), ops.end());

  std::vector<std::thread> threads;
  threads.reserve(ops.size());

  Semaphore sem;
  for (auto& op : ops) {
    int n = op.second;
    if (op.first == Op::UP) {
      auto fn = [&sem, n] () mutable {
        while (n--) {
          sem.up();
        }
      };
      threads.push_back(std::thread(fn));
    } else {
      auto fn = [&sem, n] () mutable {
        while (n--) {
          sem.down();
        }
      };
      threads.push_back(std::thread(fn));
    }
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(0, sem.value());
}


TEST_F(EventCountTest, SpuriousNotify) {
  EventCount ec;

  ASSERT_FALSE(ec.notify());
  ASSERT_FALSE(ec.notifyAll());

  int val = 0;

  auto check_positive = [&val]() -> bool { return val > 0; };
  t1_.reset(new std::thread([check_positive, &ec]() {ec.await(check_positive);}));
  SleepForMilliseconds(1);
  val = 1;
  ASSERT_TRUE(ec.notify());
  t1_->join();
  ASSERT_FALSE(ec.notify());
}

TEST_F(EventCountTest, WaitTimedOut) {
  EventCount ec;

  MicrosecondsInt64 now = GetMonotonicMicros();
  EventCount::Key key = ec.prepareWait();
  ASSERT_FALSE(ec.wait(key, now, now + base::kNumMicrosPerMilli*20));

  now = GetMonotonicMicros();
  key = ec.prepareWait();
  ASSERT_FALSE(ec.wait(key, now, now + base::kNumMicrosPerMilli*20));
  ASSERT_FALSE(ec.wait(key, 0, now + base::kNumMicrosPerMilli*20));

  int val = 0;
  auto check_positive = [&val]() -> bool { return val > 0; };
  ASSERT_FALSE(ec.await(check_positive, base::kNumMicrosPerMilli));
  ASSERT_FALSE(ec.await(check_positive, base::kNumMicrosPerMilli));
  ASSERT_FALSE(ec.await(check_positive, base::kNumMicrosPerMilli));
  ASSERT_FALSE(ec.notify());

  bool res = false;
  t1_.reset(new std::thread([check_positive, &ec, &res]() {
    res = ec.await(check_positive, base::kNumMicrosPerMilli*100);
  }));

  SleepForMilliseconds(25);
  val = 1;
  ASSERT_TRUE(ec.notify());
  t1_->join();
  ASSERT_FALSE(ec.notify());
  ASSERT_TRUE(res);
}

TEST_F(EventCountTest, Futex) {
  int test_val = 1;
  timespec ts;
  ts.tv_sec = 0;
  ts.tv_nsec = 1000000;  // 1ms

  MicrosecondsInt64 start = GetMonotonicMicros();
  int res = detail::futex(&test_val, FUTEX_WAIT_PRIVATE, 1, &ts, nullptr, 0);

  EXPECT_EQ(-1, res);
  EXPECT_EQ(ETIMEDOUT, errno) << strerror(errno);
  MicrosecondsInt64 end = GetMonotonicMicros();
  EXPECT_GT(end - start, 1000);
  EXPECT_LT(end - start, 2000);
}

