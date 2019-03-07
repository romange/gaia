// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/simple_channel.h"
#include "base/gtest.h"
#include "base/walltime.h"

using namespace boost;

namespace util {
namespace fibers_ext {

class FibersTest : public testing::Test {
 protected:
  void SetUp() final {
  }

  void TearDown() final {
  }
};

TEST_F(FibersTest, SimpleChannel) {
  SimpleChannel<int> channel(10);
  ASSERT_TRUE(channel.TryPush(2));
  channel.Push(4);

  int val = 0;
  ASSERT_TRUE(channel.Pop(val));
  EXPECT_EQ(2, val);
  ASSERT_TRUE(channel.Pop(val));
  EXPECT_EQ(4, val);
}

TEST_F(FibersTest, EventCount) {
  EventCount ec;
  bool signal = false;
  bool fb_exit = false;

  fibers::fiber fb(fibers::launch::dispatch, [&] {
    ec.await([&] { return signal;});
    fb_exit = true;
  });
  ec.notify();
  this_fiber::yield();
  EXPECT_FALSE(fb_exit);

  signal = true;
  ec.notify();
  fb.join();
}

TEST_F(FibersTest, SpuriousNotify) {
  EventCount ec;

  ASSERT_FALSE(ec.notify());
  ASSERT_FALSE(ec.notifyAll());

  int val = 0;

  auto check_positive = [&val]() -> bool { return val > 0; };
  std::thread t1([check_positive, &ec]() { ec.await(check_positive);});

  SleepForMilliseconds(1);
  val = 1;
  ASSERT_TRUE(ec.notify());
  t1.join();
  
  ASSERT_FALSE(ec.notify());
}

}  // namespace fibers_ext
}  // namespace util
