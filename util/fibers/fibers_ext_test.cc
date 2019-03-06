// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/simple_channel.h"
#include "base/gtest.h"

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

}  // namespace fibers_ext
}  // namespace util
