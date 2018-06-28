// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/stats/sliding_counter.h"

#include "base/gtest.h"
#include "base/logging.h"

namespace util {

class SlidingCounterTest : public testing::Test {
};


class SlidingSecondCounterTest : public testing::Test {
};

TEST_F(SlidingSecondCounterTest, Basic) {
  SlidingSecondBase::SetCurrentTime_Test(1);
  SlidingSecondCounter<10,1> second_counter;
  second_counter.Inc();
  second_counter.Inc();
  EXPECT_EQ(2, second_counter.Sum());
  SlidingSecondBase::SetCurrentTime_Test(2);
  EXPECT_EQ(2, second_counter.Sum());
  EXPECT_EQ(0, second_counter.SumLast(0, 1));
  EXPECT_EQ(2, second_counter.SumLast(1, 1));
  EXPECT_EQ(0, second_counter.SumLast(2, 1));
  second_counter.Inc();
  EXPECT_EQ(1, second_counter.SumLast(0, 1));
  EXPECT_EQ(3, second_counter.Sum());

  SlidingSecondBase::SetCurrentTime_Test(11);
  EXPECT_EQ(1, second_counter.Sum());
  SlidingSecondBase::SetCurrentTime_Test(12);
  EXPECT_EQ(0, second_counter.Sum());
  EXPECT_EQ(0, second_counter.DecIfNotLess(1));

  EXPECT_LE(sizeof(SlidingSecondCounter<10,1>), 44);
}


}  // namespace util
