// Copyright 2020, Beeri 15. All right reserved.
// Author: Ori Brostovski (ori@ubimo.com)

#include "strings/numbers.h"
#include <gtest/gtest.h>

class NumberTest : public testing::Test {};

TEST_F(NumberTest, u64tostr) {
  char buf[65];
  char buf2[2] = {'x', 'x'};

  u64tostr(8765, sizeof(buf2), buf2, 10);
  EXPECT_EQ('8', buf2[0]);
  EXPECT_EQ('\0', buf2[1]);

  u64tostr(0, sizeof(buf), buf, 36);
  EXPECT_EQ(std::string("0"), buf);

  u64tostr((('O' - 'A' + 10) * (36 * 36) +
            ('R' - 'A' + 10) * 36 +
            ('I' - 'A' + 10)),
           sizeof(buf), buf, 36);
  EXPECT_EQ(std::string("ORI"), buf);

  u64tostr(0xFFFFFFFFFFFFFFFF, sizeof(buf), buf, 2);
  EXPECT_EQ(std::string(64, '1'), buf);


}
