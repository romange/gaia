// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "strings/strcat.h"
#include <gtest/gtest.h>

class StrCatTest : public testing::Test {
};

TEST_F(StrCatTest, Base) {
  char buf[200] = {0};
  StrAppend(buf, sizeof(buf), {"a", "b", 5, "cdef"});
  EXPECT_STREQ("ab5cdef", buf);
  memset(buf, 'a', 200);
  StrAppend(buf, sizeof(buf), {"foo", 5, "bar"});
  EXPECT_STREQ("foo5bar", buf);
}