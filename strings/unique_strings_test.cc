// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "strings/unique_strings.h"
#include <gtest/gtest.h>

using std::string;

class UniqueStringsTest : public testing::Test {
};

TEST_F(UniqueStringsTest, Base) {
  UniqueStrings unique;
  StringPiece foo1 = unique.Get("foo");
  string str("foo");
  StringPiece foo2 = unique.Get(str);

  EXPECT_EQ(foo1, foo2);
  EXPECT_EQ(foo1.data(), foo2.data());
  StringPiece bar = unique.Get("bar");
  EXPECT_NE(bar, foo2);
}

TEST_F(UniqueStringsTest, DenseMap) {
  StringPieceDenseMap<int> unique;
  unique.set_empty_key(StringPiece());
  unique["r1"] = 1;
  unique["r2"] = 2;
  unique.insert(StringPieceDenseMap<int>::value_type("r3", 3));
  auto it = unique.find("r1");
  EXPECT_TRUE(it != unique.end());
  EXPECT_EQ(1, it->second);
  EXPECT_EQ("r1", it->first);

  unique["r1"]++;
  it = unique.find("r1");
  EXPECT_TRUE(it != unique.end());
  EXPECT_EQ(2, it->second);
  EXPECT_EQ(2, unique["r1"]);

  EXPECT_EQ(3, unique["r3"]);
}
