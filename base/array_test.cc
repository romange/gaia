// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/gtest.h"
#include "base/array.h"

namespace base {

using namespace std;

class NonTrivialCtor {
 public:
  static int count;

  NonTrivialCtor(int i, int j) {
    ++count;
  }
  ~NonTrivialCtor() {
    --count;
  }
};

int NonTrivialCtor::count = 0;
class ArrayTest { };

TEST(ArrayTest, Basic) {
  const array<int, 2> a(42);
  EXPECT_EQ(42, a[0]);
  EXPECT_EQ(42, a[1]);
  static_assert(sizeof(int) * a.size() == sizeof(a), "");

  const array<int, 2> b(int(43));
  for (auto i : b) {
    EXPECT_EQ(43, i);
  }
}

TEST(ArrayTest, String) {
  const array<string, 2> a("Roman");
  EXPECT_EQ("Roman", a[0]);
  EXPECT_EQ("Roman", a[1]);

  const array<string, 2> c(5, 'c');
  for (const auto& s : c) {
    EXPECT_EQ("ccccc", s);
  }
  auto d = c;
  for (const auto& s : d) {
    EXPECT_EQ("ccccc", s);
  }
}

TEST(ArrayTest, Destruct) {
  {
    array<NonTrivialCtor, 5> a(5, 6);
    EXPECT_EQ(5, NonTrivialCtor::count);
  }
  EXPECT_EQ(0, NonTrivialCtor::count);
}

}  // namespace base
