// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "strings/strpmr.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/integral_types.h"
#include "base/macros.h"

using testing::ContainerEq;

namespace strings {


class StrPmrTest : public testing::Test {
 public:
  StrPmrTest() {
  }
 protected:
};


TEST_F(StrPmrTest, Basic) {
  ArraysMap<int, unsigned> am(pmr::get_default_resource());

  std::vector<int> v(3);

  Range<const int*> r1(v.data(), v.size());

  am.emplace(r1, 5);
  auto it = am.begin();
  EXPECT_EQ(it->first, r1);
}


}  // namespace strings

