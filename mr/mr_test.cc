// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"
#include "mr/mr.h"

namespace mr3 {

class MrTest : public testing::Test {
 protected:
  void SetUp() final {}

  void TearDown() final {}
};

TEST_F(MrTest, Basic) {
  Pipeline p;
  StringStream str1 = p.ReadText("foo", "/tmp/bar");
  str1.Write("new_table", pb::WireFormat::TXT).AndCompress(pb::Output::GZIP, 1)
      .WithSharding([](const std::string& rec) { return "shard1"; });
}

}  // namespace mr3
