// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr.h"
#include "base/gtest.h"

namespace mr3 {

class MrTest : public testing::Test {
 protected:
  void SetUp() final {}

  void TearDown() final {}
};

TEST_F(MrTest, Basic) {
  Pipeline p;
  StringTable str1 = p.ReadText("foo", "/tmp/bar");
  str1.Write("new_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithSharding([](const std::string& rec) { return "shard1"; });

  PTable<rapidjson::Document> json_table = str1.AsJson();
  auto json_shard_func = [](const rapidjson::Document& doc) {
    return doc.HasMember("foo") ? "shard0" : "shard1";
  };

  json_table.Write("json_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithSharding(json_shard_func);
}

}  // namespace mr3
