// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <gmock/gmock.h>

#include "mr/pipeline.h"

#include "absl/strings/str_join.h"
#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/io_context_pool.h"

namespace mr3 {

using namespace std;
using namespace util;
using namespace boost;
using testing::Contains;
using testing::ElementsAre;
using testing::Pair;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;

using ShardedOutput = std::unordered_map<ShardId, std::vector<string>>;

void PrintTo(const ShardId& src, std::ostream* os) {
  if (absl::holds_alternative<uint32_t>(src)) {
    *os << absl::get<uint32_t>(src);
  } else {
    *os << absl::get<std::string>(src);
  }
}

class TestContext : public RawContext {
  ShardedOutput& outp_;
  fibers::mutex& mu_;

 public:
  TestContext(ShardedOutput* outp, fibers::mutex* mu) : outp_(*outp), mu_(*mu) {}

  void WriteInternal(const ShardId& shard_id, std::string&& record) {
    std::lock_guard<fibers::mutex> lk(mu_);

    outp_[shard_id].push_back(record);
  }

  void Assign(pb::Output* output) final {};
};

class TestRunner : public Runner {
 public:
  void Init() final;

  void Shutdown() final;

  RawContext* CreateContext() final;

  void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) final;

  // Read file and fill queue. This function must be fiber-friendly.
  void ProcessFile(const std::string& filename, pb::WireFormat::Type type,
                   RecordQueue* queue) final;

  void AddRecords(const string& fl, const std::vector<string>& records) {
    std::copy(records.begin(), records.end(), back_inserter(fs_[fl]));
  }

  const ShardedOutput& out() const { return out_; }

 private:
  std::unordered_map<string, std::vector<string>> fs_;
  ShardedOutput out_;
  fibers::mutex mu_;
};

void TestRunner::Init() {}

void TestRunner::Shutdown() {}

RawContext* TestRunner::CreateContext() { return new TestContext(&out_, &mu_); }

void TestRunner::ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) {
  auto it = fs_.find(glob);
  if (it != fs_.end()) {
    cb(it->first);
  }
}

// Read file and fill queue. This function must be fiber-friendly.
void TestRunner::ProcessFile(const std::string& filename, pb::WireFormat::Type type,
                             RecordQueue* queue) {
  auto it = fs_.find(filename);
  CHECK(it != fs_.end());
  for (const auto& str : it->second) {
    queue->Push(str);
  }
}

class MrTest : public testing::Test {
 protected:
  void SetUp() final {
    pool_.reset(new IoContextPool{1});
    pool_->Run();
  }

  void TearDown() final { pool_.reset(); }

  auto MatchShard(const string& sh_name, const vector<string>& elems) {
    // matcher references the array, so we must to store it in case we pass a temporary rvalue.
    tmp_.push_back(elems);

    return Pair(ShardId{sh_name}, UnorderedElementsAreArray(tmp_.back()));
  }

  std::unique_ptr<IoContextPool> pool_;
  Pipeline pipeline_;
  TestRunner runner_;

  vector<vector<string>> tmp_;
};

TEST_F(MrTest, Basic) {
  EXPECT_EQ(ShardId{1}, ShardId{1});
  EXPECT_NE(ShardId{1}, ShardId{"foo"});

  StringTable str1 = pipeline_.ReadText("read_bar", "bar.txt");
  str1.Write("new_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithSharding([](const std::string& rec) { return "shard1"; });

  vector<string> elements{"1", "2", "3", "4"};

  runner_.AddRecords("bar.txt", elements);
  pipeline_.Run(pool_.get(), &runner_);

  EXPECT_THAT(runner_.out(), ElementsAre(MatchShard("shard1", elements)));
}

TEST_F(MrTest, Json) {
  PTable<rapidjson::Document> json_table = pipeline_.ReadText("read_bar", "bar.txt").AsJson();
  auto json_shard_func = [](const rapidjson::Document& doc) {
    return doc.HasMember("foo") ? "shard0" : "shard1";
  };

  json_table.Write("json_table", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP, 1)
      .WithSharding(json_shard_func);

  const char kJson1[] = R"({"foo":"bar"})";
  const char kJson2[] = R"({"id":1})";
  const char kJson3[] = R"({"foo":null})";

  vector<string> elements{kJson2, kJson1, kJson3};

  runner_.AddRecords("bar.txt", elements);
  pipeline_.Run(pool_.get(), &runner_);
  EXPECT_THAT(runner_.out(), UnorderedElementsAre(MatchShard("shard0", {kJson3, kJson1}),
                                                  MatchShard("shard1", {kJson2})));
}

}  // namespace mr3
