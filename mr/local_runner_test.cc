// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/local_runner.h"
#include <gmock/gmock.h>
#include "base/gtest.h"
#include "base/logging.h"
#include "mr/do_context.h"

#include "file/filesource.h"
#include "file/file_util.h"
#include "file/test_util.h"
#include "file/filesource.h"
#include "util/asio/io_context_pool.h"
#include "util/plang/addressbook.pb.h"
#include "util/zlib_source.h"

namespace mr3 {
using namespace util;
using namespace std;

using testing::EndsWith;
using testing::Pair;
using testing::UnorderedElementsAre;
using testing::UnorderedElementsAreArray;
using testing::Key;

class LocalRunnerTest : public testing::Test {
 protected:
  void SetUp() final {
    pool_.reset(new IoContextPool{1});
    pool_->Run();
    runner_.reset(new LocalRunner{pool_.get(), base::GetTestTempDir()});
    runner_->Init();
  }

  void TearDown() final {
    runner_->Shutdown();
    pool_.reset();
  }

  void Start(pb::WireFormat::Type type) {
    op_.set_op_name("op");
    auto* out = op_.mutable_output();
    out->set_name("w1");
    out->mutable_format()->set_type(type);
    runner_->OperatorStart(&op_);
  }

  auto MatchShard(ShardId shard_id, string glob) { return Pair(shard_id, EndsWith(glob)); }

  pb::Operator op_;
  std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<LocalRunner> runner_;
};

static void EnsureComperssedIntegrity(const std::string& str) {
  std::array<unsigned char, 1024> buf;
  std::unique_ptr<file::ReadonlyFile> file(file::ReadonlyFile::Open(str).obj);
  CHECK(file);
  util::ZlibSource src(new file::Source(file.release()));
  StatusObject<size_t> bytes_read;
  do {
    bytes_read = src.Read(strings::MutableByteRange(buf));
    CHECK(bytes_read.ok()) << bytes_read.status;
  } while (bytes_read.obj);
}

const ShardId kShard0{0}, kShard1{1};

TEST_F(LocalRunnerTest, Basic) {
  ShardFileMap out_files;
  Start(pb::WireFormat::TXT);
  std::unique_ptr<RawContext> context{runner_->CreateContext()};
  context->TEST_Write(kShard0, "foo");

  context->Flush();
  runner_->OperatorEnd(&out_files);

  ASSERT_THAT(out_files, UnorderedElementsAre(MatchShard(kShard0, "shard-0000.txt")));

  string shard_name = base::GetTestTempPath("w1/w1-shard-0000.txt");
  string contents;
  ASSERT_TRUE(file_util::ReadFileToString(shard_name, &contents));
  EXPECT_EQ("foo\n", contents);
}

TEST_F(LocalRunnerTest, MaxShardSize) {
  Start(pb::WireFormat::TXT);
  op_.mutable_output()->mutable_compress()->set_type(pb::Output::GZIP);
  op_.mutable_output()->mutable_shard_spec()->set_max_raw_size_mb(1);

  std::unique_ptr<RawContext> context{runner_->CreateContext()};
  std::default_random_engine rd(10);

  for (unsigned i = 0; i < 2000; ++i) {
    string v(1000, 'a');

    for (unsigned j = 0; j < v.size(); ++j) {
      v[j] = rd() % 256;
    }
    context->TEST_Write(kShard0, std::move(v));
  }

  context->Flush();

  ShardFileMap out_files;
  runner_->OperatorEnd(&out_files);
  ASSERT_THAT(out_files, UnorderedElementsAre(MatchShard(kShard0, "shard-0000-*.txt.gz")));
  std::vector<string> expanded;
  runner_->ExpandGlob(out_files.begin()->second,
                      [&](size_t sz, auto& s) { expanded.push_back(s); });
  EXPECT_THAT(expanded, UnorderedElementsAre(EndsWith("shard-0000-000.txt.gz"),
                                             EndsWith("shard-0000-001.txt.gz")));
  for (const string& str : expanded) {
    EnsureComperssedIntegrity(str);
  }
}

TEST_F(LocalRunnerTest, Lst) {
  ShardFileMap out_files;
  Start(pb::WireFormat::LST);
  op_.mutable_output()->set_type_name("tutorial.Address");
  tutorial::Address addr;
  addr.set_street("forrest");

  std::unique_ptr<RawContext> context{runner_->CreateContext()};
  context->TEST_Write(kShard0, addr.SerializeAsString());

  context->Flush();
  runner_->OperatorEnd(&out_files);
  ASSERT_THAT(out_files, UnorderedElementsAre(MatchShard(kShard0, "w1/w1-shard-0000.lst")));
}


TEST_F(LocalRunnerTest, Subdir) {
  ShardFileMap out_files;
  Start(pb::WireFormat::TXT);

  std::unique_ptr<RawContext> context{runner_->CreateContext()};
  const ShardId subdir_shard{"foo/bar/zed"};
  context->TEST_Write(subdir_shard, "zed is dead, baby");

  context->Flush();
  runner_->OperatorEnd(&out_files);
  ASSERT_THAT(out_files, UnorderedElementsAre(MatchShard(subdir_shard, "w1/foo/bar/zed.txt")));
}

auto KeyMatch(const vector<ShardId>& arr) {
  vector<decltype(Key(arr[0]))> res;
  for (const auto& val : arr) {
    res.push_back(Key(val));
  }
  return UnorderedElementsAreArray(res);
};

TEST_F(LocalRunnerTest, CloseShard) {
  ShardFileMap out_files;
  Start(pb::WireFormat::TXT);
  std::unique_ptr<RawContext> context{runner_->CreateContext()};
  context->TEST_Write(kShard0, "foo");
  context->CloseShard(kShard0);
  context->TEST_Write(kShard1, "bar");

  context->Flush();
  runner_->OperatorEnd(&out_files);
  vector<ShardId> shards{kShard0, kShard1};

  ASSERT_THAT(out_files, KeyMatch(shards));
}

using benchmark::DoNotOptimize;

static void BM_ReadTextAndPassIt(benchmark::State& state) {
  string buffer;
  const size_t line_sz = state.range(0);
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(string(line_sz, 'a')).append("\n");
  }
  RawSinkCb cb = [](string&& val) {
    string tmp = std::move(val);
    DoNotOptimize(tmp);
  };

  file::RingSource rs(197, buffer);
  file::LineReader lr(&rs, DO_NOT_TAKE_OWNERSHIP);
  StringPiece line;

  string scratch;
  string record;
  while (state.KeepRunning()) {
    CHECK(lr.Next(&line, &scratch));
    // record.assign(line.data(), line.size());
    string record{line};

    cb(std::move(record));
  }
}
BENCHMARK(BM_ReadTextAndPassIt)->Range(100, 4000);

}  // namespace mr3
