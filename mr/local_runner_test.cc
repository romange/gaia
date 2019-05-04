// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <gmock/gmock.h>
#include "base/gtest.h"

#include "mr/do_context.h"
#include "mr/local_runner.h"

#include "file/file_util.h"
#include "util/asio/io_context_pool.h"

namespace mr3 {
using namespace util;
using namespace std;

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

  pb::Operator op_;
  std::unique_ptr<IoContextPool> pool_;
  std::unique_ptr<LocalRunner> runner_;
};

TEST_F(LocalRunnerTest, Basic) {
  ShardFileMap out_files;
  Start(pb::WireFormat::TXT);
  RawContext* context = runner_->CreateContext();
  context->TEST_Write(ShardId{0}, "foo");

  context->Flush();
  runner_->OperatorEnd(&out_files);
  string shard_name = base::GetTestTempPath("w1/w1-shard-0000.txt");
  string contents;
  ASSERT_TRUE(file_util::ReadFileToString(shard_name, &contents));
  EXPECT_EQ("foo\n", contents);
}

}  // namespace mr3
