// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/test_utils.h"
#include "base/logging.h"

namespace mr3 {
using namespace std;
using namespace boost;

void PrintTo(const ShardId& src, ostream* os) {
  if (absl::holds_alternative<uint32_t>(src)) {
    *os << absl::get<uint32_t>(src);
  } else {
    *os << absl::get<string>(src);
  }
}

void TestContext::WriteInternal(const ShardId& shard_id, string&& record) {
  lock_guard<fibers::mutex> lk(outp_ss_.mu);
  CHECK(!outp_ss_.is_finished);
  outp_ss_.s_out[shard_id].push_back(record);
}

void TestRunner::Init() {}

void TestRunner::Shutdown() {}

RawContext* TestRunner::CreateContext(const pb::Operator& op) {
  CHECK(!op.output().name().empty());
  last_out_name_= op.output().name();
  auto& res = out_fs_[last_out_name_];
  if (!res)
    res.reset(new OutputShardSet);

  return new TestContext(res.get());
}

void TestRunner::ExpandGlob(const string& glob, function<void(const string&)> cb) {
  auto it = input_fs_.find(glob);
  CHECK(it != input_fs_.end());

  if (it != input_fs_.end()) {
    cb(it->first);
  }
}

void TestRunner::OperatorEnd(ShardFileMap* out_files) {
  auto it = out_fs_.find(last_out_name_);
  CHECK(it != out_fs_.end());
  it->second->is_finished = true;

  for (const auto& k_v : it->second->s_out) {
    string name = last_out_name_ + "/" + k_v.first.ToString("shard");
    out_files->emplace(k_v.first, name);
    input_fs_[name] = k_v.second;
  }
}

// Read file and fill queue. This function must be fiber-friendly.
size_t TestRunner::ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                                    std::function<void(std::string&&)> cb) {
  auto it = input_fs_.find(filename);
  CHECK(it != input_fs_.end());
  for (const auto& str : it->second) {
    cb(string{str});
  }

  return it->second.size();
}

const ShardedOutput& TestRunner::Table(const std::string& tb_name) const {
  auto it = out_fs_.find(tb_name);
  CHECK(it != out_fs_.end()) << "Missing table file " << tb_name;

  return it->second->s_out;
}

}  // namespace mr3
