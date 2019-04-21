// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <boost/fiber/mutex.hpp>

#include "absl/container/flat_hash_map.h"
#include "mr/pipeline.h"
#include "mr/runner.h"

namespace other {
struct StrVal {
  std::string val;
};

}  // namespace other

namespace mr3 {

// For gcc less than 7 we should enclose the specialization into the original namespace.
// https://stackoverflow.com/questions/25594644/warning-specialization-of-template-in-different-namespace
template <> class RecordTraits<other::StrVal> {
 public:
  static std::string Serialize(other::StrVal&& doc) { return std::move(doc.val); }

  bool Parse(std::string&& tmp, other::StrVal* res) {
    res->val = tmp;
    return true;
  }
};

void PrintTo(const ShardId& src, std::ostream* os);

using ShardedOutput = std::unordered_map<ShardId, std::vector<std::string>>;

struct OutputShardSet {
  ShardedOutput s_out;
  bool is_finished = false;
  ::boost::fibers::mutex mu;
};

class TestContext : public RawContext {
  OutputShardSet& outp_ss_;

 public:
  TestContext(OutputShardSet* outp) : outp_ss_(*outp) {}

  void WriteInternal(const ShardId& shard_id, std::string&& record);
};

class TestRunner : public Runner {
 public:
  void Init() final;

  void Shutdown() final;

  RawContext* CreateContext(const pb::Operator& op) final;

  void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) final;

  // Read file and fill queue. This function must be fiber-friendly.
  size_t ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                          std::function<void(std::string&&)> cb) final;

  void OperatorStart() final {}
  void OperatorEnd(ShardFileMap* out_files) final;

  void AddInputRecords(const std::string& fl, const std::vector<std::string>& records) {
    std::copy(records.begin(), records.end(), std::back_inserter(input_fs_[fl]));
  }

  const ShardedOutput& Table(const std::string& tb_name) const;

 private:
  absl::flat_hash_map<std::string, std::vector<std::string>> input_fs_;
  absl::flat_hash_map<std::string, std::unique_ptr<OutputShardSet>> out_fs_;
  std::string last_out_name_;
};

class EmptyRunner : public Runner {
 public:
  std::function<bool(std::string* val)> gen_fn;

  class Context : public RawContext {
    public:
    void WriteInternal(const ShardId& shard_id, std::string&& record) {}
  };

  void Init() final {}

  void Shutdown() final {}

  RawContext* CreateContext(const pb::Operator& op) final { return new Context; }

  void ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) final {
    cb(glob);
  }

  void OperatorStart() final {}
  void OperatorEnd(ShardFileMap* out_files) final  {}

  size_t ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                          std::function<void(std::string&&)> cb) final;
};

}  // namespace mr3
