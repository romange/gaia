// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/joiner_executor.h"

#include "base/logging.h"
#include "mr/impl/table_impl.h"

namespace mr3 {

namespace {

ShardId GetShard(const pb::Input::FileSpec& fspec) {
  if (fspec.has_shard_id())
    return ShardId{fspec.shard_id()};
  return ShardId{fspec.custom_shard_id()};
}

}  // namespace

JoinerExecutor::JoinerExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {}
JoinerExecutor::~JoinerExecutor() {}

void JoinerExecutor::Init() {}

void JoinerExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  CHECK_EQ(tb->op().type(), pb::Operator::HASH_JOIN);
  CHECK_GT(inputs.size(), 1);
  uint32 modn = 0;
  for (const auto& input : inputs) {
    const pb::Output* linked_outp = input->linked_outp();
    CHECK(linked_outp && linked_outp->has_shard_spec());
    CHECK_EQ(linked_outp->shard_spec().type(), pb::ShardSpec::MODN);
    if (!modn) {
      modn = linked_outp->shard_spec().modn();
    } else {
      CHECK_EQ(modn, linked_outp->shard_spec().modn());
    }

    for (const auto& fspec : input->msg().file_spec()) {
      CHECK_GT(fspec.shard_id_ref_case(), 0);  // all inputs have sharding info.
    }
  }

  using IndexedInput = std::pair<uint32_t, const pb::Input::FileSpec*>;
  absl::flat_hash_map<ShardId, std::vector<IndexedInput>> joined_shards;
  for (size_t i = 0; i < inputs.size(); ++i) {
    for (const auto& fspec : inputs[i]->msg().file_spec()) {
      ShardId sid = GetShard(fspec);
      joined_shards[sid].emplace_back(i, &fspec);
    }
  }
}

// Stops the executor in the middle.
void JoinerExecutor::Stop() {}

}  // namespace mr3
