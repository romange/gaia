// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/joiner_executor.h"

#include "base/logging.h"
#include "mr/impl/table_impl.h"

namespace mr3 {

using namespace boost;

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
  if (inputs.empty())
    return;

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

  struct IndexedInput {
    uint32_t index;
    const pb::Input::FileSpec* fspec;
    const pb::WireFormat* wf;
  };

  std::map<ShardId, std::vector<IndexedInput>> shard_inputs;
  for (uint32_t i = 0; i < inputs.size(); ++i) {
    const pb::Input& input = inputs[i]->msg();
    for (const auto& fspec : input.file_spec()) {
      ShardId sid = GetShard(fspec);
      shard_inputs[sid].emplace_back(IndexedInput{i, &fspec, &input.format()});
    }
  }
  std::unique_ptr<RawContext> do_context{runner_->CreateContext(tb->op())};
  RecordQueue record_q(256);
  runner_->OperatorStart();

  fibers::fiber join_fiber(&JoinerExecutor::JoinerFiber, this);

  for (const auto& k_v : shard_inputs) {
    for (const IndexedInput& ii : k_v.second) {
      runner_->ProcessInputFile(ii.fspec->url_glob(), ii.wf->type(), &record_q);
      // TODO: Mark end of input
    }
    // TODO: finalize shard.
  }
  join_fiber.join();
  runner_->OperatorEnd(out_files);
}

// Stops the executor in the middle.
void JoinerExecutor::Stop() {}

void JoinerExecutor::JoinerFiber() {

}

}  // namespace mr3
