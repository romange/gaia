// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/joiner_executor.h"

#include "base/logging.h"
#include "mr/impl/table_impl.h"
#include "mr/pipeline.h"
#include "mr/runner.h"
#include "util/asio/io_context_pool.h"

namespace mr3 {

using namespace boost;
using fibers::channel_op_status;
using namespace util;

namespace {

ShardId GetShard(const pb::Input::FileSpec& fspec) {
  if (fspec.has_shard_id())
    return ShardId{fspec.shard_id()};
  return ShardId{fspec.custom_shard_id()};
}

}  // namespace

struct JoinerExecutor::PerIoStruct {
  unsigned index;
  ::boost::fibers::fiber process_fd;

  void Shutdown();

  PerIoStruct(unsigned i) : index(i) {}
};

thread_local std::unique_ptr<JoinerExecutor::PerIoStruct> JoinerExecutor::per_io_;


void JoinerExecutor::PerIoStruct::Shutdown() {
  process_fd.join();
}

JoinerExecutor::JoinerExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {}

JoinerExecutor::~JoinerExecutor() {}

void JoinerExecutor::Init() {}

void JoinerExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  CHECK_EQ(tb->op().type(), pb::Operator::HASH_JOIN);
  if (inputs.empty())
    return;
  CheckInputs(inputs);

  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));

    per_io_->process_fd = fibers::fiber{&JoinerExecutor::ProcessInputQ, this, tb};
  });

  std::map<ShardId, std::vector<IndexedInput>> shard_inputs;
  for (uint32_t i = 0; i < inputs.size(); ++i) {
    const pb::Input& input = inputs[i]->msg();
    for (const auto& fspec : input.file_spec()) {
      ShardId sid = GetShard(fspec);
      shard_inputs[sid].emplace_back(IndexedInput{i, &fspec, &input.format()});
    }
  }
  runner_->OperatorStart();

  for (auto& k_v : shard_inputs) {
    VLOG(1) << "Pushing shard " << k_v.first;

    ShardInput si{k_v.first, std::move(k_v.second)};
    channel_op_status st = input_q_.push(std::move(si));
    CHECK_EQ(channel_op_status::success, st);
  }
  input_q_.close();

  pool_->AwaitFiberOnAll([&](IoContext&) {
    per_io_->Shutdown();
    per_io_.reset();
  });

  runner_->OperatorEnd(out_files);
}

void JoinerExecutor::CheckInputs(const std::vector<const InputBase*>& inputs) {
  uint32_t modn = 0;
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
}

// Stops the executor in the middle.
void JoinerExecutor::Stop() {}

void JoinerExecutor::JoinerFiber() {}

void JoinerExecutor::ProcessInputQ(detail::TableBase* tb) {
  // PerIoStruct* trd_local = per_io_.get();
  ShardInput shard_input;
  uint64_t cnt = 0;

  std::unique_ptr<RawContext> raw_context(runner_->CreateContext(tb->op()));
  std::unique_ptr<detail::HandlerWrapperBase> handler{tb->CreateHandler(raw_context.get())};

  while (true) {
    channel_op_status st = input_q_.pop(shard_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    handler->SetOutputShard(shard_input.first);
    VLOG(1) << "Processing shard " << shard_input.first;

    for (const IndexedInput& ii : shard_input.second) {
      CHECK_LT(ii.index, handler->Size());
      auto emit_cb = handler->Get(ii.index);

      cnt += runner_->ProcessInputFile(ii.fspec->url_glob(), ii.wf->type(),
        emit_cb);
      // TODO: Mark end of input
    }
  }
  VLOG(1) << "ProcessInputQ finished after processing " << cnt << " items";
}

}  // namespace mr3
