// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/joiner_executor.h"

#include "base/logging.h"
#include "base/walltime.h"
#include "mr/impl/table_impl.h"
#include "mr/pipeline.h"
#include "mr/runner.h"
#include "util/asio/io_context_pool.h"
#include "util/stats/varz_stats.h"

namespace mr3 {

using namespace boost;
using fibers::channel_op_status;
using namespace util;
using namespace std;

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
  std::unique_ptr<RawContext> raw_context;

  void Shutdown();

  PerIoStruct(unsigned i) : index(i) {}
};

thread_local std::unique_ptr<JoinerExecutor::PerIoStruct> JoinerExecutor::per_io_;

void JoinerExecutor::PerIoStruct::Shutdown() { process_fd.join(); }

JoinerExecutor::JoinerExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {}

JoinerExecutor::~JoinerExecutor() {}

void JoinerExecutor::InitInternal() {}

void JoinerExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  CHECK_EQ(tb->op().type(), pb::Operator::GROUP);
  if (inputs.empty())
    return;
  util::VarzFunction varz_func("joiner", [this] { return GetStats(); });

  CheckInputs(inputs);

  // ProcessInputQ uses runner_ immediately when starts.
  runner_->OperatorStart(&tb->op());

  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    per_io_->raw_context.reset(runner_->CreateContext());
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

  LOG(INFO) << "Started joining on " << tb->op().op_name() << " with " << shard_inputs.size()
            << " shards";
  for (auto& k_v : shard_inputs) {
    VLOG(1) << "Pushing shard " << k_v.first;

    ShardInput si{k_v.first, std::move(k_v.second)};
    channel_op_status st = input_q_.push(std::move(si));
    CHECK_EQ(channel_op_status::success, st);
  }
  input_q_.close();

  pool_->AwaitFiberOnAllSerially([&](IoContext&) {
    per_io_->Shutdown();
    FinalizeContext(per_io_->raw_context.get());
    per_io_.reset();
  });

  const string& op_name = tb->op().op_name();
  LOG_IF(WARNING, parse_errors_ > 0) << op_name << " had " << parse_errors_.load() << " errors";
  for (const auto& k_v : metric_map_) {
    LOG(INFO) << op_name << "-" << k_v.first << ": " << k_v.second;
  }

  runner_->OperatorEnd(out_files);
}

util::VarzValue::Map JoinerExecutor::GetStats() const {
  util::VarzValue::Map res;

  uint64_t latency = 0;
  uint64_t cnt = finish_shard_latency_cnt_.load(std::memory_order_acquire);
  if (cnt) {  // not 100% correct but will do for this.
    latency = finish_shard_latency_sum_.load(std::memory_order_relaxed) / cnt;
  }
  res.emplace_back("finish-latency-usec", util::VarzValue::FromInt(latency));

  auto start = base::GetMonotonicMicrosFast();
  fibers::mutex mu;
  MetricMap metric_map;

  pool_->AwaitFiberOnAll([&, me = shared_from_this()](IoContext& io) {
    VLOG(1) << "JoinerExecutor::GetStats CB";
    auto delta = base::GetMonotonicMicrosFast() - start;
    LOG_IF(INFO, delta > 10000) << "Started late " << delta / 1000 << "ms";

    PerIoStruct* aux_local = per_io_.get();
    if (aux_local) {
      if (aux_local->raw_context) {
        std::lock_guard<fibers::mutex> lk(mu);
        UpdateMetricMap(aux_local->raw_context.get(), &metric_map);
      }
    }
  });

  for (const auto& k_v : metric_map) {
    res.emplace_back(k_v.first, util::VarzValue::FromInt(k_v.second));
  }
  return res;
}

void JoinerExecutor::CheckInputs(const std::vector<const InputBase*>& inputs) {
  uint32_t modn = 0;
  for (const auto& input : inputs) {
    const pb::Output* linked_outp = input->linked_outp();
    CHECK(linked_outp) << input->msg().DebugString();

    if (linked_outp->has_shard_spec()) {
      CHECK_EQ(linked_outp->shard_spec().type(), pb::ShardSpec::MODN);
      if (!modn) {
        modn = linked_outp->shard_spec().modn();
      } else {
        CHECK_EQ(modn, linked_outp->shard_spec().modn());
      }
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
  this_fiber::properties<IoFiberProperties>().set_name("JoinerInputQ");

  // PerIoStruct* trd_local = per_io_.get();
  ShardInput shard_input;
  uint64_t cnt = 0;

  RawContext *raw_context = per_io_->raw_context.get();
  RegisterContext(raw_context);

  std::unique_ptr<detail::HandlerWrapperBase> handler_wrapper{tb->CreateHandler(raw_context)};

  while (true) {
    channel_op_status st = input_q_.pop(shard_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    SetCurrentShard(shard_input.first, raw_context);
    handler_wrapper->SetGroupingShard(shard_input.first);

    VLOG(1) << "Processing shard " << shard_input.first;

    for (const IndexedInput& ii : shard_input.second) {
      CHECK_LT(ii.index, handler_wrapper->Size());
      RawSinkCb emit_cb = handler_wrapper->Get(ii.index);
      bool is_binary = detail::IsBinary(ii.wf->type());

      SetFileName(is_binary, ii.fspec->url_glob(), raw_context);
      SetMetaData(*ii.fspec, raw_context);
      cnt += runner_->ProcessInputFile(ii.fspec->url_glob(), ii.wf->type(), emit_cb);
      raw_context->IncBy("fn-calls", cnt);
    }
    auto start = base::GetMonotonicMicrosFast();
    handler_wrapper->OnShardFinish();
    finish_shard_latency_sum_.fetch_add(base::GetMonotonicMicrosFast() - start,
                                        std::memory_order_relaxed);
    finish_shard_latency_cnt_.fetch_add(1, std::memory_order_acq_rel);
  }
  VLOG(1) << "ProcessInputQ finished after processing " << cnt << " items";
}

}  // namespace mr3
