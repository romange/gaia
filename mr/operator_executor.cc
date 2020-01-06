// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/operator_executor.h"

#include "base/logging.h"
#include "base/walltime.h"

#include "util/asio/io_context_pool.h"

namespace mr3 {
using namespace boost;
using namespace std;

OperatorExecutor::PerIoStruct::PerIoStruct(unsigned i) : index(i) {
}

thread_local std::unique_ptr<OperatorExecutor::PerIoStruct> OperatorExecutor::per_io_;

void OperatorExecutor::PerIoStruct::Shutdown() {
  VLOG(1) << "PerIoStruct::ShutdownStart";
  for (auto& f : process_fd)
    f.join();
  VLOG(1) << "PerIoStruct::ShutdownEnd";
}


void OperatorExecutor::RegisterContext(RawContext* context) {
  context->finalized_maps_ = finalized_maps_;
}

void OperatorExecutor::FinalizeContext(RawContext* raw_context) {
  raw_context->Flush();

  UpdateMetricMap(raw_context, &metric_map_);

  // Merge frequency maps. We aggregate counters for all the contexts.
  for (auto& k_v : raw_context->freq_maps_) {
    auto& any = freq_maps_[k_v.first];
    any.Add(k_v.second);
  }
}

void OperatorExecutor::UpdateMetricMap(RawContext *raw_context, MetricMap *metric_map) {
  for (const auto& k_v : raw_context->metric_map_) {
    (*metric_map)[string(k_v.first)] += k_v.second;
  }
}

void OperatorExecutor::Init(const RawContext::FreqMapRegistry& prev_maps) {
  finalized_maps_ = &prev_maps;
  InitInternal();
}

void OperatorExecutor::SetMetaData(const pb::Input::FileSpec& fs, RawContext* context) {
  using FS = pb::Input::FileSpec;
  switch (fs.metadata_case()) {
    case FS::METADATA_NOT_SET:
      context->metadata_.emplace<absl::monostate>();
    break;
    case FS::kStrval:
      context->metadata_ = fs.strval();
    break;
    case FS::kI64Val:
      context->metadata_ = fs.i64val();
    break;
    default:
      LOG(FATAL) << "Invalid file spec tag " << fs.ShortDebugString();
  }
}

util::VarzValue::Map OperatorExecutor::GetStats() {
  util::VarzValue::Map res;

  LOG(INFO) << "MapperExecutor::GetStats";

  auto start = base::GetMonotonicMicrosFast();
  absl::flat_hash_map<string, uint64_t> input_read;

  // TODO: Implement finish-latency-usec, a variable that tells us how much time, on average,
  //       we spent per shard.

  MetricMap metric_map;

  pool_->AwaitFiberOnAllSerially([&, me = shared_from_this()](util::IoContext& io) {
    VLOG(1) << "MapperExecutor::GetStats CB";
    auto delta = base::GetMonotonicMicrosFast() - start;
    LOG_IF(INFO, delta > 10000) << "Started late " << delta / 1000 << "ms";

    PerIoStruct* aux_local = per_io_.get();
    if (aux_local) {
      if (aux_local->raw_context) {
        UpdateMetricMap(aux_local->raw_context.get(), &metric_map);
      }
    }
  });

  res.emplace_back("stats-latency",
                   util::VarzValue::FromInt(base::GetMonotonicMicrosFast() - start));
  for (const auto& k_v : metric_map) {
    res.emplace_back(k_v.first, util::VarzValue::FromInt(k_v.second));
  }
  return res;
}


}  // namespace mr3
