// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/operator_executor.h"

#include "base/logging.h"

namespace mr3 {
using namespace boost;
using namespace std;

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

void OperatorExecutor::UpdateMetricMap(RawContext *raw_context, MetricMap *metric_map) const {
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

}  // namespace mr3
