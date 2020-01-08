// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/mutex.hpp>

#include "mr/impl/table_impl.h"
#include "mr/runner.h"

namespace util {
class IoContextPool;
}  // namespace util

namespace mr3 {
class InputBase;


/*! \brief Base class for operator executors.
    \author Roman Gershman

    OperatorExecutor derives from std::enable_shared_from_this<> to allow flexible
    ownership semantics when passing its objects between asynchronous callbacks.
*/
class OperatorExecutor : public std::enable_shared_from_this<OperatorExecutor> {
 public:
  OperatorExecutor(util::IoContextPool* pool, Runner* runner)
    : pool_(pool), runner_(runner) {}

  virtual ~OperatorExecutor() {}

  void Init(const RawContext::FreqMapRegistry& prev_maps);

  virtual void Run(const std::vector<const InputBase*>& inputs,
                   detail::TableBase* ss, ShardFileMap* out_files) = 0;

  // Stops the executor in the middle.
  virtual void Stop() = 0;

  const RawContext::FreqMapRegistry& GetFreqMaps() const { return freq_maps_; }
  const std::map<std::string, long>& GetCounterMap() const { return metric_map_; }

protected:
  void RegisterContext(RawContext* context);

  /// Called from all IO threads once they finished running the operator.
  void FinalizeContext(RawContext* context);

  static void SetFileName(bool is_binary, const std::string& file_name, RawContext* context) {
    context->is_binary_ = is_binary;
    context->file_name_ = file_name;
  }

  static void SetMetaData(const pb::Input::FileSpec& fs, RawContext* context);

  static void SetPosition(size_t pos, RawContext* context) {
    context->input_pos_ = pos;
  }

  static void SetCurrentShard(ShardId shard, RawContext* context) {
    context->current_shard_ = std::move(shard);
  }

  virtual void InitInternal() = 0;

  util::IoContextPool* pool_;
  Runner* runner_;

  /// I keep it as std::map to print counters in lexicographic order.
  /// Performance is negligible since it's used only for final aggregation.
  std::map<std::string, long> metric_map_;
  std::atomic<uint64_t> parse_errors_{0};

  RawContext::FreqMapRegistry freq_maps_;
  const RawContext::FreqMapRegistry* finalized_maps_;
};

}  // namespace mr3
