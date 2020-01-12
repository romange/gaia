// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/fiber/unbuffered_channel.hpp>

#include "mr/operator_executor.h"

namespace mr3 {

class JoinerExecutor : public OperatorExecutor {
  struct IndexedInput {
    uint32_t index;
    const pb::Input::FileSpec* fspec;
    const pb::WireFormat* wf;
  };

  using ShardInput = std::pair<ShardId, std::vector<IndexedInput>>;
 public:
  JoinerExecutor(util::IoContextPool* pool, Runner* runner);
  ~JoinerExecutor();

  void Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
           ShardFileMap* out_files) final;

  // Stops the executor in the middle.
  void Stop() final;

 private:
  void InitInternal() final;
  void CheckInputs(const std::vector<const InputBase*>& inputs);

  void ProcessInputQ(detail::TableBase* tb);

  void JoinerFiber();

  ::boost::fibers::unbuffered_channel<ShardInput> input_q_;

  std::atomic<uint64_t> finish_shard_latency_sum_{0}, finish_shard_latency_cnt_{0};
};

}  // namespace mr3
