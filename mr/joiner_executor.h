// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "mr/operator_executor.h"

namespace mr3 {

class JoinerExecutor  : public OperatorExecutor {
  struct PerIoStruct;

 public:
  JoinerExecutor(util::IoContextPool* pool, Runner* runner);
  ~JoinerExecutor();

  void Init() final;

  void Run(const std::vector<const InputBase*>& inputs,
           detail::TableBase* tb, ShardFileMap* out_files) final;

  // Stops the executor in the middle.
  void Stop() final;

 private:
  void JoinerFiber();
  
  static thread_local std::unique_ptr<PerIoStruct> per_io_;
};

}  // namespace mr3
