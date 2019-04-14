// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "mr/impl/table_impl.h"
#include "mr/runner.h"

namespace util {
class IoContextPool;
}  // namespace util

namespace mr3 {
class InputBase;

class OperatorExecutor {
 public:
  OperatorExecutor(util::IoContextPool* pool, Runner* runner)
    : pool_(pool), runner_(runner) {}

  virtual ~OperatorExecutor() {}

  virtual void Init() = 0;

  virtual void Run(const std::vector<const InputBase*>& inputs,
                   detail::TableBase* ss, ShardFileMap* out_files) = 0;

  // Stops the executor in the middle.
  virtual void Stop() = 0;
 protected:
  util::IoContextPool* pool_;
  Runner* runner_;
};

}  // namespace mr3
