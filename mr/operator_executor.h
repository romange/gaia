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
  void FinalizeContext(long items_cnt, RawContext* context);

  static void SetFileName(const std::string& file_name, RawContext* context) {
    context->file_name_ = file_name;
  }

  static void SetIsBinary(bool is_binary, RawContext* context) {
    context->is_binary_ = is_binary;
  }

  util::IoContextPool* pool_;
  Runner* runner_;

  ::boost::fibers::mutex mu_;
  absl::flat_hash_map<std::string, long> counter_map_;
  std::atomic<uint64_t> parse_errors_{0};
};

}  // namespace mr3
