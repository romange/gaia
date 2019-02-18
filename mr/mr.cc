// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr.h"

#include "base/logging.h"

namespace mr3 {

const InputBase& Pipeline::input(const std::string& name) const {
  for (const auto& ptr : inputs_) {
    if (ptr->msg().name() == name) {
      return *ptr;
    }
  }
  LOG(FATAL) << "Could not find " << name;
  return *inputs_.front();
}

ExecutionOutputContext::ExecutionOutputContext(OutputBase* ob) : ob_(ob) {
  const pb::Output& out = ob->msg();
  // TBD
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
  
}

void ExecutionOutputContext::WriteRecord(const std::string& record) {

}

}  // namespace mr3
