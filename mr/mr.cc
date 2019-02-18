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

ExecutionOutputContext::ExecutionOutputContext(const std::string& root_dir, OutputBase* ob)
    : root_dir_(root_dir), ob_(ob) {
  files_.set_empty_key(StringPiece());

  const pb::Output& out = ob->msg();
  // TBD
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
}

/*void ExecutionOutputContext::WriteRecord(std::string&& record) {
  OutputBase::RecordResult rec_result;

  ob_->WriteInternal(std::move(record), &rec_result);
  CHECK(!rec_result.file_key.empty());

  LOG(FATAL) << "TODO: open a new file file_key";
}
*/

}  // namespace mr3
