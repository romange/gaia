// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr.h"
#include "mr/pipeline.h"

#include "base/logging.h"

namespace mr3 {
using namespace std;

RawContext::~RawContext() {}

void TableBase::SetOutput(const std::string& name, pb::WireFormat::Type type) {
  if (!op_.has_output()) {
    pipeline_->tables_.emplace_back(this);
  }

  auto* out = op_.mutable_output();
  out->set_name(name);
  out->mutable_format()->set_type(type);
}

PTable<rapidjson::Document> StringTable::AsJson() const {
  TableImpl<rapidjson::Document>::PtrType ptr(
      new TableImpl<rapidjson::Document>(impl_->op().op_name(), impl_->pipeline()));

  return PTable<rapidjson::Document>{ptr};
}

}  // namespace mr3
