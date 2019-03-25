// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr.h"

#include "base/logging.h"

namespace mr3 {
using namespace std;

RawContext::~RawContext() {}

PTable<rapidjson::Document> StringTable::AsJson() const {
  TableImpl<rapidjson::Document>::PtrType ptr(
    new TableImpl<rapidjson::Document>(impl_->op().op_name()));

  return PTable<rapidjson::Document>{ptr};
}

}  // namespace mr3
