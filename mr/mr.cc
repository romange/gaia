// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr.h"

#include <rapidjson/error/en.h>

#include "mr/pipeline.h"

#include "base/logging.h"

namespace mr3 {
using namespace std;
namespace rj = rapidjson;

RawContext::~RawContext() {}

void TableBase::SetOutput(const std::string& name, pb::WireFormat::Type type) {
  if (!op_.has_output()) {
    pipeline_->tables_.emplace_back(this);
  }

  auto* out = op_.mutable_output();
  out->set_name(name);
  out->mutable_format()->set_type(type);
}

PTable<rj::Document> StringTable::AsJson() const {
  pb::Operator new_op = impl_->op();
  new_op.mutable_op_name()->append("_as_json");

  TableImpl<rj::Document>::PtrType ptr(
      new TableImpl<rj::Document>(std::move(new_op), impl_->pipeline()));

  return PTable<rj::Document>{ptr};
}

std::string RecordTraits<rj::Document>::Serialize(rj::Document&& doc) {
  rj::StringBuffer s;
  rj::Writer<rj::StringBuffer> writer(s);
  doc.Accept(writer);

  return s.GetString();
}

rj::Document RecordTraits<rj::Document>::Parse(std::string&& tmp) {
  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.Parse<kFlags>(tmp.c_str(), tmp.size());

  CHECK(!doc.HasParseError()) << rj::GetParseError_En(doc.GetParseError()) << " for string " << tmp;

  return doc;
}

}  // namespace mr3
