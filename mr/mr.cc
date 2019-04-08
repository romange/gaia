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

void detail::TableBase::SetOutput(const std::string& name, pb::WireFormat::Type type) {
  if (!op_.has_output()) {
    pipeline_->tables_.emplace_back(this);
  }

  auto* out = op_.mutable_output();
  out->set_name(name);
  out->mutable_format()->set_type(type);
}

void OutputBase::SetCompress(pb::Output::CompressType ct, unsigned level) {
  auto* co = out_->mutable_compress();
  co->set_type(ct);
  if (level) {
    co->set_level(level);
  }
}

void OutputBase::SetShardType(pb::Output::ShardType st, unsigned modn) {
  CHECK(!out_->has_shard_type()) << "Must be defined only once. \n" << out_->ShortDebugString();

  out_->set_shard_type(st);
  if (st == pb::Output::MODN) {
    CHECK_GT(modn, 0);
    out_->set_modn(modn);
  }
}

std::string RecordTraits<rj::Document>::Serialize(rj::Document&& doc) {
  rj::StringBuffer s;
  rj::Writer<rj::StringBuffer> writer(s);
  doc.Accept(writer);

  return s.GetString();
}

bool RecordTraits<rj::Document>::Parse(std::string&& tmp, rj::Document* res) {
  tmp_ = std::move(tmp);

  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  res->ParseInsitu<kFlags>(&tmp_.front());

  bool has_error = res->HasParseError();
  LOG_IF(INFO, has_error) << rj::GetParseError_En(res->GetParseError()) << " for string " << tmp_;
  return !has_error;
}

}  // namespace mr3
