// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "absl/strings/str_format.h"
#include "base/logging.h"

#include "mr/operator_executor.h"
#include "mr/pipeline.h"

using namespace boost;
using namespace std;

namespace mr3 {
namespace rj = rapidjson;

namespace detail {

void VerifyUnspecifiedSharding(const pb::Output& outp) {
  CHECK(!outp.has_shard_spec()) << "Output " << outp.name() << " should not have sharding spec";
}

TableBase::~TableBase() {}

void TableBase::SetOutput(const std::string& name, pb::WireFormat::Type type) {
  CHECK(!name.empty());

  if (!op_.has_output()) {
    pipeline_->tables_.emplace_back(shared_from_this());
  }

  auto* out = op_.mutable_output();
  out->set_name(name);
  out->mutable_format()->set_type(type);

  std::unique_ptr<InputBase> ib(new InputBase(name, type, out));
  auto res = pipeline_->inputs_.emplace(name, std::move(ib));
  CHECK(res.second) << "Input '" << name << "' already exists";
}

pb::Operator TableBase::CreateMapOp(const string& name) const {
  pb::Operator new_op = GetDependeeOp();
  new_op.set_op_name(name);
  new_op.set_type(pb::Operator::MAP);

  return new_op;
}

pb::Operator TableBase::GetDependeeOp() const {
  pb::Operator res;

  if (!is_identity_) {
    CHECK(!op_.output().name().empty());
    res.add_input_name(op_.output().name());
  } else {
    res = op_;
    res.clear_output();
  }
  return res;
}

HandlerWrapperBase* TableBase::CreateHandler(RawContext* context) {
  CHECK(defined()) << op_.DebugString();

  return handler_factory_(context);
}

void TableBase::CheckFailIdentity() const { CHECK(defined() && is_identity_); }

void TableBase::ValidateGroupInputOrDie(const TableBase* other) {
  const auto& op = other->op();
  CHECK(!op.output().name().empty()) << "Table '" << op.op_name() << "' does not produce "
    "output. Did you forget to call .Write(..) on it?";
}

}  // namespace detail

namespace {

struct ShardVisitor {
  absl::string_view base;

  ShardVisitor(absl::string_view b) : base(b) {}

  string operator()(const string& id) const { return id; }

  string operator()(uint32_t val) const {
    return absl::StrCat(base, "-", absl::Dec(val, absl::kZeroPad4));
  }

  string operator()(absl::monostate ms) const { return "undefined"; }
};

}  // namespace

RawContext::RawContext() { metric_map_.set_empty_key(StringPiece{}); }

RawContext::~RawContext() {}

FrequencyMap<uint32_t>& RawContext::GetFreqMapStatistic(const std::string& map_id) {
  auto res = freq_maps_.emplace(map_id, nullptr);
  if (res.second) {
    res.first->second.reset(new FrequencyMap<uint32_t>);
  }
  return *res.first->second;
}

const FrequencyMap<uint32_t>* RawContext::FindMaterializedFreqMapStatistic(
    const std::string& map_id) const {
  auto it = CHECK_NOTNULL(finalized_maps_)->find(map_id);
  if (it == finalized_maps_->end())
    return nullptr;
  else
    return it->second.get();
}

std::string ShardId::ToString(absl::string_view basename) const {
  return absl::visit(ShardVisitor{basename}, static_cast<const Parent&>(*this));
}

void OutputBase::SetCompress(pb::Output::CompressType ct, int level) {
  auto* co = out_->mutable_compress();
  co->set_type(ct);
  if (level != -10000) {
    co->set_level(level);
  }
}

void OutputBase::SetShardSpec(pb::ShardSpec::Type st, unsigned modn) {
  CHECK(!out_->has_shard_spec()) << "Must be defined only once. \n" << out_->ShortDebugString();

  out_->mutable_shard_spec()->set_type(st);
  if (st == pb::ShardSpec::MODN) {
    CHECK_GT(modn, 0);
    out_->mutable_shard_spec()->set_modn(modn);
  }
}

void OutputBase::FailUndefinedShard() const {
  LOG(FATAL) << "Sharding function for output " << out_->ShortDebugString() << " is not defined.\n"
             << "Did you forget to call .With<Some>Sharding()?";
}

std::string RecordTraits<rj::Document>::Serialize(bool is_binary, const rj::Document& doc) {
  sb_.Clear();
  rj::Writer<rj::StringBuffer> writer(sb_);
  doc.Accept(writer);

  return string(sb_.GetString(), sb_.GetLength());
}

bool RecordTraits<rj::Document>::Parse(bool is_binary, std::string&& tmp, rj::Document* res) {
  tmp_ = std::move(tmp);

  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  res->ParseInsitu<kFlags>(&tmp_.front());

  bool has_error = res->HasParseError();
  LOG_IF(INFO, has_error) << rj::GetParseError_En(res->GetParseError()) << " for string " << tmp_;
  return !has_error;
}

}  // namespace mr3

ostream& operator<<(ostream& os, const mr3::ShardId& sid) {
  os << sid.ToString("shard");

  return os;
}
