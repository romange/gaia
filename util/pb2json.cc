// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pb2json.h"

#include <google/protobuf/reflection.h>
#include <google/protobuf/repeated_field.h>
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

#include "absl/container/inlined_vector.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"

#include "base/logging.h"
#include "util/pb/refl.h"

using std::string;
namespace gpb = ::google::protobuf;
namespace rj = ::rapidjson;

namespace util {
namespace {

typedef gpb::FieldDescriptor FD;
using RapidWriter =
    rj::Writer<rj::StringBuffer, rj::UTF8<>, rj::UTF8<>, rj::CrtAllocator, rj::kWriteNanAndInfFlag>;

void PrintValue(const gpb::Message& msg, const Pb2JsonOptions& options,
                const gpb::FieldDescriptor* fd, const gpb::Reflection* refl, RapidWriter* res);
void PrintRepeated(const gpb::Message& msg, const Pb2JsonOptions& options,
                   const gpb::FieldDescriptor* fd, const gpb::Reflection* refl, RapidWriter* res);

void Pb2JsonInternal(const ::google::protobuf::Message& msg, const Pb2JsonOptions& options,
                     RapidWriter* res) {
  const gpb::Descriptor* descr = msg.GetDescriptor();
  const gpb::Reflection* refl = msg.GetReflection();
  res->StartObject();
  for (int i = 0; i < descr->field_count(); ++i) {
    const gpb::FieldDescriptor* fd = descr->field(i);
    bool is_set = (fd->is_repeated() && refl->FieldSize(msg, fd) > 0) || fd->is_required() ||
                  (fd->is_optional() && refl->HasField(msg, fd));
    if (!is_set)
      continue;

    const string& fname = options.field_name_cb ? options.field_name_cb(*fd) : fd->name();
    if (fname.empty())
      continue;
    res->Key(fname.c_str(), fname.size());
    if (fd->is_repeated()) {
      PrintRepeated(msg, options, fd, refl, res);
    } else {
      PrintValue(msg, options, fd, refl, res);
    }
  }
  res->EndObject();
}

void PrintValue(const gpb::Message& msg, const Pb2JsonOptions& options,
                const gpb::FieldDescriptor* fd, const gpb::Reflection* refl, RapidWriter* res) {
  switch (fd->cpp_type()) {
    case FD::CPPTYPE_INT32:
      res->Int(refl->GetInt32(msg, fd));
      break;
    case FD::CPPTYPE_UINT32:
      res->Uint(refl->GetUInt32(msg, fd));
      break;
    case FD::CPPTYPE_INT64:
      res->Int64(refl->GetInt64(msg, fd));
      break;
    case FD::CPPTYPE_UINT64:
      res->Uint64(refl->GetUInt64(msg, fd));
      break;
    case FD::CPPTYPE_FLOAT: {
      float fval = refl->GetFloat(msg, fd);
      char buf[40];
      int sz = absl::SNPrintF(buf, sizeof(buf), "%.7g", fval);
      res->RawValue(buf, sz, rj::kNumberType);
    } break;
    case FD::CPPTYPE_DOUBLE:
      res->Double(refl->GetDouble(msg, fd));
      break;
    case FD::CPPTYPE_STRING: {
      string scratch;
      const string& value = refl->GetStringReference(msg, fd, &scratch);
      res->String(value.c_str(), value.size());
    } break;
    case FD::CPPTYPE_BOOL: {
      bool b = refl->GetBool(msg, fd);
      // Unfortunate hack in our company code.
      if (options.bool_as_int && options.bool_as_int(*fd)) {
        res->Int(int(b));
      } else {
        res->Bool(b);
      }
    } break;

    case FD::CPPTYPE_ENUM:
      if (options.enum_as_ints) {
        res->Int(refl->GetEnum(msg, fd)->number());
      } else {
        const auto& tmp = refl->GetEnum(msg, fd)->name();
        res->String(tmp.c_str(), tmp.size());
      }
      break;
    case FD::CPPTYPE_MESSAGE:
      Pb2JsonInternal(refl->GetMessage(msg, fd), options, res);
      break;
    default:
      LOG(FATAL) << "Not supported field " << fd->cpp_type_name();
  }
}

template <FD::CppType t, typename Cb>
void UnwindArr(const gpb::Message& msg, const gpb::FieldDescriptor* fd, const gpb::Reflection* refl,
               Cb cb) {
  using CppType = typename pb::FD_Traits_t<t>;
  const auto& arr = refl->GetRepeatedFieldRef<CppType>(msg, fd);
  std::for_each(std::begin(arr), std::end(arr), cb);
}

void PrintRepeated(const gpb::Message& msg, const Pb2JsonOptions& options,
                   const gpb::FieldDescriptor* fd, const gpb::Reflection* refl, RapidWriter* res) {
  res->StartArray();
  switch (fd->cpp_type()) {
    case FD::CPPTYPE_INT32:
      UnwindArr<FD::CPPTYPE_INT32>(msg, fd, refl, [res](auto val) { res->Int(val); });
      break;
    case FD::CPPTYPE_UINT32:
      UnwindArr<FD::CPPTYPE_UINT32>(msg, fd, refl, [res](auto val) { res->Uint(val); });
      break;
    case FD::CPPTYPE_INT64:
      UnwindArr<FD::CPPTYPE_INT64>(msg, fd, refl, [res](auto val) { res->Int64(val); });
      break;
    case FD::CPPTYPE_UINT64:
      UnwindArr<FD::CPPTYPE_UINT64>(msg, fd, refl, [res](auto val) { res->Uint64(val); });
      break;
    case FD::CPPTYPE_FLOAT:
      UnwindArr<FD::CPPTYPE_FLOAT>(msg, fd, refl, [res](auto val) { res->Double(val); });
      break;
    case FD::CPPTYPE_DOUBLE:
      UnwindArr<FD::CPPTYPE_DOUBLE>(msg, fd, refl, [res](auto val) { res->Double(val); });
      break;
    case FD::CPPTYPE_STRING:
      UnwindArr<FD::CPPTYPE_STRING>(
          msg, fd, refl, [res](const string& val) { res->String(val.c_str(), val.size()); });
      break;
    case FD::CPPTYPE_BOOL:
      UnwindArr<FD::CPPTYPE_BOOL>(msg, fd, refl, [res](auto val) { res->Bool(val); });
      break;

    case FD::CPPTYPE_ENUM: {
      int sz = refl->FieldSize(msg, fd);
      for (int i = 0; i < sz; ++i) {
        const gpb::EnumValueDescriptor* edescr = refl->GetRepeatedEnum(msg, fd, i);
        const string& name = edescr->name();
        res->String(name.c_str(), name.size());
      }
    } break;
    case FD::CPPTYPE_MESSAGE: {
      const auto& arr = refl->GetRepeatedFieldRef<gpb::Message>(msg, fd);
      std::unique_ptr<gpb::Message> scratch_space(arr.NewMessage());
      for (int i = 0; i < arr.size(); ++i) {
        Pb2JsonInternal(arr.Get(i, scratch_space.get()), options, res);
      }
    } break;
    default:
      LOG(FATAL) << "Not supported field " << fd->cpp_type_name();
  }
  res->EndArray();
}

class PbHandler {
  const Json2PbOptions& opts_;

 public:
  string err_msg;
  using Ch = char;

  PbHandler(const Json2PbOptions& opts, ::google::protobuf::Message* msg) : opts_(opts) {
    stack_.emplace_back(msg->GetReflection(), msg);
  }

  bool Key(const Ch* str, size_t len, bool copy);

  bool String(const Ch* str, size_t len, bool);

  bool StartObject();

  bool EndObject(size_t member_count);

  bool Null();
  bool Bool(bool b);
  bool Int(int i);
  bool Uint(unsigned i);
  bool Int64(int64_t i);
  bool Uint64(uint64_t i);
  bool Double(double d);

  /// enabled via kParseNumbersAsStringsFlag, string is not null-terminated (use length)
  bool RawNumber(const Ch* str, size_t length, bool copy) { return false; }

  bool StartArray();
  bool EndArray(size_t elementCount);

 private:
  bool IntRepeated(int i);
  bool UintRepeated(unsigned i);
  bool Int64Repeated(int64_t i);
  bool UInt64Repeated(uint64_t i);
  bool DoubleRepeated(double d);

  using FD = gpb::FieldDescriptor;
  template <typename T> using MRFR = gpb::MutableRepeatedFieldRef<T>;

  using ArrRef =
      absl::variant<MRFR<bool>, MRFR<uint32_t>, MRFR<int32_t>, MRFR<uint64_t>, MRFR<int64_t>,
                    MRFR<float>, MRFR<double>, MRFR<string>, MRFR<gpb::Message>>;

  struct Object {
    const gpb::Reflection* refl;
    gpb::Message* msg;

    absl::optional<std::pair<const FD*, ArrRef>> arr_ref;

    Object(const gpb::Reflection* r, gpb::Message* m) : refl(r), msg(m) {}

    template <FD::CppType t> auto& GetArray() {
      return absl::get<MRFR<pb::FD_Traits_t<t>>>(arr_ref->second);
    }
  };

  template <FD::CppType t> static auto MakeArr(const FD* f, const Object& o) {
    return std::pair<const FD*, ArrRef>{f, pb::GetMutableArray<t>(o.refl, f, o.msg)};
  }

  absl::InlinedVector<Object, 16> stack_;
  const gpb::FieldDescriptor* field_ = nullptr;
  string key_name_;

  unsigned disabled_level_ = 0;
};

bool PbHandler::Key(const Ch* str, size_t len, bool copy) {
  if (disabled_level_ > 1) {
    return true;
  }

  key_name_.assign(str, len);

  DCHECK(!stack_.empty());
  auto& obj = stack_.back();

  const auto& msg = *obj.msg;
  field_ = msg.GetDescriptor()->FindFieldByName(key_name_);

  return field_ != nullptr || opts_.skip_unknown_fields;
}

bool PbHandler::String(const Ch* str, size_t len, bool) {
  DCHECK(!stack_.empty());
  auto& obj = stack_.back();

  const string str_val(str, len);

  if (obj.arr_ref) {
    obj.GetArray<FD::CPPTYPE_STRING>().Add(str_val);
    return true;
  }

  if (!field_)
    return opts_.skip_unknown_fields;

  switch (field_->cpp_type()) {
    case FD::CPPTYPE_STRING:
      obj.refl->SetString(obj.msg, field_, str_val);
      break;
    case FD::CPPTYPE_ENUM: {
      const gpb::EnumValueDescriptor* ev = field_->enum_type()->FindValueByName(str_val);
      if (!ev)
        return false;
      obj.refl->SetEnum(obj.msg, field_, ev);
    } break;

    default:
      return false;
  }
  field_ = nullptr;

  return true;
}

bool PbHandler::StartObject() {
  DCHECK(!stack_.empty());

  auto& obj = stack_.back();
  if (obj.arr_ref) {
    const FD* field = obj.arr_ref->first;
    if (field->cpp_type() != FD::CPPTYPE_MESSAGE) {
      err_msg = absl::StrCat("Expected msg type but found ", field->cpp_type_name());
      return false;
    }
    gpb::Message* child = obj.refl->AddMessage(obj.msg, field);
    stack_.emplace_back(child->GetReflection(), child);
    return true;
  }

  if (!field_) {
    ++disabled_level_;
    return disabled_level_ == 1 || opts_.skip_unknown_fields;
  }

  if (field_->cpp_type() != gpb::FieldDescriptor::CPPTYPE_MESSAGE) {
    err_msg = absl::StrCat("Error in StartObject, type ", field_->cpp_type_name());
    return false;
  }

  gpb::Message* child = obj.refl->MutableMessage(obj.msg, field_);
  stack_.emplace_back(child->GetReflection(), child);
  return true;
}

bool PbHandler::EndObject(size_t member_count) {
  if (disabled_level_ > 1) {
    --disabled_level_;
    DCHECK(field_ == nullptr);
    return true;
  }

  DVLOG(2) << "EndObject " << member_count;
  stack_.pop_back();
  field_ = nullptr;
  return true;
}

bool PbHandler::Null() {
  auto& obj = stack_.back();
  if (field_ && !obj.arr_ref) {
    obj.refl->ClearField(obj.msg, field_);
  }
  return true;
}

#define CASE(Type)                                    \
  case FD::Type:                                      \
    SetField<FD::Type>(obj.refl, field_, i, obj.msg); \
    break

bool PbHandler::Bool(bool b) {
  auto& obj = stack_.back();
  if (obj.arr_ref) {
    const FD* field = obj.arr_ref->first;
    if (field->cpp_type() != FD::CPPTYPE_BOOL) {
      err_msg = absl::StrCat("Expected msg type but found ", field->cpp_type_name());
      return false;
    }
    obj.GetArray<FD::CPPTYPE_BOOL>().Add(b);
    return true;
  }

  if (!field_)
    return opts_.skip_unknown_fields;

  if (field_->cpp_type() != FD::CPPTYPE_BOOL) {
    err_msg = absl::StrCat("Expected BOOL, found ", field_->cpp_type_name());
    return false;
  }

  pb::SetField<FD::CPPTYPE_BOOL>(obj.refl, field_, b, obj.msg);

  return true;
}

bool PbHandler::Int(int i) {
  auto& obj = stack_.back();

  if (obj.arr_ref) {
    return IntRepeated(i);
  }

  if (!field_) {
    return opts_.skip_unknown_fields;
  }

  using namespace pb;
  switch (field_->cpp_type()) {
    CASE(CPPTYPE_INT32);
    CASE(CPPTYPE_INT64);
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_DOUBLE);
    default:
      err_msg = absl::StrCat("Unexpected int type ", field_->cpp_type_name());
      return false;
  }
  return true;
}

bool PbHandler::IntRepeated(int i) {
  auto& obj = stack_.back();

  const FD* field = obj.arr_ref->first;
  switch (field->cpp_type()) {
    case FD::CPPTYPE_INT32:
      obj.GetArray<FD::CPPTYPE_INT32>().Add(i);
      break;
    case FD::CPPTYPE_INT64:
      obj.GetArray<FD::CPPTYPE_INT64>().Add(i);
      break;
    case FD::CPPTYPE_FLOAT:
      obj.GetArray<FD::CPPTYPE_FLOAT>().Add(i);
      break;
    case FD::CPPTYPE_DOUBLE:
      obj.GetArray<FD::CPPTYPE_DOUBLE>().Add(i);
      break;
    default:
      err_msg = absl::StrCat("Unexpected int type ", field_->cpp_type_name());
      return false;
  }

  return true;
}

bool PbHandler::Uint(unsigned i) {
  auto& obj = stack_.back();
  if (obj.arr_ref) {
    return UintRepeated(i);
  }

  if (!field_) {
    return opts_.skip_unknown_fields;
  }

  using namespace pb;
  switch (field_->cpp_type()) {
    CASE(CPPTYPE_INT32);
    CASE(CPPTYPE_UINT32);
    CASE(CPPTYPE_INT64);
    CASE(CPPTYPE_UINT64);
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_DOUBLE);
    case FD::CPPTYPE_BOOL:
      if (i > 1) {
        err_msg = absl::StrCat("Invalid bool value ", i);
        return false;
      }
      SetField<FD::CPPTYPE_BOOL>(obj.refl, field_, i != 0, obj.msg);
      break;
    default:
      err_msg = absl::StrCat("Unexpected Uint type ", field_->cpp_type_name());
      return false;
  }
  return true;
}

bool PbHandler::Int64(int64_t i) {
  auto& obj = stack_.back();
  if (obj.arr_ref) {
    return Int64Repeated(i);
  }

  if (!field_) {
    return opts_.skip_unknown_fields;
  }

  using namespace pb;

  switch (field_->cpp_type()) {
    CASE(CPPTYPE_INT64);
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_DOUBLE);
    default:
      return false;
  }
  return true;
}

bool PbHandler::Uint64(uint64_t i) {
  auto& obj = stack_.back();
  if (obj.arr_ref) {
    return UInt64Repeated(i);
  }

  if (!field_) {
    return opts_.skip_unknown_fields;
  }

  using namespace pb;
  switch (field_->cpp_type()) {
    CASE(CPPTYPE_UINT64);
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_DOUBLE);
    default:
      err_msg = absl::StrCat("Unexpected Uint64 type ", field_->cpp_type_name());
      return false;
  }
  return true;
}

bool PbHandler::Double(double i) {
  auto& obj = stack_.back();
  if (obj.arr_ref) {
    return DoubleRepeated(i);
  }

  if (!field_) {
    return opts_.skip_unknown_fields;
  }

  using namespace pb;

  switch (field_->cpp_type()) {
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_DOUBLE);
    default:
      err_msg = absl::StrCat("Unexpected Double type ", field_->cpp_type_name());
      return false;
  }
  return true;
}

#undef CASE

bool PbHandler::UintRepeated(unsigned i) {
  auto& obj = stack_.back();

  const FD* field = obj.arr_ref->first;

  switch (field->cpp_type()) {
    case FD::CPPTYPE_INT32:
      obj.GetArray<FD::CPPTYPE_INT32>().Add(i);
      break;
    case FD::CPPTYPE_UINT32:
      obj.GetArray<FD::CPPTYPE_UINT32>().Add(i);
      break;
    case FD::CPPTYPE_INT64:
      obj.GetArray<FD::CPPTYPE_INT64>().Add(i);
      break;
    case FD::CPPTYPE_UINT64:
      obj.GetArray<FD::CPPTYPE_UINT64>().Add(i);
      break;
    case FD::CPPTYPE_FLOAT:
      obj.GetArray<FD::CPPTYPE_FLOAT>().Add(i);
      break;
    case FD::CPPTYPE_DOUBLE:
      obj.GetArray<FD::CPPTYPE_DOUBLE>().Add(i);
      break;
    case FD::CPPTYPE_BOOL:
      if (i > 1) {
        err_msg = absl::StrCat("Invalid bool value ", i);
        return false;
      }
      obj.GetArray<FD::CPPTYPE_BOOL>().Add(i != 0);
      break;
    default:
      err_msg = absl::StrCat("Unexpected uint type ", field_->cpp_type_name());
      return false;
  }

  return true;
}

bool PbHandler::Int64Repeated(int64_t i) {
  auto& obj = stack_.back();

  const FD* field = obj.arr_ref->first;

  switch (field->cpp_type()) {
    case FD::CPPTYPE_INT64:
      obj.GetArray<FD::CPPTYPE_INT64>().Add(i);
      break;
    case FD::CPPTYPE_FLOAT:
      obj.GetArray<FD::CPPTYPE_FLOAT>().Add(i);
      break;
    case FD::CPPTYPE_DOUBLE:
      obj.GetArray<FD::CPPTYPE_DOUBLE>().Add(i);
      break;
    default:
      err_msg = absl::StrCat("Unexpected uint type ", field_->cpp_type_name());
      return false;
  }

  return true;
}

bool PbHandler::UInt64Repeated(uint64_t i) {
  auto& obj = stack_.back();

  const FD* field = obj.arr_ref->first;

  switch (field->cpp_type()) {
    case FD::CPPTYPE_UINT64:
      obj.GetArray<FD::CPPTYPE_UINT64>().Add(i);
      break;
    case FD::CPPTYPE_FLOAT:
      obj.GetArray<FD::CPPTYPE_FLOAT>().Add(i);
      break;
    case FD::CPPTYPE_DOUBLE:
      obj.GetArray<FD::CPPTYPE_DOUBLE>().Add(i);
      break;
    default:
      err_msg = absl::StrCat("Unexpected uint type ", field_->cpp_type_name());
      return false;
  }

  return true;
}

bool PbHandler::DoubleRepeated(double d) {
  auto& obj = stack_.back();

  const FD* field = obj.arr_ref->first;

  switch (field->cpp_type()) {
    case FD::CPPTYPE_FLOAT:
      obj.GetArray<FD::CPPTYPE_FLOAT>().Add(d);
      break;
    case FD::CPPTYPE_DOUBLE:
      obj.GetArray<FD::CPPTYPE_DOUBLE>().Add(d);
      break;
    default:
      err_msg = absl::StrCat("Unexpected double type ", field_->cpp_type_name());
      return false;
  }

  return true;
}

bool PbHandler::StartArray() {
  if (!field_) {
    ++disabled_level_;
    return opts_.skip_unknown_fields;
  }

  if (!field_->is_repeated()) {
    err_msg = absl::StrCat("Bad array ", key_name_);
    return false;
  }
  using FD = gpb::FieldDescriptor;
  auto& obj = stack_.back();
  DCHECK(!obj.arr_ref);

#define CASE(Type)                                       \
  case FD::Type:                                         \
    obj.arr_ref.emplace(MakeArr<FD::Type>(field_, obj)); \
    break

  switch (field_->cpp_type()) {
    CASE(CPPTYPE_UINT32);
    CASE(CPPTYPE_INT32);
    CASE(CPPTYPE_UINT64);
    CASE(CPPTYPE_INT64);
    CASE(CPPTYPE_DOUBLE);
    CASE(CPPTYPE_FLOAT);
    CASE(CPPTYPE_STRING);
    CASE(CPPTYPE_MESSAGE);

    default:
      err_msg = absl::StrCat("Unknown array type ", field_->cpp_type_name());
      return false;
  }
#undef CASE
  return true;
}

bool PbHandler::EndArray(size_t elementCount) {
  auto& obj = stack_.back();

  if (disabled_level_ > 1) {
    DCHECK(!obj.arr_ref && field_ == nullptr);
    --disabled_level_;
  } else {
    DCHECK(obj.arr_ref);
    obj.arr_ref.reset();
    field_ = nullptr;
  }
  return true;
}

}  // namespace

std::string Pb2Json(const ::google::protobuf::Message& msg, const Pb2JsonOptions& options) {
  rj::StringBuffer sb;
  RapidWriter rw(sb);
  rw.SetMaxDecimalPlaces(9);

  Pb2JsonInternal(msg, options, &rw);
  return string(sb.GetString(), sb.GetSize());
}

Status Json2Pb(std::string json, ::google::protobuf::Message* msg, const Json2PbOptions& opts) {
  rj::Reader reader;

  PbHandler h(opts, msg);
  rj::InsituStringStream stream(&json.front());

  rj::ParseResult pr = reader.Parse<rj::kParseInsituFlag | rj::kParseTrailingCommasFlag>(stream, h);
  if (pr.IsError()) {
    Status st(StatusCode::PARSE_ERROR,
              absl::StrCat(rj::GetParseError_En(pr.Code()), "/", h.err_msg));
    return st;
  }
  return Status::OK;
}

}  // namespace util
