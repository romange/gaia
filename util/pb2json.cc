// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pb2json.h"

#include "base/logging.h"
#include "absl/strings/str_cat.h"

#include <google/protobuf/repeated_field.h>
#include <google/protobuf/reflection.h>
#include <rapidjson/writer.h>

using std::string;
namespace gpb = ::google::protobuf;
namespace rj = ::rapidjson;

namespace util {
namespace {

typedef gpb::FieldDescriptor FD;
using RapidWriter = rj::Writer<rj::StringBuffer, rj::UTF8<>, rj::UTF8<>,
                               rj::CrtAllocator, rj::kWriteNanAndInfFlag>;

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
    bool is_set = (fd->is_repeated() && refl->FieldSize(msg, fd) > 0) ||
      fd->is_required() || (fd->is_optional() && refl->HasField(msg, fd));
    if (!is_set)
      continue;

    const gpb::FieldOptions& fo = fd->options();
    const string& fname = options.field_name_cb ? options.field_name_cb(fo, *fd) : fd->name();
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
      // absl::AlphaNum al(refl->GetFloat(msg, fd));
      // res->RawNumber(al.data(), al.size());
      res->Double(refl->GetFloat(msg, fd));
    }
    break;
    case FD::CPPTYPE_DOUBLE:
      res->Double(refl->GetDouble(msg, fd));
    break;
    case FD::CPPTYPE_STRING: {
      string scratch;
      const string& value = refl->GetStringReference(msg, fd, &scratch);
      res->String(value.c_str(), value.size());
      // absl::StrAppend(res, "\"", strings::JsonEscape(value), "\"");
    }
    break;
    case FD::CPPTYPE_BOOL: {
      bool b = refl->GetBool(msg, fd);
      res->Bool(b);
    }
    break;

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

template<FD::CppType> struct FD_Traits;
#define DECLARE_FD_TRAITS(CPP_TYPE, src_type) \
    template<> struct FD_Traits<gpb::FieldDescriptor::CPP_TYPE> { \
      typedef src_type type; }

DECLARE_FD_TRAITS(CPPTYPE_BOOL, bool);
DECLARE_FD_TRAITS(CPPTYPE_INT32, gpb::int32);
DECLARE_FD_TRAITS(CPPTYPE_UINT32, gpb::uint32);
DECLARE_FD_TRAITS(CPPTYPE_INT64, gpb::int64);
DECLARE_FD_TRAITS(CPPTYPE_UINT64, gpb::uint64);
DECLARE_FD_TRAITS(CPPTYPE_DOUBLE, double);
DECLARE_FD_TRAITS(CPPTYPE_FLOAT, float);
DECLARE_FD_TRAITS(CPPTYPE_STRING, std::string);

template<FD::CppType t, typename Cb>
void UnwindArr(const gpb::Message& msg, const gpb::FieldDescriptor* fd,
               const gpb::Reflection* refl, Cb cb) {
  using CppType = typename FD_Traits<t>::type;
  const auto& arr = refl->GetRepeatedFieldRef<CppType>(msg, fd);
  std::for_each(std::begin(arr), std::end(arr), cb);
}

void PrintRepeated(const gpb::Message& msg, const Pb2JsonOptions& options,
                   const gpb::FieldDescriptor* fd, const gpb::Reflection* refl, RapidWriter* res) {
  res->StartArray();
  switch (fd->cpp_type()) {
    case FD::CPPTYPE_INT32:
      UnwindArr<FD::CPPTYPE_INT32>(msg, fd, refl, [res](auto val) {res->Int(val);});
    break;
    case FD::CPPTYPE_UINT32:
      UnwindArr<FD::CPPTYPE_UINT32>(msg, fd, refl, [res](auto val) {res->Uint(val);});
    break;
    case FD::CPPTYPE_INT64:
      UnwindArr<FD::CPPTYPE_INT64>(msg, fd, refl, [res](auto val) {res->Int64(val);});
    break;
    case FD::CPPTYPE_UINT64:
      UnwindArr<FD::CPPTYPE_UINT64>(msg, fd, refl, [res](auto val) {res->Uint64(val);});
    break;
    case FD::CPPTYPE_FLOAT:
      UnwindArr<FD::CPPTYPE_FLOAT>(msg, fd, refl, [res](auto val) {res->Double(val);});
    break;
    case FD::CPPTYPE_DOUBLE:
      UnwindArr<FD::CPPTYPE_DOUBLE>(msg, fd, refl, [res](auto val) {res->Double(val);});
    break;
    case FD::CPPTYPE_STRING:
      UnwindArr<FD::CPPTYPE_STRING>(msg, fd, refl,
                                    [res](const string& val) {
                                      res->String(val.c_str(), val.size());});
    break;
    case FD::CPPTYPE_BOOL:
      UnwindArr<FD::CPPTYPE_BOOL>(msg, fd, refl, [res](auto val) {res->Bool(val);});
    break;

    case FD::CPPTYPE_ENUM: {
      int sz = refl->FieldSize(msg, fd);
      for (int i = 0; i < sz; ++i) {
        const gpb::EnumValueDescriptor* edescr = refl->GetRepeatedEnum(msg, fd, i);
        const string& name = edescr->name();
        res->String(name.c_str(), name.size());
      }
    }
    break;
    case FD::CPPTYPE_MESSAGE: {
      const auto& arr = refl->GetRepeatedFieldRef<gpb::Message>(msg, fd);
      std::unique_ptr<gpb::Message> scratch_space(arr.NewMessage());
      for (int i = 0; i < arr.size(); ++i) {
        Pb2JsonInternal(arr.Get(i, scratch_space.get()), options, res);
      }
    }
    break;
    default:
      LOG(FATAL) << "Not supported field " << fd->cpp_type_name();
  }
  res->EndArray();
}

}  // namespace

std::string Pb2Json(const ::google::protobuf::Message& msg, const Pb2JsonOptions& options) {
  rj::StringBuffer sb;
  RapidWriter rw(sb);
  rw.SetMaxDecimalPlaces(9);

  Pb2JsonInternal(msg, options, &rw);
  return string(sb.GetString(), sb.GetSize());
}


}  // namespace util
