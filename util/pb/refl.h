// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace util {
namespace pb {

using FD = ::google::protobuf::FieldDescriptor;
using Msg = ::google::protobuf::Message;

template <FD::CppType t> struct FD_Traits;

#define DECLARE_FD_TRAITS(CPP_TYPE, src_type) \
  template <> struct FD_Traits<FD::CPP_TYPE> { typedef src_type type; }

DECLARE_FD_TRAITS(CPPTYPE_INT32, int32_t);
DECLARE_FD_TRAITS(CPPTYPE_UINT32, uint32_t);
DECLARE_FD_TRAITS(CPPTYPE_INT64, int64_t);
DECLARE_FD_TRAITS(CPPTYPE_UINT64, uint64_t);
DECLARE_FD_TRAITS(CPPTYPE_DOUBLE, double);
DECLARE_FD_TRAITS(CPPTYPE_FLOAT, float);
DECLARE_FD_TRAITS(CPPTYPE_STRING, std::string);
DECLARE_FD_TRAITS(CPPTYPE_MESSAGE, Msg);

#undef DECLARE_FD_TRAITS

template <FD::CppType t> using FD_Traits_t = typename FD_Traits<t>::type;

template <FD::CppType t>
auto GetMutableArray(const ::google::protobuf::Reflection* refl, const FD* field, Msg* msg) {
  return refl->GetMutableRepeatedFieldRef<FD_Traits_t<t>>(msg, field);
}

template <FD::CppType t>
void SetField(const ::google::protobuf::Reflection* refl, const FD* field,
              const FD_Traits_t<t>& val, Msg* msg);

template <FD::CppType t>
FD_Traits_t<t> GetField(const ::google::protobuf::Reflection* refl, const FD* field,
                        const Msg* msg);

#define DEFINE_MODIFIER(CPP_TYPE, Name)                                                        \
  template <>                                                                                  \
  FD_Traits_t<FD::CPP_TYPE> GetField<FD::CPP_TYPE>(const ::google::protobuf::Reflection* refl, \
                                                   const FD* field, const Msg* msg) {          \
    return refl->Get##Name(*msg, field);                                                       \
  }                                                                                            \
                                                                                               \
  template <>                                                                                  \
  void SetField<FD::CPP_TYPE>(const ::google::protobuf::Reflection* refl, const FD* field,     \
                              const FD_Traits_t<FD::CPP_TYPE>& val, Msg* msg) {                \
    refl->Set##Name(msg, field, val);                                                         \
  }

DEFINE_MODIFIER(CPPTYPE_INT32, Int32);
DEFINE_MODIFIER(CPPTYPE_UINT32, UInt32);
DEFINE_MODIFIER(CPPTYPE_INT64, Int64);
DEFINE_MODIFIER(CPPTYPE_UINT64, UInt64);
DEFINE_MODIFIER(CPPTYPE_DOUBLE, Double);
DEFINE_MODIFIER(CPPTYPE_FLOAT, Float);
DEFINE_MODIFIER(CPPTYPE_STRING, String);

}  // namespace pb

}  // namespace util
