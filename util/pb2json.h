// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <string>

#include <google/protobuf/message.h>

#include "util/status.h"

namespace util {

struct Pb2JsonOptions {
  typedef std::function<std::string(const ::google::protobuf::FieldDescriptor& fd)> FieldNameCb;

  typedef std::function<bool(const ::google::protobuf::FieldDescriptor& fd)> BoolAsIntegerPred;

  bool enum_as_ints = false;

  FieldNameCb field_name_cb;
  BoolAsIntegerPred bool_as_int;
};

std::string Pb2Json(const ::google::protobuf::Message& msg,
                    const Pb2JsonOptions& options = Pb2JsonOptions());

struct Json2PbOptions {
  bool skip_unknown_fields;

  Json2PbOptions(bool sk = true) : skip_unknown_fields(sk) {}
};

Status Json2Pb(std::string json, ::google::protobuf::Message* msg, const Json2PbOptions& options);

inline Status Json2Pb(std::string json, ::google::protobuf::Message* msg,
                      bool skip_unknown_fields = true) {
  return Json2Pb(std::move(json), msg, Json2PbOptions(skip_unknown_fields));
}

}  // namespace util
