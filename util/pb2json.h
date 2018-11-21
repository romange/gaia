// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <string>

#include <google/protobuf/message.h>

namespace util {

struct Pb2JsonOptions {
  typedef std::function<std::string(const ::google::protobuf::FieldDescriptor& fd)>
      FieldNameCb;

  bool enum_as_ints = false;
  FieldNameCb field_name_cb;
};

std::string Pb2Json(const ::google::protobuf::Message& msg,
                    const Pb2JsonOptions& options = Pb2JsonOptions());

}  // namespace util
