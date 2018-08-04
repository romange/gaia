// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <google/protobuf/message.h>

namespace util {

std::string Pb2Json(const ::google::protobuf::Message& msg);


}  // namespace util
