// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (roman@ubimo.com)
//
#include "util/rpc/service_descriptor.h"

namespace util {

// RPC-Server side part.

namespace rpc {

ServiceDescriptor::ServiceDescriptor() {
}

ServiceDescriptor::~ServiceDescriptor() {
}

void ServiceDescriptor::SetOptions(size_t index, const MethodOptions& opts) {
  methods_[index].options = opts;
}


}  // namespace rpc
}  // namespace util
