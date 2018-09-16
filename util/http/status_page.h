// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <string>

#include "util/http/http_conn_handler.h"

namespace util {
namespace http {

std::string BuildStatusPage(const QueryArgs& args, const char* resource_prefix);

}  // namespace http
}  // namespace util

