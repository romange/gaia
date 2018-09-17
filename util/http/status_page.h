// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <string>

#include "util/http/http_conn_handler.h"

namespace util {
namespace http {

void BuildStatusPage(const QueryArgs& args, const char* resource_prefix,
                     HttpHandler::Response* response);

}  // namespace http
}  // namespace util

