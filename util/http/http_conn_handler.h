// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/connection_handler.h"

namespace util {
namespace http {

class HttpHandler : public ConnectionHandler {
 public:
  HttpHandler();

  boost::system::error_code HandleRequest() final override;

 private:
  const char* favicon_;
};

}  // namespace http

}  // namespace util
