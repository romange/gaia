// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/connection_handler.h"
#include "strings/stringpiece.h"

namespace util {
namespace http {

// URL consists of path and query delimited by '?'.
// query can be broken into query args delimited by '&'.
// Each query arg can be a pair of "key=value" values.
// In case there is not '=' delimiter, only the first field is filled.
typedef std::vector<std::pair<StringPiece, StringPiece>> QueryArgs;

class HttpHandler : public ConnectionHandler {
 public:
  HttpHandler();

  boost::system::error_code HandleRequest() final override;
 private:
  virtual bool Authorize(StringPiece key, StringPiece value) const {
    return true;
  }

  bool Authorize(const QueryArgs& args) const;

  const char* favicon_;
};

}  // namespace http

}  // namespace util
