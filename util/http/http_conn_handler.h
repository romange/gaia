// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include "strings/unique_strings.h"
#include "util/asio/connection_handler.h"

namespace util {
namespace http {

// URL consists of path and query delimited by '?'.
// query can be broken into query args delimited by '&'.
// Each query arg can be a pair of "key=value" values.
// In case there is not '=' delimiter, only the first field is filled.
typedef std::vector<std::pair<StringPiece, StringPiece>> QueryArgs;

class CallbackRegistry;

class HttpHandler : public ConnectionHandler {
 public:
  typedef ::boost::beast::http::string_body BodyType;
  typedef ::boost::beast::http::response<BodyType> Response;
  typedef std::function < void(const QueryArgs&, Response*)> RequestCb;

  HttpHandler(CallbackRegistry* registry = nullptr);

  boost::system::error_code HandleRequest() final override;

 protected:
  virtual bool Authorize(StringPiece key, StringPiece value) const {
    return true;
  }
  const char* favicon_;
  const char* resource_prefix_;

 private:
  bool Authorize(const QueryArgs& args) const;
  void HandleRequestInternal(StringPiece target, Response* dest);

  CallbackRegistry* registry_;
};

// Should be one per process. HandlerFactory should pass it to HttpHandler's c-tor once
// the registry is finalized. Currently does not support on the fly updates - requires
// multi-threading support.
class CallbackRegistry {
  friend class HttpHandler;
 public:
  // Returns true if a callback was registered.
  bool RegisterCb(StringPiece path, bool protect, HttpHandler::RequestCb cb);

 private:
  struct CbInfo {
    bool is_protected;
    HttpHandler::RequestCb cb;
  };
  StringPieceMap<CbInfo> cb_map_;
};

}  // namespace http

}  // namespace util
