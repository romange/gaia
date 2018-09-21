// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/beast/http/message.hpp>
#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "strings/unique_strings.h"
#include "util/asio/connection_handler.h"
#include "util/asio/yield.h"

namespace util {
namespace http {

// URL consists of path and query delimited by '?'.
// query can be broken into query args delimited by '&'.
// Each query arg can be a pair of "key=value" values.
// In case there is not '=' delimiter, only the first field is filled.
typedef std::vector<std::pair<StringPiece, StringPiece>> QueryArgs;

typedef ::boost::beast::http::response<::boost::beast::http::string_body> StringResponse;

class CallbackRegistry;

extern const char kHtmlMime[];
extern const char kJsonMime[];
extern const char kSvgMime[];
extern const char kTextMime[];

// This is the C++11 equivalent of a generic lambda.
// The function object is used to send an HTTP message.
template <typename Stream>
struct SendLambda {
  template <typename Body>
  using Response = ::boost::beast::http::response<Body>;
  using error_code = ::boost::system::error_code;

  Stream& stream_;
  error_code ec;

  explicit SendLambda(Stream& stream) : stream_(stream) {
  }

  template <typename Body>
  void Invoke(Response<Body>&& msg) {
    using fibers_ext::yield;

    // Determine if we should close the connection after
    // close_ = msg.need_eof();

    // We need the serializer here because the serializer requires
    // a non-const file_body, and the message oriented version of
    // http::write only works with const messages.
    msg.prepare_payload();
    ::boost::beast::http::response_serializer<Body> sr{msg};

    ::boost::beast::http::async_write(stream_, sr, yield[ec]);
  }
};

class HttpHandler : public ConnectionHandler {
 public:
  typedef ::boost::beast::http::request<::boost::beast::http::string_body> RequestType;
  typedef SendLambda<socket_t> SendFunction;
  // typedef ::boost::beast::http::string_body BodyType;
  // typedef ::boost::beast::http::response<BodyType> Response;
  typedef std::function<void(const QueryArgs&, SendFunction*)> RequestCb;

  HttpHandler(const CallbackRegistry* registry = nullptr);

  boost::system::error_code HandleRequest() final override;

 protected:
  virtual bool Authorize(StringPiece key, StringPiece value) const {
    return true;
  }
  const char* favicon_;
  const char* resource_prefix_;

 private:
  bool Authorize(const QueryArgs& args) const;
  void HandleRequestInternal(const RequestType& req, SendFunction* send);

  const CallbackRegistry* registry_;
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
