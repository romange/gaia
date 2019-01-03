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

inline StringResponse MakeStringResponse(
    ::boost::beast::http::status st = ::boost::beast::http::status::ok) {
  return StringResponse(st, 11);
}

inline void SetMime(const char* mime, ::boost::beast::http::fields* dest) {
  dest->set(::boost::beast::http::field::content_type, mime);
}

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

// Should be one per process. Represents http server interface.
// Currently does not support on the fly updates - requires
// multi-threading support.
class ListenerBase : public ListenerInterface {
  friend class HttpHandler;

 public:
  typedef SendLambda<::boost::asio::ip::tcp::socket> SendFunction;
  typedef std::function<void(const QueryArgs&, SendFunction*)> RequestCb;

  // Returns true if a callback was registered.
  bool RegisterCb(StringPiece path, bool protect, RequestCb cb);

 private:
  struct CbInfo {
    bool is_protected;
    RequestCb cb;
  };
  StringPieceMap<CbInfo> cb_map_;
};

class HttpHandler : public ConnectionHandler {
 public:
  using RequestType = ::boost::beast::http::request<::boost::beast::http::string_body>;
  using SendFunction = ListenerBase::SendFunction;

  HttpHandler(const ListenerBase* registry, IoContext* cntx);

  boost::system::error_code HandleRequest() final override;

 protected:
  const char* favicon_;
  const char* resource_prefix_;
  virtual bool Authorize(const QueryArgs& args) const { return true; }

 private:

  void HandleRequestInternal(const RequestType& req, SendFunction* send);

  const ListenerBase* registry_;
};

// http Listener + handler factory. By default creates HttpHandler.
template <typename Handler = HttpHandler>
class Listener : public ListenerBase {
 public:
  static_assert(std::is_base_of<HttpHandler, Handler>::value,
                "Handler must be derived from HttpHandler");

  ConnectionHandler* NewConnection(IoContext& cntx) final {
    return new Handler(this, &cntx);
  }
};

}  // namespace http
}  // namespace util
