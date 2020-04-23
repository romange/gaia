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
#include "util/http/http_common.h"

namespace util {
namespace http {

// This is the C++11 equivalent of a generic lambda.
// The function object is used to send an HTTP message.
template <typename FiberSyncStream>
struct SendLambda {
  template <typename Body>
  using Response = ::boost::beast::http::response<Body>;
  using error_code = ::boost::system::error_code;

  FiberSyncStream& stream_;
  error_code ec;

  explicit SendLambda(FiberSyncStream& stream) : stream_(stream) {
  }

  template <typename Body>
  void Invoke(Response<Body>&& msg) {
    // Determine if we should close the connection after
    // close_ = msg.need_eof();

    // We need the serializer here because the serializer requires
    // a non-const file_body, and the message oriented version of
    // http::write only works with const messages.
    msg.prepare_payload();
    ::boost::beast::http::response_serializer<Body> sr{msg};

    ::boost::beast::http::write(stream_, sr, ec);
  }
};

// Should be one per process. Represents http server interface.
// Currently does not support on the fly updates - requires
// multi-threading support.
class ListenerBase : public ListenerInterface {
  friend class HttpHandler;

 public:
  typedef SendLambda<FiberSyncSocket> SendFunction;
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
