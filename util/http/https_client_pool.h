// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl/context.hpp>
#include <memory>

namespace util {

class IoContext;

namespace http {

class HttpsClient;

// IoContext specific, thread-local pool that manages a set of https connections.
class HttpsClientPool {
 public:
  struct HandleGuard;
  using ClientHandle = std::unique_ptr<HttpsClient, HandleGuard>;

  HttpsClientPool(const std::string& domain, ::boost::asio::ssl::context* ssl_ctx,
                  IoContext* io_cntx);
  ~HttpsClientPool();

  //! Must be called withing IoContext thread. Once ClientHandle destructs, the connection returns
  //! to the pool.
  ClientHandle GetHandle();

 private:

  using SslContext = ::boost::asio::ssl::context;

  SslContext& ssl_cntx_;
  IoContext& io_cntx_;

  std::vector<std::unique_ptr<HttpsClient>> available_handles_;
};

}  // namespace http
}  // namespace util
