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
  class HandleGuard {
   public:
    HandleGuard(HttpsClientPool* pool = nullptr) : pool_(pool) {}

    void operator()(HttpsClient* client);

   private:
    HttpsClientPool* pool_;
  };

  using ClientHandle = std::unique_ptr<HttpsClient, HandleGuard>;

  HttpsClientPool(const std::string& domain, ::boost::asio::ssl::context* ssl_ctx,
                  IoContext* io_cntx);

  ~HttpsClientPool();

  /*! @brief Returns https client connection from the pool.
   *
   * Must be called withing IoContext thread. Once ClientHandle destructs,
   * the connection returns to the pool. GetHandle() might block the calling fiber for
   * connect_msec_ millis in case it creates a new connection.
   */
  ClientHandle GetHandle();

  void set_connect_timeout(unsigned msec) { connect_msec_ = msec; }

  IoContext& io_context() { return io_cntx_; }

 private:
  using SslContext = ::boost::asio::ssl::context;

  SslContext& ssl_cntx_;
  IoContext& io_cntx_;
  std::string domain_;
  unsigned connect_msec_ = 1000;

  std::vector<std::unique_ptr<HttpsClient>> available_handles_;
};

}  // namespace http
}  // namespace util
