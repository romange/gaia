// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ssl.hpp>
#include <memory>

#include "util/status.h"

namespace util {
class IoContext;
class FiberSyncSocket;

using SslStream = ::boost::asio::ssl::stream<FiberSyncSocket>;

class GCE {
 public:
  using SslContext = ::boost::asio::ssl::context;
  using error_code = ::boost::system::error_code;
  GCE() = default;

  Status Init();

  const std::string& project_id() const { return project_id_; }
  const std::string& client_id() const { return client_id_; }
  const std::string& client_secret() const { return client_secret_; }
  const std::string& account_id() const { return account_id_; }
  const std::string& refresh_token() const { return refresh_token_; }

  SslContext& ssl_context() const { return *ssl_ctx_; }

  StatusObject<std::string> GetAccessToken(IoContext* context) const;
  bool is_prod_env() const { return is_prod_env_; }

 private:
  util::Status ParseDefaultConfig();
  util::Status ReadDevCreds(const std::string& root_path);

  std::string project_id_, client_id_, client_secret_, account_id_, refresh_token_;
  std::unique_ptr<SslContext> ssl_ctx_;
  bool is_prod_env_ = false;
};

util::Status SslConnect(SslStream* stream, unsigned msec);

}  // namespace util
