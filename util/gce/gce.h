// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <boost/fiber/mutex.hpp>
#include <memory>

#include "util/status.h"

namespace util {
class IoContext;

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

  // refresh_token is used for refreshing an access token.
  const std::string& refresh_token() const { return refresh_token_; }

  static const char* GoogleCert();
  static const char* kApiDomain;

  static ::boost::asio::ssl::context CheckedSslContext();

  //! Returns cached access_token.
  //! Must be called after RefreshAccessToken has been called.
  std::string access_token() const;

  StatusObject<std::string> RefreshAccessToken(IoContext* context) const;
  bool is_prod_env() const { return is_prod_env_; }

  void Test_InjectAcessToken(std::string access_token);

 private:
  util::Status ParseDefaultConfig();
  util::Status ReadDevCreds(const std::string& root_path);
  util::StatusObject<std::string> ParseTokenResponse(std::string&& response) const;

  std::string project_id_, client_id_, client_secret_, account_id_, refresh_token_;

  mutable ::boost::fibers::mutex mu_;
  mutable std::string access_token_;

  std::unique_ptr<SslContext> ssl_ctx_;
  bool is_prod_env_ = false;
};

}  // namespace util
