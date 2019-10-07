// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>

#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"

#include "util/gce/gce.h"
#include "util/http/https_client_pool.h"
#include "util/status.h"

namespace util {
namespace detail {

namespace h2 = ::boost::beast::http;

using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

h2::request<h2::empty_body> PrepareGenericRequest(h2::verb req_verb, const bb_str_view url,
                                                  const bb_str_view token);

inline Status ToStatus(const ::boost::system::error_code& ec) {
  return ec ? Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()))
            : Status::OK;
}

inline bool DoesServerPushback(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}

inline bool IsUnauthorized(const h2::header<false, h2::fields>& header) {
  if (header.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = header.find("WWW-Authenticate");

  return it != header.end();
}

inline void AddBearer(absl::string_view token, h2::header<true, h2::fields>* req) {
  std::string token_header = absl::StrCat("Bearer ", token);
  req->set(h2::field::authorization, token_header);
}

class ApiSenderBase {
 public:
  using Request = h2::request<h2::empty_body>;
  using ClientHandle = http::HttpsClientPool::ClientHandle;

  ApiSenderBase(const GCE& gce, http::HttpsClientPool* pool) : gce_(gce), pool_(pool) {}

  virtual ~ApiSenderBase();

  StatusObject<ClientHandle> SendGeneric(unsigned num_iterations, Request req);

 protected:
  using error_code = ::boost::system::error_code;

  virtual error_code SendRequestIterative(const Request& req, http::HttpsClient* client) = 0;

  const GCE& gce_;
  http::HttpsClientPool* const pool_;
};

class ApiSenderBufferBody : public ApiSenderBase {
 public:
  using Parser = h2::response_parser<h2::buffer_body>;

  using ApiSenderBase::ApiSenderBase;

  //! Can be called only SendGeneric returned success.
  Parser* parser() { return parser_.has_value() ? &parser_.value() : nullptr; }

 private:
  error_code SendRequestIterative(const Request& req, http::HttpsClient* client) final;
  absl::optional<Parser> parser_;
};


}  // namespace detail
}  // namespace util
