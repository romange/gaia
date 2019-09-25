// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>

#include "absl/strings/str_cat.h"
#include "util/status.h"
#include "util/gce/gce.h"

namespace util {
namespace detail {

namespace h2 = ::boost::beast::http;

using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

inline h2::request<h2::empty_body> PrepareRequest(
  h2::verb req_verb, const bb_str_view url, const bb_str_view token) {
  h2::request<h2::empty_body> req(req_verb, url, 11);
  req.set(h2::field::host, GCE::kApiDomain);

  std::string access_token_header = absl::StrCat("Bearer ", absl_sv(token));
  req.set(h2::field::authorization, access_token_header);

  return req;
}

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

}
}
