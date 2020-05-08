// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/fiber/mutex.hpp>

#include "absl/strings/string_view.h"
#include "util/status.h"

namespace util {

class AWS {
 public:
  AWS(const std::string& region_id, const std::string& service)
      : region_id_(region_id), service_(service) {
  }

  Status Init();

  // TODO: we should remove domain argument in favor to subdomain (bucket).
  // and build the whole domain it from service and region
  // for example, "<bucket>.s3.eu-west-1.amazonaws.com"
  // See: https://docs.aws.amazon.com/general/latest/gr/s3.html
  //
  void Sign(absl::string_view domain,
            ::boost::beast::http::header<true, ::boost::beast::http::fields>* req) const;

  static ::boost::asio::ssl::context CheckedSslContext();

 private:
  std::string region_id_, service_, secret_, access_key_;

  mutable ::boost::fibers::mutex mu_;
  mutable std::string sign_key_;

  std::string credential_scope_;
  char date_str_[32];
};

}  // namespace util
