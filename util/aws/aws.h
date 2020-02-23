// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

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

  void Sign(absl::string_view domain,
            ::boost::beast::http::header<true, ::boost::beast::http::fields>* req);

 private:
  std::string region_id_, service_, secret_, access_key_;

  mutable ::boost::fibers::mutex mu_;
  mutable std::string sign_key_;

  std::string credential_scope_;
  char date_str_[32];
};

}  // namespace util
