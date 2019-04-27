// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <memory>

#include "util/gce/gce.h"
#include "util/status.h"
#include "strings/stringpiece.h"

namespace util {

class IoContext;

class GCS {
  const GCE& gce_;
  IoContext& io_context_;

 public:
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;
  using ReadObjectResult = util::StatusObject<size_t>;

  GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  void List(absl::string_view bucket, absl::string_view prefix);
  
  ReadObjectResult Read(const std::string& bucket, const std::string& path, size_t ofs,
                        const strings::MutableByteRange& range);
  util::Status ReadToString(const std::string& bucket, const std::string& path, std::string* dest);

 private:
  util::Status RefreshTokenIfNeeded();

  void BuildGetObjUrl(absl::string_view bucket, absl::string_view path);

  std::string access_token_, access_token_header_;
  std::unique_ptr<SslStream> client_;
  std::string read_obj_url_, last_obj_;
};

}  // namespace util

