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
  using ListObjectResult = util::Status;

  GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  ListObjectResult List(absl::string_view bucket, absl::string_view prefix,
                        std::function<void(absl::string_view)> cb);

  ReadObjectResult Read(absl::string_view bucket, absl::string_view path, size_t ofs,
                        const strings::MutableByteRange& range);
  util::Status ReadToString(absl::string_view bucket, absl::string_view path, std::string* dest);

 private:
  util::Status RefreshTokenIfNeeded();

  void BuildGetObjUrl(absl::string_view bucket, absl::string_view path);

  std::string access_token_, access_token_header_;
  std::unique_ptr<SslStream> client_;
  std::string read_obj_url_, last_obj_;
};

}  // namespace util

