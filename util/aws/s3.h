// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/status.h"

#pragma once

namespace util {

namespace http {

class HttpsClient;
class HttpsClientPool;

}  // namespace http

class AWS;

class S3 {
 public:
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;

  S3(const AWS& aws, http::HttpsClientPool* pool);

  ListBucketResult ListBuckets();

private:
  const AWS& aws_;
  http::HttpsClientPool* pool_;
};

}  // namespace util
