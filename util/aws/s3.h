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

class S3Bucket {
 public:
  using ListObjectResult = util::Status;

  //! Called with (size, key_name) pairs.
  using ListObjectCb = std::function<void(size_t, absl::string_view)>;

  //! Constructs S3 bucket handler.
  //! pool should point to the bucket dns we are working with since S3 has bucket centric API.
  S3Bucket(const AWS& aws, http::HttpsClientPool* pool);

  /** @brief Lists objects for a particular bucket.
  *
  *  In s3 the bucket is determined by the dns "mybucket.s3.amazonaws.com"
  *  so unlike in GCS, in S3 api the http pool dns destination determines which bucket
  *  we are going to list.
  *  glob contains the object prefix path not including the bucket part.
  *  if fs_mode is true returns all paths upto the delimeter '/'.
  */
  ListObjectResult List(absl::string_view glob, bool fs_mode, ListObjectCb cb);

private:
  const AWS& aws_;
  http::HttpsClientPool* pool_;
};


using ListS3BucketResult = util::StatusObject<std::vector<std::string>>;

//! pool should be connected to s3.amazonaws.com
ListS3BucketResult ListS3Buckets(const AWS& aws, http::HttpsClientPool* pool);

}  // namespace util
