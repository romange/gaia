// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "file/file.h"
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

  static const char* kRootDomain;

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

  static bool SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                                absl::string_view* path);

  static std::string ToFullPath(absl::string_view bucket, absl::string_view key_path);

private:
  const AWS& aws_;
  http::HttpsClientPool* pool_;
};


using ListS3BucketResult = util::StatusObject<std::vector<std::string>>;

//! pool should be connected to s3.amazonaws.com
ListS3BucketResult ListS3Buckets(const AWS& aws, http::HttpsClientPool* pool);

/**
 * @brief Opens an s3 object for sequential reading.
 *
 * @param key_path an object path without bucket prefix. The bucket is already predefined in
 *                 pool connection.
 * @param aws      an AWS handler
 * @param pool     a pool handling https connections to the bucket.
 * @param opts     ReadonlyFile::Options argument.
 * @return StatusObject<file::ReadonlyFile*>
 */
StatusObject<file::ReadonlyFile*> OpenS3ReadFile(
    absl::string_view key_path, const AWS& aws, http::HttpsClientPool* pool,
    const file::ReadonlyFile::Options& opts = file::ReadonlyFile::Options{});

bool IsS3Path(absl::string_view path);

}  // namespace util
