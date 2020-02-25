// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <memory>

#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>

#include "absl/types/optional.h"

#include "file/file.h"
#include "strings/stringpiece.h"
#include "util/gce/gce.h"
#include "util/status.h"

namespace util {

namespace http {

class HttpsClient;
class HttpsClientPool;

}  // namespace http

class IoContext;

// Single threaded, fiber blocking class. Should be created 1 instance per http connection.
// All IO functions must run from IoContext thread passed to c'tor.
class GCS {
  const GCE& gce_;
  IoContext& io_context_;

 public:
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;
  using ReadObjectResult = util::StatusObject<size_t>;
  using ListObjectResult = util::Status;
  using error_code = ::boost::system::error_code;

  GCS(const GCE& gce, ::boost::asio::ssl::context* ssl_cntx, IoContext* io_context);
  ~GCS();

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  //! Called with (size, key_name) pairs.
  using ListObjectCb = std::function<void(size_t, absl::string_view)>;

  //! fs_mode = true - will return files only without "/" delimiter after the prefix.
  //! fs_mode = false - will return all files recursively containing the prefix.
  ListObjectResult List(absl::string_view bucket, absl::string_view prefix, bool fs_mode,
                        ListObjectCb cb);

  ReadObjectResult Read(absl::string_view bucket, absl::string_view path, size_t ofs,
                        const strings::MutableByteRange& range);

  //! Input: full gcs uri path that starts with "gs://"
  //! fills correspondent bucket and object paths. Returns true if succeeds and false otherwise.
  static bool SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                                absl::string_view* path);

  //! Inverse function to SplitToBucketPath. Returns full gcs URI that starts with "gs://"".
  static std::string ToGcsPath(absl::string_view bucket, absl::string_view obj_path);

 private:

  using Request = ::boost::beast::http::request<::boost::beast::http::empty_body>;
  template <typename Body> using Response = ::boost::beast::http::response<Body>;
  template <typename Body> using Parser = ::boost::beast::http::response_parser<Body>;

  // Parser can not be reset, so we use absl::optional to workaround.
  using ReusableParser = absl::optional<Parser<boost::beast::http::buffer_body>>;

  util::Status RefreshToken(Request* req);

  std::string BuildGetObjUrl(absl::string_view bucket, absl::string_view path);
  util::Status PrepareConnection();

  //! Higher level function. Handles auth token expiration use-case.
  template <typename RespBody> util::Status SendWithToken(Request* req, Response<RespBody>* resp);

  uint32_t native_handle();

  std::string access_token_header_;
  std::unique_ptr<http::HttpsClient> https_client_;
};

bool IsGcsPath(absl::string_view path);

/**
 * @brief Opens a new GCS file for writes.
 *
 * Must be called from the IO thread that manages 'pool'. All accesses to this file
 * must be done from the same IO thread.
 *
 * @param full_path - aka "gs://somebucket/path_to_obj"
 * @param gce
 * @param pool - https connection pool connected to google api server.
 * @return StatusObject<file::WriteFile*>
 */
StatusObject<file::WriteFile*> OpenGcsWriteFile(
    absl::string_view full_path, const GCE& gce, http::HttpsClientPool* pool);


/**
 * @brief Opens read-only, GCS-backed file.
 *
 * It's single threaded and fiber-friendly. Must be called within IoContext thread sponsoring
 * HttpsClientPool. Only sequential read files are currently supported.
 *
 * @param full_path - aka "gs://my_bucket/path/obj_name"
 * @param gce
 * @param pool
 * @param opts
 * @return StatusObject<file::ReadonlyFile*>
 */
StatusObject<file::ReadonlyFile*> OpenGcsReadFile(
    absl::string_view full_path, const GCE& gce, http::HttpsClientPool* pool,
    const file::ReadonlyFile::Options& opts = file::ReadonlyFile::Options{});
}  // namespace util
