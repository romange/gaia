// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>

#include <memory>

#include "absl/types/variant.h"
#include "file/file.h"
#include "strings/stringpiece.h"
#include "util/gce/gce.h"
#include "util/status.h"

namespace util {

class IoContext;

// Single threaded, fiber blocking class. Should be created 1 instance per http connection.
// All IO functions must run from IoContext thread passed to c'tor.
class GCS {
  const GCE& gce_;
  IoContext& io_context_;

 public:
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;
  using ReadObjectResult = util::StatusObject<size_t>;
  using OpenSeqResult = util::StatusObject<size_t>;  // Total size of the object.
  using ListObjectResult = util::Status;
  using error_code = ::boost::system::error_code;

  GCS(const GCE& gce, IoContext* context);
  ~GCS();

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  // Called with (size, key_name) pairs.
  using ListObjectCb = std::function<void(size_t, absl::string_view)>;

  // fs_mode = true - will return files only without "/" delimiter after the prefix.
  // fs_mode = false - will return all files recursively containing the prefix.
  ListObjectResult List(absl::string_view bucket, absl::string_view prefix, bool fs_mode,
                        ListObjectCb cb);

  ReadObjectResult Read(absl::string_view bucket, absl::string_view path, size_t ofs,
                        const strings::MutableByteRange& range);

  // Read API

  OpenSeqResult OpenSequential(absl::string_view bucket, absl::string_view path);
  ReadObjectResult ReadSequential(const strings::MutableByteRange& range);
  util::Status CloseSequential();

  bool IsOpenSequential() const;

  util::StatusObject<file::ReadonlyFile*> OpenGcsFile(absl::string_view full_path);

  // Input: full gcs uri path that starts with "gs://"
  // returns bucket and object paths accordingly.
  static bool SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                                absl::string_view* path);

  // Inverse function. Returns full gcs URI that starts with "gs://"".
  static std::string ToGcsPath(absl::string_view bucket, absl::string_view obj_path);

  // Write Interface
  util::Status OpenForWrite(absl::string_view bucket, absl::string_view obj_path);
  util::Status Write(absl::string_view data);

 private:
  using Request = ::boost::beast::http::request<::boost::beast::http::empty_body>;
  template <typename Body> using Response = ::boost::beast::http::response<Body>;
  template <typename Body> using Parser = ::boost::beast::http::response_parser<Body>;

  struct SeqReadHandler;
  struct WriteHandler;
  class ConnState;

  util::Status RefreshToken(Request* req);

  std::string BuildGetObjUrl(absl::string_view bucket, absl::string_view path);
  util::Status InitSslClient();
  util::Status PrepareConnection();

  OpenSeqResult OpenSequentialInternal(Request* req, SeqReadHandler* file);

  // Higher level function. Handles token expiration use-cases.
  template <typename RespBody> util::Status HttpMessage(Request* req, Response<RespBody>* resp);

  template <typename RespBody>
  error_code WriteAndRead(const Request& req, Response<RespBody>* resp);

  error_code DrainResponse(Parser<::boost::beast::http::buffer_body>* parser);

  // Returns true if the request succeeded and the response is ok.
  // Returns false if we had an intermittent error and need to retry.
  // Returns error status if we are completely screwed.
  util::StatusObject<bool> SendRequestIterative(Request* req,
                                                Parser<::boost::beast::http::buffer_body>* parser);


  ::boost::beast::flat_buffer tmp_buffer_;
  std::string access_token_header_;
  std::unique_ptr<SslStream> client_;

  std::unique_ptr<ConnState> conn_state_;

  uint32_t reconnect_msec_ = 1000;
  bool reconnect_needed_ = true;
};

bool IsGcsPath(absl::string_view path);

}  // namespace util
