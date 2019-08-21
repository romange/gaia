// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/parser.hpp>

// Implementation part.
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>

#include <memory>

#include "absl/types/optional.h"
#include "absl/types/variant.h"

#include "file/file.h"
#include "strings/stringpiece.h"
#include "util/asio/fiber_socket.h"
#include "util/gce/gce.h"
#include "util/status.h"

namespace util {

class IoContext;

class HttpsClient {
 public:
  using error_code = ::boost::system::error_code;

  HttpsClient(absl::string_view host, IoContext* context, ::boost::asio::ssl::context* ssl_ctx);
  HttpsClient(const HttpsClient&) = delete;
  HttpsClient(HttpsClient&&) = delete;

  error_code Connect(unsigned msec);

  template <typename Req> error_code Send(const Req& req);

  template <typename Req, typename Resp> error_code Send(const Req& req, Resp* resp);

  template <typename Parser> error_code ReadHeader(Parser* parser);
  template <typename Parser> error_code Read(Parser* parser);

  error_code DrainResponse(
      ::boost::beast::http::response_parser<::boost::beast::http::buffer_body>* parser);

  SslStream* client() { return client_.get(); }

  void schedule_reconnect() {reconnect_needed_ = true;}

 private:
  error_code HandleError(const error_code& ec);

  error_code ReconnectIfNeeded() {
    if (reconnect_needed_)
      return InitSslClient();
    return error_code{};
  }

  error_code InitSslClient();

  IoContext& io_context_;
  ::boost::asio::ssl::context& ssl_cntx_;

  ::boost::beast::flat_buffer tmp_buffer_;

  std::string host_name_;
  std::unique_ptr<SslStream> client_;

  uint32_t reconnect_msec_ = 1000;
  bool reconnect_needed_ = true;
};

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

  bool IsBusy() const;

  util::StatusObject<file::ReadonlyFile*> OpenGcsFile(absl::string_view full_path);

  // Input: full gcs uri path that starts with "gs://"
  // returns bucket and object paths accordingly.
  static bool SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                                absl::string_view* path);

  // Inverse function. Returns full gcs URI that starts with "gs://"".
  static std::string ToGcsPath(absl::string_view bucket, absl::string_view obj_path);

  // Write Interface
  util::Status OpenForWrite(absl::string_view bucket, absl::string_view obj_path);
  util::Status Write(strings::ByteRange data);
  util::Status CloseWrite(bool abort_write);

 private:
  using Request = ::boost::beast::http::request<::boost::beast::http::empty_body>;
  template <typename Body> using Response = ::boost::beast::http::response<Body>;
  template <typename Body> using Parser = ::boost::beast::http::response_parser<Body>;

  // Parser can not be reset, so we use absl::optional to workaround.
  using ReusableParser = absl::optional<Parser<boost::beast::http::buffer_body>>;

  class ConnState;

  util::Status RefreshToken(Request* req);

  std::string BuildGetObjUrl(absl::string_view bucket, absl::string_view path);
  util::Status PrepareConnection();
  util::Status ClearConnState();

  OpenSeqResult OpenSequentialInternal(Request* req, ReusableParser* parser);

  // Higher level function. Handles token expiration use-cases.
  template <typename RespBody> util::Status HttpMessage(Request* req, Response<RespBody>* resp);

  // Returns true if the request succeeded and the response is ok.
  // Returns false if we had an intermittent error and need to retry.
  // Returns error status if we are completely screwed.
  util::StatusObject<bool> SendRequestIterative(Request* req,
                                                Parser<::boost::beast::http::buffer_body>* parser);

  std::string access_token_header_;
  std::unique_ptr<ConnState> conn_state_;
  std::unique_ptr<HttpsClient> https_client_;
};

bool IsGcsPath(absl::string_view path);

template <typename Req, typename Resp>
auto HttpsClient::Send(const Req& req, Resp* resp) -> error_code {
  namespace h2 = ::boost::beast::http;
  error_code ec = Send(req);
  if (ec)
    return ec;

  h2::read(*client_, tmp_buffer_, *resp, ec);
  return HandleError(ec);
}

template <typename Req> auto HttpsClient::Send(const Req& req) -> error_code {
  error_code ec = ReconnectIfNeeded();
  if (ec)
    return ec;
  ::boost::beast::http::write(*client_, req, ec);
  return HandleError(ec);
}

// Read methods should not reconnect since they assume some state (i.e. reading http request).
template <typename Parser> auto HttpsClient::ReadHeader(Parser* parser) -> error_code {
  error_code ec;

  ::boost::beast::http::read_header(*client_, tmp_buffer_, *parser, ec);
  return HandleError(ec);
}

template <typename Parser> auto HttpsClient::Read(Parser* parser) -> error_code {
  error_code ec;

  // Note that read returns number of raw bytes read from stream before parsing which
  // does not correlate to the final data stored inside the parser.
  ::boost::beast::http::read(*client_, tmp_buffer_, *parser, ec);
  return HandleError(ec);
}

inline auto HttpsClient::HandleError(const error_code& ec) -> error_code {
  if (ec && ec != ::boost::beast::http::error::need_buffer)
    reconnect_needed_ = true;
  return ec;
}

}  // namespace util
