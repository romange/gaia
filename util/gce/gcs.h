// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/empty_body.hpp>

#include <memory>

#include "strings/stringpiece.h"
#include "util/gce/gce.h"
#include "util/status.h"

namespace util {

class IoContext;

// Single threaded, fiber blocking class. Should be created 1 instance per http connection.
class GCS {
  const GCE& gce_;
  IoContext& io_context_;

 public:
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;
  using ReadObjectResult = util::StatusObject<size_t>;
  using ListObjectResult = util::Status;
  using error_code = ::boost::system::error_code;

  GCS(const GCE& gce, IoContext* context);
  ~GCS();

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  ListObjectResult List(absl::string_view bucket, absl::string_view prefix,
                        std::function<void(absl::string_view)> cb);

  ReadObjectResult Read(absl::string_view bucket, absl::string_view path, size_t ofs,
                        const strings::MutableByteRange& range);
  util::Status ReadToString(absl::string_view bucket, absl::string_view path, std::string* dest);

  util::Status OpenSequential(absl::string_view bucket, absl::string_view path);
  ReadObjectResult ReadSequential(const strings::MutableByteRange& range);

 private:
   using Request = ::boost::beast::http::request<::boost::beast::http::empty_body>;
  template <typename Body> using Response = ::boost::beast::http::response<Body>;

  util::Status ResetSeqReadState();
  util::Status RefreshToken(Request* req);

  void BuildGetObjUrl(absl::string_view bucket, absl::string_view path);


  // Higher level function. Handles token expiration use-cases.
  template <typename RespBody> util::Status HttpMessage(Request* req, Response<RespBody>* resp);

  template <typename RespBody> error_code WriteAndRead(Request* req, Response<RespBody>* resp);


  ::boost::beast::flat_buffer tmp_buffer_;
  std::string access_token_header_;
  std::unique_ptr<SslStream> client_;
  std::string read_obj_url_, last_obj_;

  struct SeqReadFile;
  std::unique_ptr<SeqReadFile> seq_file_;
};

}  // namespace util
