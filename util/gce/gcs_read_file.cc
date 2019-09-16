// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/gce/gcs_read_file.h"

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/fiber/operations.hpp>

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "strings/escaping.h"

#include "util/gce/gcs.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

using namespace boost;
using namespace http;
using namespace ::std;
namespace h2 = beast::http;
using file::ReadonlyFile;

namespace {

// TODO: to factor out common utilities/constants to a separate file.

constexpr char kDomain[] = "www.googleapis.com";

string BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendEncodedUrl(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

inline absl::string_view absl_sv(beast::string_view s) {
  return absl::string_view{s.data(), s.size()};
}

inline h2::request<h2::empty_body> PrepareRequest(h2::verb req_verb, const beast::string_view url,
                                                  const beast::string_view access_token) {
  h2::request<h2::empty_body> req(req_verb, url, 11);
  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, access_token);
  CHECK(req.keep_alive());

  return req;
}

inline Status ToStatus(const system::error_code& ec) {
  return ec ? Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()))
            : Status::OK;
}

inline bool DoesServerPushback(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}

inline bool IsUnauthorized(const h2::header<false, h2::fields>& header) {
  if (header.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = header.find("WWW-Authenticate");

  return it != header.end();
}

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  os << "-------------------------";

  return os;
}

using Parser = h2::response_parser<h2::buffer_body>;
using EmptyRequest = h2::request<h2::empty_body>;

class GcsReadFile : public ReadonlyFile {
 public:
  using error_code = ::boost::system::error_code;

  // does not own gcs object, only wraps it with ReadonlyFile interface.
  GcsReadFile(const GCE& gce, HttpsClientPool* pool, string read_obj_url)
      : gce_(gce), pool_(pool), read_obj_url_(std::move(read_obj_url)) {}
  ~GcsReadFile();

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) final;

  // releases the system handle for this file.
  Status Close() final;

  size_t Size() const final { return size_; }

  int Handle() const final { return -1; }

  Status Open();

 private:
  system::error_code SendRequestIterative(HttpsClient* client, EmptyRequest* req);

  const GCE& gce_;
  HttpsClientPool* pool_;
  string read_obj_url_;
  size_t size_;
  size_t offs_ = 0;

  using OptParser = absl::optional<h2::response_parser<h2::buffer_body>>;
  OptParser parser_;

  HttpsClientPool::ClientHandle https_handle_;
};

Status GcsReadFile::Open() {
  auto token_res = gce_.GetAccessToken(&pool_->io_context());

  if (!token_res.ok())
    return token_res.status;

  HttpsClientPool::ClientHandle handle = pool_->GetHandle();
  system::error_code ec = handle->status();

  if (ec) {
    return ToStatus(ec);
  }
  string access_token_header = absl::StrCat("Bearer ", token_res.obj);

  auto req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header);

  for (unsigned iters = 0; iters < 3; ++iters) {
    VLOG(1) << "OpenIter" << iters << ": socket " << handle->native_handle();

    parser_.emplace().body_limit(kuint64max);

    ec = SendRequestIterative(handle.get(), &req);

    if (!ec) {
      const auto& msg = parser_->get();
      auto content_len_it = msg.find(h2::field::content_length);
      if (content_len_it != msg.end()) {
        CHECK(absl::SimpleAtoi(absl_sv(content_len_it->value()), &size_));
      }
      return Status::OK;
    }

    if (ec == asio::error::no_permission) {
      auto token_res = gce_.GetAccessToken(&pool_->io_context(), true);

      if (!token_res.ok())
        return token_res.status;
      access_token_header = absl::StrCat("Bearer ", token_res.obj);
      req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header);
    }
  }

  return Status(StatusCode::IO_ERROR, "Maximum iterations reached");
}

system::error_code GcsReadFile::SendRequestIterative(HttpsClient* client, EmptyRequest* req) {
  VLOG(1) << "Req: " << *req;

  system::error_code ec = client->Send(*req);
  if (ec)
    return ec;

  ec = client->ReadHeader(&parser_.value());

  if (ec) {
    return ec;
  }

  if (!parser_->keep_alive()) {
    client->schedule_reconnect();
    LOG(INFO) << "Scheduling reconnect due to conn-close header";
  }

  const auto& msg = parser_->get();
  VLOG(1) << "HeaderResp: " << msg;

  // Partial content can appear because of the previous reconnect.
  if (msg.result() == h2::status::ok || msg.result() == h2::status::partial_content) {
    return error_code{};  // all is good.
  }

  // Parse & drain whatever comes after problematic status.
  // We must do it as long as we plan to use this connection for more requests.
  ec = client->DrainResponse(&parser_.value());

  if (DoesServerPushback(msg.result())) {
    this_fiber::sleep_for(1s);
    return asio::error::try_again;  // retry
  }

  if (IsUnauthorized(msg)) {
    return asio::error::no_permission;
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return h2::error::bad_status;
}

}  // namespace

StatusObject<ReadonlyFile*> OpenGcsReadFile(absl::string_view full_path, const GCE& gce,
                                            HttpsClientPool* pool,
                                            const ReadonlyFile::Options& opts) {
  CHECK(opts.sequential && pool);
  CHECK(IsGcsPath(full_path));

  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  string read_obj_url = BuildGetObjUrl(bucket, obj_path);

  std::unique_ptr<GcsReadFile> fl(new GcsReadFile(gce, pool, std::move(read_obj_url)));
  RETURN_IF_ERROR(fl->Open());

  return fl.release();
}

}  // namespace util
