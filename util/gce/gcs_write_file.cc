// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/fiber/operations.hpp>

#include "absl/strings/strip.h"
#include "base/logging.h"
#include "strings/escaping.h"

#include "util/asio/io_context.h"
#include "util/gce/detail/gcs_utils.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

DEFINE_uint32(gcs_upload_buf_log_size, 20, "Upload buffer size is 2^k of this parameter.");

using namespace boost;
using namespace http;
using namespace ::std;
namespace h2 = detail::h2;
using file::WriteFile;

namespace {

//! [from, to) limited range out of total. If total is < 0 then it's unknown.
string ContentRangeHeader(size_t from, size_t to, ssize_t total) {
  CHECK_LE(from, to);
  string tmp{"bytes "};

  if (from < to) {                                  // common case.
    absl::StrAppend(&tmp, from, "-", to - 1, "/");  // content-range is inclusive.
    if (total >= 0) {
      absl::StrAppend(&tmp, total);
    } else {
      tmp.push_back('*');
    }
  } else {
    // We can write empty ranges only when we finalize the file and total is known.
    CHECK_GE(total, 0);
    absl::StrAppend(&tmp, "*/", total);
  }

  return tmp;
}

class ApiSenderDynamicBody : public detail::ApiSenderBase {
 public:
  using Parser = h2::response_parser<h2::dynamic_body>;

  using ApiSenderBase::ApiSenderBase;

  //! Can be called only SendGeneric returned success.
  Parser* parser() { return parser_.has_value() ? &parser_.value() : nullptr; }

 private:
  error_code SendRequestIterative(const Request& req, http::HttpsClient* client) final;
  absl::optional<Parser> parser_;
};

class GcsWriteFile : public WriteFile, protected ApiSenderDynamicBody {
 public:
  /**
   * @brief Construct a new Gcs Write File object.
   *
   * @param name - aka "gs://somebucket/path_to_obj"
   * @param gce - initialized GCE object for access token.
   * @param pool - https connection pool connected to google api server.
   */
  GcsWriteFile(absl::string_view name, const GCE& gce, string obj_url, HttpsClientPool* pool);

  bool Close() final;

  bool Open() final;

  Status Write(const uint8* buffer, uint64 length) final;

 private:
  size_t FillBuf(const uint8* buffer, size_t length);

  Status Upload();

  string obj_url_;
  beast::multi_buffer body_mb_;
  size_t uploaded_ = 0;
};

GcsWriteFile::GcsWriteFile(absl::string_view name, const GCE& gce, string obj_url,
                           HttpsClientPool* pool)
    : WriteFile(name), ApiSenderDynamicBody(gce, pool), obj_url_(std::move(obj_url)),
      body_mb_(1 << FLAGS_gcs_upload_buf_log_size) {
  CHECK(!obj_url_.empty());
  CHECK_GE(FLAGS_gcs_upload_buf_log_size, 18);
}

bool GcsWriteFile::Close() {
  CHECK(pool_->io_context().InContextThread());

  h2::request<h2::dynamic_body> req(h2::verb::put, obj_url_, 11);
  req.body() = std::move(body_mb_);
  size_t to = uploaded_ + req.body().size();

  req.set(h2::field::content_range, ContentRangeHeader(uploaded_, to, to));
  req.prepare_payload();

  auto res = SendGeneric(3, std::move(req));
  if (res.ok()) {
    VLOG(1) << "Finalized file " << obj_url_ << " " << uploaded_ << "/"  << to;
  } else {
    LOG(ERROR) << "Error closing GCS file " << parser()->get() << " for request: \n" << req
               << ", status " << res.status;
  }
  delete this;

  return res.ok();
}

bool GcsWriteFile::Open() {
  LOG(FATAL) << "Should not be called";

  return true;
}

Status GcsWriteFile::Write(const uint8* buffer, uint64 length) {
  CHECK_GT(length, 0);
  CHECK(pool_->io_context().InContextThread());

  while (length) {
    size_t written = FillBuf(buffer, length);
    if (body_mb_.size() < body_mb_.max_size())
      break;
    length -= written;
    buffer += written;
    RETURN_IF_ERROR(Upload());
  }

  return Status::OK;
}

size_t GcsWriteFile::FillBuf(const uint8* buffer, size_t length) {
  size_t prepare_size = std::min(length, body_mb_.max_size() - body_mb_.size());
  auto mbs = body_mb_.prepare(prepare_size);
  size_t offs = 0;
  for (auto mb : mbs) {
    memcpy(mb.data(), buffer + offs, mb.size());
    offs += mb.size();
  }
  CHECK_EQ(offs, prepare_size);
  body_mb_.commit(prepare_size);

  return offs;
}

Status GcsWriteFile::Upload() {
  size_t body_size = body_mb_.size();
  CHECK_GT(body_size, 0);
  CHECK_EQ(0, body_size % (1U << 18)) << body_size;  // Must be multiple of 256KB.

  size_t to = uploaded_ + body_size;

  h2::request<h2::dynamic_body> req(h2::verb::put, obj_url_, 11);
  req.body() = std::move(body_mb_);
  req.set(h2::field::content_range, ContentRangeHeader(uploaded_, to, -1));
  req.set(h2::field::content_type, "application/octet-stream");
  req.prepare_payload();

  CHECK_EQ(0, body_mb_.size());

  auto res = SendGeneric(3, std::move(req));
  if (!res.ok())
    return res.status;
  VLOG(1) << "Uploaded range " << uploaded_ << "/" << to << " for " << obj_url_;

  Parser* upload_parser = CHECK_NOTNULL(parser());
  const auto& resp_msg = upload_parser->get();
  auto it = resp_msg.find(h2::field::range);
  CHECK(it != resp_msg.end()) << resp_msg;

  absl::string_view range = detail::absl_sv(it->value());
  CHECK(absl::ConsumePrefix(&range, "bytes="));
  size_t pos = range.find('-');
  CHECK_LT(pos, range.size());
  size_t uploaded_pos = 0;
  CHECK(absl::SimpleAtoi(range.substr(pos + 1), &uploaded_pos));
  CHECK_EQ(uploaded_pos + 1, to);

  uploaded_ = to;

  return Status::OK;
}

auto ApiSenderDynamicBody::SendRequestIterative(const Request& req, HttpsClient* client)
    -> error_code {
  system::error_code ec = client->Send(req);
  if (ec) {
    VLOG(1) << "Error sending to socket " << client->native_handle() << " " << ec;
    return ec;
  }

  parser_.emplace();  // .body_limit(kuint64max);
  ec = client->Read(&parser_.value());
  if (ec) {
    return ec;
  }

  if (!parser_->keep_alive()) {
    client->schedule_reconnect();
    LOG(INFO) << "Scheduling reconnect due to conn-close header";
  }

  const auto& msg = parser_->get();
  VLOG(1) << "HeaderResp(" << client->native_handle() << "): " << msg;

  // 308 or http ok are both good responses.
  if (msg.result() == h2::status::ok || msg.result() == h2::status::permanent_redirect) {
    return error_code{};  // all is good.
  }

  if (detail::DoesServerPushback(msg.result())) {
    LOG(INFO) << "Retrying(" << client->native_handle() << ") with " << msg;

    this_fiber::sleep_for(1s);
    return asio::error::try_again;  // retry
  }

  if (detail::IsUnauthorized(msg)) {
    return asio::error::no_permission;
  } else if (msg.result() == h2::status::gone) {
    LOG(INFO) << "Closing(" << client->native_handle() << ") with " << msg;

    this_fiber::sleep_for(1s);

    return system::errc::make_error_code(system::errc::connection_refused);
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return h2::error::bad_status;
}

}  // namespace

StatusObject<file::WriteFile*> OpenGcsWriteFile(absl::string_view full_path, const GCE& gce,
                                                http::HttpsClientPool* pool) {
  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendEncodedUrl(obj_path, &url);
  string token = gce.access_token();

  CHECK(!token.empty());

  auto req = detail::PrepareGenericRequest(h2::verb::post, url, token);
  req.prepare_payload();

  ApiSenderDynamicBody sender(gce, pool);
  auto res = sender.SendGeneric(3, std::move(req));
  if (!res.ok())
    return res.status;

  const auto& resp = sender.parser()->get();

  // HttpsClientPool::ClientHandle handle = std::move(res.obj);

  auto it = resp.find(h2::field::location);
  if (it == resp.end()) {
    return Status(StatusCode::PARSE_ERROR, "Can not find location header");
  }
  string upload_id = string(it->value());

  return new GcsWriteFile(full_path, gce, std::move(upload_id), pool);
}

}  // namespace util
