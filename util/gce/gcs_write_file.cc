// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/fiber/operations.hpp>

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
};

GcsWriteFile::GcsWriteFile(absl::string_view name, const GCE& gce, string obj_url,
                           HttpsClientPool* pool)
    : WriteFile(name), ApiSenderDynamicBody(gce, pool), obj_url_(std::move(obj_url)),
      body_mb_(1 << FLAGS_gcs_upload_buf_log_size) {
  CHECK(!obj_url_.empty());
  CHECK_GE(FLAGS_gcs_upload_buf_log_size, 18);
}

bool GcsWriteFile::Close() { return true; }

bool GcsWriteFile::Open() { return true; }

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
  return Status::OK;
}

auto ApiSenderDynamicBody::SendRequestIterative(const Request& req, HttpsClient* client)
    -> error_code {
  VLOG(1) << "Req: " << req;

  system::error_code ec = client->Send(req);
  if (ec)
    return ec;

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
  VLOG(1) << "HeaderResp: " << msg;

  if (msg.result() == h2::status::ok) {
    return error_code{};  // all is good.
  }

  if (detail::DoesServerPushback(msg.result())) {
    this_fiber::sleep_for(1s);
    return asio::error::try_again;  // retry
  }

  if (detail::IsUnauthorized(msg)) {
    return asio::error::no_permission;
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
