// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/fiber/operations.hpp>
#include <boost/beast/http/dynamic_body.hpp>
// #include <boost/beast/http/parser.hpp>

#include "base/logging.h"
#include "strings/escaping.h"

#include "util/gce/detail/gcs_utils.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

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

class GcsWriteFile : public WriteFile, private ApiSenderDynamicBody {
 public:
  /**
   * @brief Construct a new Gcs Write File object.
   *
   * @param name - aka "gs://somebucket/path_to_obj"
   * @param gce - initialized GCE object for access token.
   * @param pool - https connection pool connected to google api server.
   */
  GcsWriteFile(absl::string_view name, const GCE& gce, HttpsClientPool* pool)
      : WriteFile(name), ApiSenderDynamicBody(gce, pool) {}

  bool Close() final;

  bool Open() final;

 private:
  string obj_url_;
};

bool GcsWriteFile::Close() { return true; }

bool GcsWriteFile::Open() { return true; }

auto ApiSenderDynamicBody::SendRequestIterative(const Request& req, HttpsClient* client)
    -> error_code {
  VLOG(1) << "Req: " << req;

  system::error_code ec = client->Send(req);
  if (ec)
    return ec;

  parser_.emplace(); // .body_limit(kuint64max);
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
  h2::response<h2::dynamic_body> resp_msg;
  req.prepare_payload();

  ApiSenderDynamicBody sender(gce, pool);
  auto res = sender.SendGeneric(3, std::move(req));
  if (!res.ok())
    return res.status;

  return nullptr;
  #if 0
  string upload_id;

  RETURN_IF_ERROR(SendWithToken(&req, &resp_msg));
    if (resp_msg.result() != h2::status::ok) {
      return HttpError(resp_msg);
    }
    auto it = resp_msg.find(h2::field::location);
    if (it == resp_msg.end()) {
      return Status(StatusCode::PARSE_ERROR, "Can not find location header");
    }
    upload_id = string(it->value());
  #endif
}

}  // namespace util
