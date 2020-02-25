// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "absl/strings/strip.h"
#include "absl/types/variant.h"

#include "base/logging.h"
#include "base/walltime.h"
#include "strings/escaping.h"
#include "util/asio/fiber_socket.h"
#include "util/asio/io_context.h"
#include "util/gce/detail/gcs_utils.h"
#include "util/http/beast_rj_utils.h"
#include "util/http/https_client.h"
#include "util/stats/varz_stats.h"

namespace util {

DECLARE_uint32(gcs_upload_buf_log_size);

using namespace std;
using namespace boost;

namespace h2 = beast::http;
namespace rj = rapidjson;

static constexpr char kDomain[] = "www.googleapis.com";

namespace {


inline Status ToStatus(const ::boost::system::error_code& ec) {
  return ec ? Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()))
            : Status::OK;
}

inline Status HttpError(const h2::header<false, h2::fields>& resp) {
  return Status(StatusCode::IO_ERROR,
                absl::StrCat("Http error: ", resp.result_int(), " ",
                detail::absl_sv(resp.reason())));
}

#define RETURN_EC_STATUS(x)                                 \
  do {                                                      \
    auto __ec$ = (x);                                       \
    if (__ec$) {                                            \
      VLOG(1) << "EC: " << __ec$ << " " << __ec$.message(); \
      return ToStatus(x);                                   \
    }                                                       \
  } while (false)

inline h2::request<h2::empty_body> PrepareRequest(h2::verb req_verb, const beast::string_view url,
                                                  const beast::string_view access_token) {
  h2::request<h2::empty_body> req(req_verb, url, 11);
  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, access_token);
  req.keep_alive(true);

  return req;
}

template <typename Msg> inline bool IsUnauthorized(const Msg& msg) {
  if (msg.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = msg.find("WWW-Authenticate");

  return it != msg.end();
}


//! [from, to) range. If to is kuint64max - then the range is unlimited from above.
inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

}  // namespace

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  os << "-------------------------";

  return os;
}

template <typename Body> std::ostream& operator<<(std::ostream& os, const h2::request<Body>& msg) {
  os << msg.method_string() << " " << msg.target() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  os << "-------------------------";

  return os;
}

GCS::GCS(const GCE& gce, asio::ssl::context* ssl_cntx, IoContext* io_context)
    : gce_(gce), io_context_(*io_context),
      https_client_(new http::HttpsClient(kDomain, io_context, ssl_cntx)) {
  https_client_->set_retry_count(3);
}

GCS::~GCS() {
  VLOG(1) << "GCS::~GCS";
  https_client_.reset();
}

Status GCS::Connect(unsigned msec) {
  detail::InitVarzStats();

  auto ec = https_client_->Connect(msec);
  if (ec) {
    VLOG(1) << "Error connecting " << ec << " " << https_client_->client()->next_layer().status();
    return ToStatus(ec);
  }

  string token = gce_.access_token();
  access_token_header_ = absl::StrCat("Bearer ", token);

  VLOG(1) << "GCS::Connect OK " << native_handle();

  return Status::OK;
}

auto GCS::ListBuckets() -> ListBucketResult {
  RETURN_IF_ERROR(PrepareConnection());

  string url = absl::StrCat("/storage/v1/b?project=", gce_.project_id());
  absl::StrAppend(&url, "&fields=items,nextPageToken");

  auto http_req = PrepareRequest(h2::verb::get, url, access_token_header_);

  // TODO: to have a handler extracting what we need.
  rj::Document doc;
  vector<string> results;

  while (true) {
    h2::response<h2::dynamic_body> resp_msg;

    RETURN_IF_ERROR(SendWithToken(&http_req, &resp_msg));
    if (resp_msg.result() != h2::status::ok) {
      return HttpError(resp_msg);
    }

    http::RjBufSequenceStream is(resp_msg.body().data());

    doc.ParseStream<rj::kParseDefaultFlags>(is);
    if (doc.HasParseError()) {
      LOG(ERROR) << rj::GetParseError_En(doc.GetParseError()) << resp_msg;
      return Status(StatusCode::PARSE_ERROR, "Could not parse json response");
    }

    auto it = doc.FindMember("items");
    CHECK(it != doc.MemberEnd()) << resp_msg;
    const auto& val = it->value;
    CHECK(val.IsArray());
    auto array = val.GetArray();

    for (size_t i = 0; i < array.Size(); ++i) {
      const auto& item = array[i];
      auto it = item.FindMember("name");
      if (it != item.MemberEnd()) {
        results.emplace_back(it->value.GetString(), it->value.GetStringLength());
      }
    }
    it = doc.FindMember("nextPageToken");
    if (it == doc.MemberEnd()) {
      break;
    }
    absl::string_view page_token{it->value.GetString(), it->value.GetStringLength()};
    http_req.target(absl::StrCat(url, "&pageToken=", page_token));
  }
  return results;
}

auto GCS::List(absl::string_view bucket, absl::string_view prefix, bool fs_mode, ListObjectCb cb)
    -> ListObjectResult {
  CHECK(!bucket.empty());
  VLOG(1) << "GCS::List " << native_handle();

  RETURN_IF_ERROR(PrepareConnection());

  string url = "/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?prefix=");
  strings::AppendEncodedUrl(prefix, &url);
  if (fs_mode) {
    absl::StrAppend(&url, "&delimiter=%2f");
  }
  auto http_req = PrepareRequest(h2::verb::get, url, access_token_header_);

  // TODO: to have a handler extracting what we need.
  rj::Document doc;
  while (true) {
    h2::response<h2::dynamic_body> resp_msg;
    RETURN_IF_ERROR(SendWithToken(&http_req, &resp_msg));
    CHECK_EQ(h2::status::ok, resp_msg.result()) << resp_msg;

    http::RjBufSequenceStream is(resp_msg.body().data());

    doc.ParseStream<rj::kParseDefaultFlags>(is);

    if (doc.HasParseError()) {
      LOG(ERROR) << rj::GetParseError_En(doc.GetParseError()) << resp_msg;
      return Status(StatusCode::PARSE_ERROR, "Could not parse json response");
    }

    auto it = doc.FindMember("items");
    if (it == doc.MemberEnd())
      break;

    const auto& val = it->value;
    CHECK(val.IsArray());
    auto array = val.GetArray();

    for (size_t i = 0; i < array.Size(); ++i) {
      const auto& item = array[i];
      auto it = item.FindMember("name");
      CHECK(it != item.MemberEnd());
      absl::string_view key_name(it->value.GetString(), it->value.GetStringLength());
      it = item.FindMember("size");
      CHECK(it != item.MemberEnd());
      absl::string_view sz_str(it->value.GetString(), it->value.GetStringLength());
      size_t item_size = 0;
      CHECK(absl::SimpleAtoi(sz_str, &item_size));
      cb(item_size, key_name);
    }
    it = doc.FindMember("nextPageToken");
    if (it == doc.MemberEnd()) {
      break;
    }
    absl::string_view page_token{it->value.GetString(), it->value.GetStringLength()};
    http_req.target(absl::StrCat(url, "&pageToken=", page_token));
  }
  return Status::OK;
}

auto GCS::Read(absl::string_view bucket, absl::string_view obj_path, size_t ofs,
               const strings::MutableByteRange& range) -> ReadObjectResult {
  CHECK(!range.empty());
  RETURN_IF_ERROR(PrepareConnection());

  string read_obj_url = BuildGetObjUrl(bucket, obj_path);

  auto req = PrepareRequest(h2::verb::get, read_obj_url, access_token_header_);
  SetRange(ofs, ofs + range.size(), &req);

  h2::response<h2::buffer_body> resp_msg;
  auto& body = resp_msg.body();
  body.data = range.data();
  body.size = range.size();
  body.more = false;

  RETURN_IF_ERROR(SendWithToken(&req, &resp_msg));
  if (resp_msg.result() != h2::status::partial_content) {
    return Status(StatusCode::IO_ERROR, string(resp_msg.reason()));
  }

  auto left_available = body.size;
  return range.size() - left_available;  // how much written
}


string GCS::BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendEncodedUrl(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

Status GCS::PrepareConnection() {
  return Status::OK;
}



Status GCS::RefreshToken(Request* req) {
  auto res = gce_.RefreshAccessToken(&io_context_);
  if (!res.ok())
    return res.status;

  access_token_header_ = absl::StrCat("Bearer ", res.obj);
  req->set(h2::field::authorization, access_token_header_);

  return Status::OK;
}

template <typename RespBody> Status GCS::SendWithToken(Request* req, Response<RespBody>* resp) {
  for (unsigned i = 0; i < 2; ++i) {  // Iterate for possible token refresh.
    VLOG(1) << "HttpReq" << i << ": " << *req << ", socket " << native_handle();

    error_code ec = https_client_->Send(*req, resp);
    if (ec) {
      return ToStatus(ec);
    }
    VLOG(1) << "HttpResp" << i << ": " << *resp;

    if (resp->result() == h2::status::ok) {
      break;
    };

    if (IsUnauthorized(*resp)) {
      RETURN_IF_ERROR(RefreshToken(req));
      *resp = Response<RespBody>{};
      continue;
    }
    LOG(FATAL) << "Unexpected response " << *resp;
  }
  return Status::OK;
}

constexpr char kGsUrl[] = "gs://";

bool GCS::SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                            absl::string_view* path) {
  if (!absl::ConsumePrefix(&input, kGsUrl))
    return false;

  auto pos = input.find('/');
  *bucket = input.substr(0, pos);
  *path = (pos == absl::string_view::npos) ? absl::string_view{} : input.substr(pos + 1);
  return true;
}

std::string GCS::ToGcsPath(absl::string_view bucket, absl::string_view obj_path) {
  return absl::StrCat(kGsUrl, bucket, "/", obj_path);
}

uint32_t GCS::native_handle() { return https_client_->client()->next_layer().native_handle(); }

bool IsGcsPath(absl::string_view path) { return absl::StartsWith(path, kGsUrl); }

}  // namespace util
