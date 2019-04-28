// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "base/logging.h"
#include "strings/escaping.h"
#include "util/asio/fiber_socket.h"
#include "util/http/beast_rj_utils.h"

namespace util {
using namespace std;
using namespace boost;

namespace h2 = beast::http;
namespace rj = rapidjson;

static constexpr char kDomain[] = "www.googleapis.com";

inline util::Status ToStatus(const ::boost::system::error_code& ec) {
  return Status(::util::StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()));
}

inline h2::request<h2::empty_body> PrepareRequest(h2::verb req_verb, const beast::string_view url,
                                                  const beast::string_view access_token) {
  h2::request<h2::empty_body> req(req_verb, url, 11);
  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, access_token);

  return req;
}

template <typename ReqBody, typename RespBody>
::boost::system::error_code WriteAndRead(SslStream* stream, h2::request<ReqBody>* req,
                                         h2::response_parser<RespBody>* resp) {
  ::boost::system::error_code ec;
  h2::write(*stream, *req, ec);
  if (ec) {
    return ec;
  }

  beast::flat_buffer buffer;

  h2::read(*stream, buffer, *resp, ec);
  return ec;
}

util::Status GCS::Connect(unsigned msec) {
  client_.reset(new SslStream(FiberSyncSocket{kDomain, "443", &io_context_}, gce_.ssl_context()));

  return SslConnect(client_.get(), msec);
}

util::Status GCS::RefreshTokenIfNeeded() {
  if (!access_token_.empty())
    return Status::OK;
  auto res = gce_.GetAccessToken(&io_context_);
  if (!res.ok())
    return res.status;

  access_token_ = res.obj;
  access_token_header_ = absl::StrCat("Bearer ", access_token_);

  return Status::OK;
}

auto GCS::ListBuckets() -> ListBucketResult {
  CHECK(client_);

  RETURN_IF_ERROR(RefreshTokenIfNeeded());

  string url = absl::StrCat("/storage/v1/b?project=", gce_.project_id());
  absl::StrAppend(&url, "&fields=items,nextPageToken");

  auto http_req = PrepareRequest(h2::verb::get, url, access_token_header_);

  VLOG(1) << "Req: " << http_req;
  h2::response_parser<h2::dynamic_body> resp;
  auto ec = WriteAndRead(client_.get(), &http_req, &resp);
  if (ec) {
    return ToStatus(ec);
  }

  auto msg = resp.release();
  http::RjBufSequenceStream is(msg.body().data());

  // TODO: to have a handler extracting what we need.
  rj::Document doc;
  doc.ParseStream<rj::kParseDefaultFlags>(is);

  if (doc.HasParseError()) {
    LOG(ERROR) << rj::GetParseError_En(doc.GetParseError()) << msg;
    return Status(StatusCode::PARSE_ERROR, "Could not parse json response");
  }

  auto it = doc.FindMember("items");
  CHECK(it != doc.MemberEnd()) << msg;
  const auto& val = it->value;
  CHECK(val.IsArray());
  auto array = val.GetArray();

  vector<string> results;
  it = doc.FindMember("nextPageToken");
  CHECK(it == doc.MemberEnd()) << "TBD - to support pagination";

  for (size_t i = 0; i < array.Size(); ++i) {
    const auto& item = array[i];
    auto it = item.FindMember("name");
    if (it != item.MemberEnd()) {
      results.emplace_back(it->value.GetString(), it->value.GetStringLength());
    }
  }
  return results;
}

auto GCS::List(absl::string_view bucket, absl::string_view prefix,
               std::function<void(absl::string_view)> cb) -> ListObjectResult {
  CHECK(client_ && !bucket.empty());

  RETURN_IF_ERROR(RefreshTokenIfNeeded());

  string url = "/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?prefix=");
  strings::AppendEncodedUrl(prefix, &url);
  auto http_req = PrepareRequest(h2::verb::get, url, access_token_header_);

  // TODO: to have a handler extracting what we need.
  rj::Document doc;
  while (true) {
    h2::response_parser<h2::dynamic_body> resp;
    auto ec = WriteAndRead(client_.get(), &http_req, &resp);
    if (ec) {
      return ToStatus(ec);
    }

    auto msg = resp.release();
    http::RjBufSequenceStream is(msg.body().data());

    VLOG(1) << "List response: " << msg;

    doc.ParseStream<rj::kParseDefaultFlags>(is);

    if (doc.HasParseError()) {
      LOG(ERROR) << rj::GetParseError_En(doc.GetParseError()) << msg;
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
      cb(absl::string_view(it->value.GetString(), it->value.GetStringLength()));
    }
    it = doc.FindMember("nextPageToken");
    if (it == doc.MemberEnd()) {
      break;
    }
    string page_token = string(it->value.GetString(), it->value.GetStringLength());
    http_req.target(absl::StrCat(url, "&pageToken=", page_token));
  }
  return Status::OK;
}

auto GCS::Read(const std::string& bucket, const std::string& obj_path, size_t ofs,
               const strings::MutableByteRange& range) -> ReadObjectResult {
  CHECK(client_ && !range.empty());

  RETURN_IF_ERROR(RefreshTokenIfNeeded());

  BuildGetObjUrl(bucket, obj_path);

  auto req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header_);
  req.set(h2::field::range, absl::StrCat("bytes=", ofs, "-", ofs + range.size() - 1));

  VLOG(1) << "Req: " << req;
  h2::response_parser<h2::buffer_body> resp;
  auto& body = resp.get().body();
  body.data = range.data();
  body.size = range.size();
  body.more = false;

  auto ec = WriteAndRead(client_.get(), &req, &resp);
  h2::response<h2::buffer_body> msg = resp.release();
  if (ec) {
    return ToStatus(ec);
  }
  VLOG(1) << "Read Response: " << msg.base();
  if (msg.result() != h2::status::ok) {
    return Status(StatusCode::IO_ERROR, string(msg.reason()));
  }

  auto left_available = body.size;
  return range.size() - left_available;  // how much written
}

util::Status GCS::ReadToString(const std::string& bucket, const std::string& obj_path,
                               std::string* dest) {
  CHECK(client_);

  RETURN_IF_ERROR(RefreshTokenIfNeeded());

  BuildGetObjUrl(bucket, obj_path);

  auto req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header_);
  VLOG(1) << "Req: " << req;

  h2::response_parser<h2::dynamic_body> resp;
  auto ec = WriteAndRead(client_.get(), &req, &resp);
  if (ec) {
    return ToStatus(ec);
  }
  auto msg = resp.release();
  VLOG(1) << msg;

  const auto& cdata = msg.body().data();

  dest->reserve(asio::buffer_size(cdata));
  dest->clear();
  for (auto const buffer : cdata) {
    dest->append(static_cast<char const*>(buffer.data()), buffer.size());
  }
  return Status::OK;
}

void GCS::BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  if (last_obj_ != obj_path) {
    read_obj_url_ = "/storage/v1/b/";
    last_obj_ = string(obj_path);

    absl::StrAppend(&read_obj_url_, bucket, "/o/");
    strings::AppendEncodedUrl(obj_path, &read_obj_url_);
    absl::StrAppend(&read_obj_url_, "?alt=media");
  }
}

}  // namespace util
