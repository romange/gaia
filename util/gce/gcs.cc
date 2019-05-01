// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "absl/strings/strip.h"
#include "base/logging.h"
#include "strings/escaping.h"
#include "util/asio/fiber_socket.h"
#include "util/asio/io_context.h"
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

inline absl::string_view absl_sv(beast::string_view s) {
  return absl::string_view{s.data(), s.size()};
}

inline h2::request<h2::empty_body> PrepareRequest(h2::verb req_verb, const beast::string_view url,
                                                  const beast::string_view access_token) {
  h2::request<h2::empty_body> req(req_verb, url, 11);
  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, access_token);

  return req;
}

template <typename Msg> inline bool IsUnauthorized(const Msg& msg) {
  if (msg.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = msg.find("WWW-Authenticate");

  return it != msg.end();
}

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << endl;
  for (const auto& f : msg) {
    os << f.name() << " : " << f.value() << endl;
  }
  return os;
}

struct GCS::SeqReadFile {
  h2::response_parser<h2::buffer_body> parser;

  SeqReadFile() { parser.body_limit(kuint64max); }
};

GCS::GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}
GCS::~GCS() {}

util::Status GCS::Connect(unsigned msec) {
  CHECK(io_context_.InContextThread());
  if (client_) {
    return Status::OK;
  }

  client_.reset(new SslStream(FiberSyncSocket{kDomain, "443", &io_context_}, gce_.ssl_context()));

  RETURN_IF_ERROR(SslConnect(client_.get(), msec));

  auto res = gce_.GetAccessToken(&io_context_);
  if (!res.ok())
    return res.status;
  access_token_header_ = absl::StrCat("Bearer ", res.obj);
  return Status::OK;
}

util::Status GCS::ResetSeqReadState() {
  CHECK(io_context_.InContextThread());

  if (seq_file_) {
    ReadObjectResult res;
    uint8_t buf[1024];
    strings::MutableByteRange mbr(buf, sizeof(buf));
    while (true) {
      res = ReadSequential(mbr);
      if (!res.ok() || res.obj < mbr.size()) {
        break;
      }
    }
    seq_file_.reset();
    return res.status;
  }
  return Status::OK;
}

auto GCS::ListBuckets() -> ListBucketResult {
  CHECK(client_);
  RETURN_IF_ERROR(ResetSeqReadState());

  string url = absl::StrCat("/storage/v1/b?project=", gce_.project_id());
  absl::StrAppend(&url, "&fields=items,nextPageToken");

  auto http_req = PrepareRequest(h2::verb::get, url, access_token_header_);

  // TODO: to have a handler extracting what we need.
  rj::Document doc;
  vector<string> results;

  while (true) {
    h2::response<h2::dynamic_body> resp_msg;

    RETURN_IF_ERROR(HttpMessage(&http_req, &resp_msg));
    if (resp_msg.result() != h2::status::ok) {
      return Status(StatusCode::IO_ERROR, absl::StrCat("Http error: ", resp_msg.result_int(), " ",
                                                       absl_sv(resp_msg.reason())));
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

auto GCS::List(absl::string_view bucket, absl::string_view prefix, bool fs_mode,
               std::function<void(absl::string_view)> cb) -> ListObjectResult {
  CHECK(client_ && !bucket.empty());
  RETURN_IF_ERROR(ResetSeqReadState());

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
    RETURN_IF_ERROR(HttpMessage(&http_req, &resp_msg));
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
      cb(absl::string_view(it->value.GetString(), it->value.GetStringLength()));
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
  CHECK(client_ && !range.empty());
  RETURN_IF_ERROR(ResetSeqReadState());

  BuildGetObjUrl(bucket, obj_path);

  auto req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header_);
  req.set(h2::field::range, absl::StrCat("bytes=", ofs, "-", ofs + range.size() - 1));

  h2::response<h2::buffer_body> resp_msg;
  auto& body = resp_msg.body();
  body.data = range.data();
  body.size = range.size();
  body.more = false;

  RETURN_IF_ERROR(HttpMessage(&req, &resp_msg));
  if (resp_msg.result() != h2::status::partial_content) {
    return Status(StatusCode::IO_ERROR, string(resp_msg.reason()));
  }

  auto left_available = body.size;
  return range.size() - left_available;  // how much written
}

util::Status GCS::OpenSequential(absl::string_view bucket, absl::string_view obj_path) {
  CHECK(client_);

  RETURN_IF_ERROR(ResetSeqReadState());

  DCHECK(!seq_file_);

  BuildGetObjUrl(bucket, obj_path);

  auto req = PrepareRequest(h2::verb::get, read_obj_url_, access_token_header_);
  error_code ec;
  unique_ptr<SeqReadFile> seq_file;
  for (unsigned i = 0; i < 2; ++i) {
    h2::write(*client_, req, ec);
    if (ec) {
      return ToStatus(ec);
    }

    seq_file.reset(new SeqReadFile);

    h2::read_header(*client_, tmp_buffer_, seq_file->parser, ec);
    if (ec) {
      return ToStatus(ec);
    }
    if (!IsUnauthorized(seq_file->parser.get())) {
      const auto& msg = seq_file->parser.get();
      CHECK_EQ(h2::status::ok, seq_file->parser.get().result()) << msg;
      break;
    }
    RETURN_IF_ERROR(RefreshToken(&req));
  }
  seq_file_ = std::move(seq_file);

  return Status::OK;
}

auto GCS::ReadSequential(const strings::MutableByteRange& range) -> ReadObjectResult {
  CHECK(seq_file_ && client_);

  if (seq_file_->parser.is_done()) {
    return 0;
  }

  auto& body = seq_file_->parser.get().body();
  body.data = range.data();
  auto& left_available = body.size;
  left_available = range.size();

  error_code ec;
  h2::read(*client_, tmp_buffer_, seq_file_->parser, ec);
  if (ec && ec != h2::error::need_buffer) {
    return ToStatus(ec);
  }
  return range.size() - left_available;  // how much written
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

template <typename RespBody>
auto GCS::WriteAndRead(Request* req, Response<RespBody>* resp) -> error_code {
  error_code ec;
  h2::write(*client_, *req, ec);
  if (ec)
    return ec;

  h2::read(*client_, tmp_buffer_, *resp, ec);
  return ec;
}

Status GCS::RefreshToken(Request* req) {
  auto res = gce_.GetAccessToken(&io_context_, true);
  if (!res.ok())
    return res.status;

  access_token_header_ = absl::StrCat("Bearer ", res.obj);
  req->set(h2::field::authorization, access_token_header_);

  return Status::OK;
}

template <typename RespBody> Status GCS::HttpMessage(Request* req, Response<RespBody>* resp) {
  for (unsigned i = 0; i < 2; ++i) {
    VLOG(1) << "Req: " << *req;

    error_code ec = WriteAndRead(req, resp);
    if (ec) {
      return ToStatus(ec);
    }
    VLOG(1) << "Resp: " << *resp;

    if (!IsUnauthorized(*resp)) {
      break;
    }
    RETURN_IF_ERROR(RefreshToken(req));
    *resp = Response<RespBody>{};
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

bool IsGcsPath(absl::string_view path) { return absl::StartsWith(path, kGsUrl); }

}  // namespace util
