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
#include "absl/types/optional.h"
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

namespace {

class GcsFile : public file::ReadonlyFile {
 public:
  // does not own gcs object, only wraps it with ReadonlyFile interface.
  GcsFile(GCS* gcs, size_t sz) : gcs_(gcs), size_(sz) {}
  ~GcsFile();

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) final;

  // releases the system handle for this file.
  Status Close() final;

  size_t Size() const final { return size_; }

  int Handle() const final { return -1; }

 private:
  GCS* gcs_;
  size_t size_;
  size_t offs_ = 0;
};

inline util::Status ToStatus(const ::boost::system::error_code& ec) {
  return Status(::util::StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()));
}

#define RETURN_EC_STATUS(x)                                 \
  do {                                                      \
    auto __ec$ = (x);                                       \
    if (__ec$) {                                            \
      VLOG(1) << "EC: " << __ec$ << " " << __ec$.message(); \
      return ToStatus(x);                                   \
    }                                                       \
  } while (false)

inline absl::string_view absl_sv(beast::string_view s) {
  return absl::string_view{s.data(), s.size()};
}

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

StatusObject<size_t> GcsFile::Read(size_t offset, const strings::MutableByteRange& range) {
  if (offset != offs_) {
    return Status(StatusCode::INVALID_ARGUMENT, "Only sequential access supported");
  }
  auto res = gcs_->ReadSequential(range);
  if (!res.ok()) {
    return res.status;
  }

  offs_ += res.obj;
  return res.obj;
}

Status GcsFile::Close() {
  Status st;
  if (gcs_) {
    st = gcs_->CloseSequential();
    gcs_ = nullptr;
  }
  return st;
}

GcsFile::~GcsFile() {
  if (gcs_) {
    LOG(WARNING) << "Close was not called";
    Close();
  }
}

inline bool ShouldRetry(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}

}  // namespace

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  return os;
}

struct GCS::SeqReadFile {
  SeqReadFile(string url) : read_obj_url(std::move(url)) {
  }

  void Init() {
    parser.emplace();
    parser->body_limit(kuint64max);
  }

  string read_obj_url;
  size_t offset = 0, file_size = 0;
  uint32_t errors = 0;
  absl::optional<h2::response_parser<h2::buffer_body>> parser;
};


GCS::GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}

GCS::~GCS() {
  if (seq_file_ && seq_file_->parser) {
    LOG(ERROR) << "File was not closed";
  }
  VLOG(1) << "GCS::~GCS";
  client_.reset();
}

util::Status GCS::Connect(unsigned msec) {
  CHECK(io_context_.InContextThread());
  if (!reconnect_needed_) {
    return Status::OK;
  }

  reconnect_msec_ = msec;
  RETURN_IF_ERROR(InitSslClient());

  auto res = gce_.GetAccessToken(&io_context_);
  if (!res.ok())
    return res.status;
  access_token_header_ = absl::StrCat("Bearer ", res.obj);
  return Status::OK;
}

util::Status GCS::CloseSequential() {
  CHECK(io_context_.InContextThread());

  if (!seq_file_) {
    return Status::OK;
  }
  CHECK(client_);

  if (seq_file_->parser) {
    error_code ec = DrainResponse(&seq_file_->parser.value());
    seq_file_->parser.reset();
    RETURN_EC_STATUS(ec);
  }

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

auto GCS::List(absl::string_view bucket, absl::string_view prefix, bool fs_mode, ListObjectCb cb)
    -> ListObjectResult {
  CHECK(!bucket.empty());
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

auto GCS::DrainResponse(Parser<h2::buffer_body>* parser) -> error_code {
  uint8_t buf[512];
  auto& body = parser->get().body();
  error_code ec;
  size_t sz = 0;

  // Drain pending response completely to allow reusing the current connection.
  while (!parser->is_done()) {
    body.data = buf;
    body.size = sizeof(buf);
    size_t s1 = h2::read(*client_, tmp_buffer_, *parser, ec);
    if (ec && ec != h2::error::need_buffer) {
      return ec;
    }
    sz += s1;
    VLOG(1) << "DrainResp: " << parser->get();
  }
  VLOG_IF(1, sz > 0) << "Drained " << sz << " bytes";

  return error_code{};
}

StatusObject<bool> GCS::SendRequestIterative(Request* req, Parser<h2::buffer_body>* parser) {
  VLOG(1) << "Req: " << req;

  error_code ec;
  h2::write(*client_, *req, ec);
  RETURN_EC_STATUS(ec);

  h2::read_header(*client_, tmp_buffer_, *parser, ec);

  if (ec) {
    LOG(ERROR) << "Socket error: " << ec << "/" << ec.message();
    VLOG(1) << "FiberSocket status: " << client_->next_layer().status();

    Status st = InitSslClient();
    if (!st.ok()) {
      LOG(ERROR) << "Could not reconnect " << st;
      return st;
    }
    return false;  // need to retry
  }

  const auto& msg = parser->get();
  VLOG(1) << "HeaderResp: " << msg;
  if (!parser->keep_alive()) {
    LOG(WARNING) << "No keep-alive, requested reconnect";
    reconnect_needed_ = true;
  }

  if (msg.result() == h2::status::ok) {
    return true;  // all is good.
  }

  if (ShouldRetry(msg.result())) {
    RETURN_EC_STATUS(DrainResponse(parser));
    this_fiber::sleep_for(1s);
    return false;  // retry
  }

  if (IsUnauthorized(msg)) {
    RETURN_EC_STATUS(DrainResponse(parser));

    auto st = RefreshToken(req);
    if (!st.ok())
      return st;
    return false;
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return Status(StatusCode::INTERNAL_ERROR, "Unexpected http response");
}

auto GCS::OpenSequential(absl::string_view bucket, absl::string_view obj_path) -> OpenSeqResult {
  string read_obj_url = BuildGetObjUrl(bucket, obj_path);
  auto req = PrepareRequest(h2::verb::get, read_obj_url, access_token_header_);

  std::unique_ptr<SeqReadFile> open_file(new SeqReadFile(read_obj_url));

  OpenSeqResult res = OpenSequentialInternal(&req, open_file.get());
  if (!res.ok())
    return res.status;

  seq_file_ = std::move(open_file);
  seq_file_->file_size = res.obj;

  return res.obj;
}

auto GCS::ReadSequential(const strings::MutableByteRange& range) -> ReadObjectResult {
  CHECK(seq_file_ && client_);

  if (seq_file_->parser->is_done()) {
    return 0;
  }

  for (unsigned iters = 0; iters < 3; ++iters) {
    auto& body = seq_file_->parser->get().body();
    body.data = range.data();
    auto& left_available = body.size;
    left_available = range.size();

    error_code ec;

    // h2::read returns number of raw bytes read from stream before parsing.
    // Also it does not consider temporary buffer caching. Therefore we use left_available to count.
    h2::read(*client_, tmp_buffer_, *seq_file_->parser, ec);
    if (!ec || ec == h2::error::need_buffer) {
      size_t http_read = range.size() - left_available;
      DVLOG(2) << "Read " << http_read << " bytes from " << seq_file_->offset << " with capacity "
               << range.size();

      // This check does not happen for some reason: https://github.com/boostorg/beast/issues/1662
      // DCHECK_EQ(sz_read, http_read) << " " << range.size() << "/" << left_available;
      seq_file_->offset += http_read;
      return http_read;
    }

    if (ec = asio::ssl::error::stream_truncated) {
      LOG(WARNING) << "Stream " << seq_file_->read_obj_url << " truncated at "
                   << seq_file_->offset << "/" << seq_file_->file_size;
      reconnect_needed_ = true;
      auto req = PrepareRequest(h2::verb::get, seq_file_->read_obj_url, access_token_header_);
      req.set(h2::field::range, absl::StrCat("bytes=", seq_file_->offset, "-"));
      OpenSeqResult res = OpenSequentialInternal(&req, seq_file_.get());

      if (!res.ok())
        return res.status;
      VLOG(1) << "Reopened the file, new size: " << seq_file_->offset + res.obj;
      // I do not change seq_file_->offset,file_size fields.
      // TODO: to validate that file version has not been changed between retries.
      continue;
    } else {
      LOG(ERROR) << "ec: " << ec << "/" << ec.message() << " at " << seq_file_->offset << "/"
                 << seq_file_->file_size;
      LOG(ERROR) << "FiberSocket status: " << client_->next_layer().status();

      return ToStatus(ec);
    }
  }

  return Status(StatusCode::INTERNAL_ERROR, "Maximum iterations reached");
}

util::StatusObject<file::ReadonlyFile*> GCS::OpenGcsFile(absl::string_view full_path) {
  CHECK(!seq_file_) << "Can not open " << full_path << " before closing the previous one ";

  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  auto res = OpenSequential(bucket, obj_path);
  if (!res.ok()) {
    return res.status;
  }

  return new GcsFile{this, res.obj};
}

string GCS::BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendEncodedUrl(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

util::Status GCS::InitSslClient() {
  client_.reset(new SslStream(FiberSyncSocket{kDomain, "443", &io_context_}, gce_.ssl_context()));

  auto status = SslConnect(client_.get(), reconnect_msec_);
  if (status.ok()) {
    reconnect_needed_ = false;
  }
  return status;
}

util::Status GCS::PrepareConnection() {
  RETURN_IF_ERROR(CloseSequential());

  if (reconnect_needed_) {
    RETURN_IF_ERROR(InitSslClient());
  }
  return Status::OK;
}

auto GCS::OpenSequentialInternal(Request* req, SeqReadFile* seq_file) -> OpenSeqResult {
  for (unsigned iters = 0; iters < 5; ++iters) {
    RETURN_IF_ERROR(PrepareConnection());  //! Drains and connects if needed.

    seq_file->Init();
    auto status_obj = SendRequestIterative(req, &seq_file->parser.value());
    if (!status_obj.ok())
      return status_obj.status;
    bool success = status_obj.obj;

    if (success) {
      const auto& msg = seq_file->parser->get();
      auto content_len_it = msg.find(h2::field::content_length);
      size_t content_sz = 0;
      if (content_len_it != msg.end()) {
        CHECK(absl::SimpleAtoi(absl_sv(content_len_it->value()), &content_sz));
      }
      return content_sz;
    }
  }

  return Status(StatusCode::INTERNAL_ERROR, "Maximum iterations reached");
}

template <typename RespBody>
auto GCS::WriteAndRead(const Request& req, Response<RespBody>* resp) -> error_code {
  error_code ec;
  h2::write(*client_, req, ec);
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

    error_code ec = WriteAndRead(*req, resp);
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
