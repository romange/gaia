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

DEFINE_bool(gcs_dry_write, false,
            "If set true do not really perform upload requests."
            "Still creates gcs connections for upload.");

namespace util {

DECLARE_uint32(gcs_upload_buf_log_size);

using namespace std;
using namespace boost;

namespace h2 = beast::http;
namespace rj = rapidjson;

using base::GetMonotonicMicrosFast;

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

inline bool DoesServerPushback(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}


//! returns true if resp is backend error 050x. In that case, sleeps using exponential backoff
// See https://cloud.google.com/storage/docs/exponential-backoff
bool BackoffForServerError(unsigned iteration, const h2::response<h2::dynamic_body>& resp,
                           http::HttpsClient* client) {
  h2::status_class st_class = h2::to_status_class(resp.result());
  if (st_class != h2::status_class::server_error)
    return false;
  VLOG(1) << "Backend error " << iteration;

  // Sometimes response contains "Connection: Close" and sometimes not.
  int32_t handle = client->native_handle();
  if (!resp.keep_alive()) {
    LOG(INFO) << "Closing connection on socket " << handle;
    client->schedule_reconnect();
  }

  unsigned millis = (1 << iteration) * 1000 + (rand() % 1000);

  LOG(INFO) << "Backing off for " << millis << " ms on socket " << handle;

  uint64_t start = GetMonotonicMicrosFast();
  this_fiber::sleep_for(chrono::milliseconds(millis));
  uint64_t duration_ms = (GetMonotonicMicrosFast() - start) / 1000;

  LOG_IF(WARNING, duration_ms < millis - 10) << "Wanted to sleep " << millis
                                             << " but slept " << duration_ms;
  return true;
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

struct SeqReadHandler {
  SeqReadHandler(string url) : read_obj_url(std::move(url)) {}

  string read_obj_url;
  size_t offset = 0, file_size = 0;
  uint32_t errors = 0;

  using OptParser = absl::optional<h2::response_parser<h2::buffer_body>>;
  OptParser parser;
};

//! [from, to) range. If to is kuint64max - then the range is unlimited from above.
inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

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

struct WriteHandler {
  WriteHandler() : body_mb(1 << FLAGS_gcs_upload_buf_log_size) {}

  string url;

  // Returns true if flush is needed. Removes the appended data from src.
  bool Append(strings::ByteRange* src);

  beast::multi_buffer body_mb;
  size_t uploaded = 0;
};

bool WriteHandler::Append(strings::ByteRange* src) {
  size_t prepare_size = std::min(src->size(), body_mb.max_size() - body_mb.size());
  auto mbs = body_mb.prepare(prepare_size);
  size_t offs = 0;
  for (auto mb : mbs) {
    memcpy(mb.data(), src->data() + offs, mb.size());
    offs += mb.size();
  }
  CHECK_EQ(offs, prepare_size);
  src->remove_prefix(prepare_size);
  body_mb.commit(prepare_size);

  DCHECK_LE(body_mb.size(), body_mb.max_size());
  return body_mb.size() == body_mb.max_size();
}

class GCS::ConnState : public absl::variant<absl::monostate, SeqReadHandler, WriteHandler> {
 public:
  ~ConnState() {
    if (auto* pval = absl::get_if<SeqReadHandler>(this)) {
      if (pval->parser) {
        LOG(ERROR) << "File was not closed";
      }
    }
  }
};

GCS::GCS(const GCE& gce, asio::ssl::context* ssl_cntx, IoContext* io_context)
    : gce_(gce), io_context_(*io_context), conn_state_(new ConnState),
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

Status GCS::CloseSequential() {
  CHECK(io_context_.InContextThread());
  return ClearConnState();
}

bool GCS::IsBusy() const {
  bool is_empty = absl::holds_alternative<absl::monostate>(*conn_state_);

  return !is_empty;
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

StatusObject<bool> GCS::SendRequestIterative(Request* req, Parser<h2::buffer_body>* parser) {
  VLOG(1) << "Req: " << *req;

  error_code ec = https_client_->Send(*req);
  RETURN_EC_STATUS(ec);

  ec = https_client_->ReadHeader(parser);

  if (ec) {
    LOG(ERROR) << "Socket error: " << ec << "/" << ec.message();
    VLOG(1) << "FiberSocket status: " << https_client_->client()->next_layer().status();

    return false;  // need to retry
  }

  const auto& msg = parser->get();
  VLOG(1) << "HeaderResp: " << msg;
  if (!parser->keep_alive()) {
    https_client_->schedule_reconnect();
    LOG(INFO) << "Scheduling reconnect due to conn-close header";
  }

  // Partial content can appear because of the previous reconnect.
  if (msg.result() == h2::status::ok || msg.result() == h2::status::partial_content) {
    return true;  // all is good.
  }

  // Parse & drain whatever comes after problematic status.
  // We must do it as long as we plan to use this connection for more requests.
  RETURN_EC_STATUS(https_client_->DrainResponse(parser));

  if (DoesServerPushback(msg.result())) {
    this_fiber::sleep_for(1s);
    return false;  // retry
  }

  if (IsUnauthorized(msg)) {
    auto st = RefreshToken(req);
    if (!st.ok())
      return st;
    return false;
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return Status(StatusCode::INTERNAL_ERROR, "Unexpected http response");
}

auto GCS::OpenSequential(absl::string_view bucket, absl::string_view obj_path) -> OpenSeqResult {
  RETURN_IF_ERROR(PrepareConnection());

  CHECK(absl::holds_alternative<absl::monostate>(*conn_state_));

  string read_obj_url = BuildGetObjUrl(bucket, obj_path);
  auto req = PrepareRequest(h2::verb::get, read_obj_url, access_token_header_);

  SeqReadHandler& handler = conn_state_->emplace<SeqReadHandler>(read_obj_url);

  OpenSeqResult res = OpenSequentialInternal(&req, &handler.parser);
  if (!res.ok()) {
    conn_state_->emplace<absl::monostate>();
    return res.status;
  }

  handler.file_size = res.obj;

  return res.obj;
}

auto GCS::ReadSequential(const strings::MutableByteRange& range) -> ReadObjectResult {
  SeqReadHandler* handler = absl::get_if<SeqReadHandler>(conn_state_.get());
  CHECK(handler && https_client_);

  if (handler->parser->is_done()) {
    return 0;
  }

  for (unsigned iters = 0; iters < 3; ++iters) {
    auto& body = handler->parser->get().body();
    body.data = range.data();
    auto& left_available = body.size;
    left_available = range.size();

    error_code ec = https_client_->Read(&handler->parser.value());
    if (!ec || ec == h2::error::need_buffer || ec == h2::error::partial_message) {
      size_t http_read = range.size() - left_available;
      DVLOG(2) << "Read " << http_read << " bytes from " << handler->offset << " with capacity "
               << range.size();

      // This check does not happen. See here why: https://github.com/boostorg/beast/issues/1662
      // DCHECK_EQ(sz_read, http_read) << " " << range.size() << "/" << left_available;
      handler->offset += http_read;
      return http_read;
    }

    if (ec == asio::ssl::error::stream_truncated) {
      LOG(WARNING) << "Stream " << handler->read_obj_url << " truncated at " << handler->offset
                   << "/" << handler->file_size;

      auto req = PrepareRequest(h2::verb::get, handler->read_obj_url, access_token_header_);
      SetRange(handler->offset, kuint64max, &req);
      OpenSeqResult res = OpenSequentialInternal(&req, &handler->parser);

      if (!res.ok())
        return res.status;
      VLOG(1) << "Reopened the file, new size: " << handler->offset + res.obj;
      // I do not change seq_file_->offset,file_size fields.
      // TODO: to validate that file version has not been changed between retries.
      continue;
    } else {
      LOG(ERROR) << "ec: " << ec << "/" << ec.message() << " at " << handler->offset << "/"
                 << handler->file_size;
      LOG(ERROR) << "FiberSocket status: " << https_client_->client()->next_layer().status();

      return ToStatus(ec);
    }
  }

  return Status(StatusCode::INTERNAL_ERROR, "Maximum iterations reached");
}

StatusObject<file::ReadonlyFile*> GCS::OpenGcsFile(absl::string_view full_path) {
  CHECK(!IsBusy()) << "Can not open " << full_path << " before closing the previous one ";

  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  auto res = OpenSequential(bucket, obj_path);
  if (!res.ok()) {
    return res.status;
  }

  VLOG(1) << "Opened gcs " << full_path << " on socket " << native_handle();

  return new GcsFile{this, res.obj};
}

util::Status GCS::OpenForWrite(absl::string_view bucket, absl::string_view obj_path) {
  RETURN_IF_ERROR(PrepareConnection());

  CHECK(absl::holds_alternative<absl::monostate>(*conn_state_));
  CHECK_GE(FLAGS_gcs_upload_buf_log_size, 18);

  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendEncodedUrl(obj_path, &url);

  auto req = PrepareRequest(h2::verb::post, url, access_token_header_);
  h2::response<h2::dynamic_body> resp_msg;
  req.prepare_payload();

  string upload_id;
  if (FLAGS_gcs_dry_write) {
    upload_id = "dry_write";
  } else {
    RETURN_IF_ERROR(SendWithToken(&req, &resp_msg));
    if (resp_msg.result() != h2::status::ok) {
      return HttpError(resp_msg);
    }
    auto it = resp_msg.find(h2::field::location);
    if (it == resp_msg.end()) {
      return Status(StatusCode::PARSE_ERROR, "Can not find location header");
    }
    upload_id = string(it->value());
  }

  WriteHandler& wh = conn_state_->emplace<WriteHandler>();
  wh.url = std::move(upload_id);

  return Status::OK;
}

util::Status GCS::Write(strings::ByteRange src) {
  CHECK(!src.empty());
  WriteHandler* wh = absl::get_if<WriteHandler>(conn_state_.get());
  CHECK(wh && !wh->url.empty());
  CHECK(io_context_.InContextThread());

  while (wh->Append(&src)) {
    h2::request<h2::dynamic_body> req(h2::verb::put, wh->url, 11);

    size_t body_size = wh->body_mb.size();
    CHECK_GT(body_size, 0);
    CHECK_EQ(0, body_size % (1U << 18)) << body_size;  // Must be multiple of 256KB.

    size_t to = wh->uploaded + body_size;

    req.body() = std::move(wh->body_mb);
    req.set(h2::field::content_range, ContentRangeHeader(wh->uploaded, to, -1));
    req.set(h2::field::content_type, "application/octet-stream");
    req.prepare_payload();

    h2::response<h2::dynamic_body> resp_msg;
    error_code ec;
    unsigned retry = 0;
    uint64_t start = GetMonotonicMicrosFast();
    for (; retry < 5; ++retry) {
      VLOG(1) << "UploadReq" << retry << ": " << req << " socket " << native_handle();
      if (FLAGS_gcs_dry_write) {
        resp_msg.set(h2::field::range, absl::StrCat("bytes=0-", to - 1));
        resp_msg.result(h2::status::permanent_redirect);
      } else {
        ec = https_client_->Send(req, &resp_msg);
        RETURN_EC_STATUS(ec);
      }

      VLOG(1) << "UploadResp" << retry << ": " << resp_msg;

      if (h2::status::permanent_redirect == resp_msg.result())  // 308
        break;

      if (BackoffForServerError(retry, resp_msg, https_client_.get())) {
        detail::gcs_latency->IncBy("retry", GetMonotonicMicrosFast() - start);
        continue;
      }
      LOG(FATAL) << "Unexpected response: " << resp_msg;
    }

    if (h2::status::permanent_redirect != resp_msg.result()) {
      LOG(ERROR) << "Service unavailable " << resp_msg << " after " << retry << " retries";

      return HttpError(resp_msg);
    }

    auto it = resp_msg.find(h2::field::range);
    CHECK(it != resp_msg.end()) << resp_msg;
    absl::string_view range = detail::absl_sv(it->value());
    CHECK(absl::ConsumePrefix(&range, "bytes="));
    size_t pos = range.find('-');
    CHECK_LT(pos, range.size());
    size_t uploaded_pos = 0;
    CHECK(absl::SimpleAtoi(range.substr(pos + 1), &uploaded_pos));
    CHECK_EQ(uploaded_pos + 1, to);

    // wh->body_mb = std::move(req.body());
    // wh->body_mb.consume(body_size);
    wh->uploaded = to;
    CHECK_EQ(0, wh->body_mb.size());
    // CHECK_EQ(wh->body_mb.capacity(), wh->body_mb.max_size());

    detail::gcs_writes->Inc();
    detail::gcs_latency->IncBy("write", GetMonotonicMicrosFast() - start);

    if (src.empty())
      break;
  }

  return Status::OK;
}

util::Status GCS::CloseWrite(bool abort_write) {
  VLOG(1) << "GCS::CloseWrite " << abort_write;

  WriteHandler* wh = absl::get_if<WriteHandler>(conn_state_.get());
  CHECK(wh && !wh->url.empty());

  h2::request<h2::dynamic_body> req(abort_write ? h2::verb::delete_ : h2::verb::put, wh->url, 11);
  if (!abort_write) {
    req.body() = std::move(wh->body_mb);
    size_t to = wh->uploaded + req.body().size();

    req.set(h2::field::content_range, ContentRangeHeader(wh->uploaded, to, to));
  }

  req.prepare_payload();
  using Response = h2::response<h2::dynamic_body>;
  Response resp_msg;

  for (unsigned retry = 0; retry < 3; ++retry) {
    resp_msg = Response{};
    if (FLAGS_gcs_dry_write) {
    } else {
      VLOG(1) << "CloseWriteReq" << retry << ": " << req;
      error_code ec = https_client_->Send(req, &resp_msg);
      RETURN_EC_STATUS(ec);
      VLOG(1) << "CloseWriteResp" << retry << ": " << resp_msg;
    }

    if (resp_msg.result() == h2::status::ok) {
      break;
    }

    if (BackoffForServerError(retry, resp_msg, https_client_.get())) {
      continue;
    }
    LOG(ERROR) << "Error closing GCS file " << resp_msg << " for request: \n" << req;
  }

  conn_state_->emplace<absl::monostate>();

  return resp_msg.result() == h2::status::ok ? Status::OK : HttpError(resp_msg);
}

string GCS::BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendEncodedUrl(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

Status GCS::PrepareConnection() {
  auto status = ClearConnState();

  if (!status.ok()) {
    LOG(ERROR) << "Error in ClearConnState " << status;
  }

  return Status::OK;
}

util::Status GCS::ClearConnState() {
  if (auto seq_ptr = absl::get_if<SeqReadHandler>(conn_state_.get())) {
    VLOG(1) << "Close::SeqReadHandler";

    CHECK(https_client_);
    error_code ec;

    if (seq_ptr->parser) {
      ec = https_client_->DrainResponse(&seq_ptr->parser.value());
      if (ec) {
        https_client_->schedule_reconnect();
      }
    }

    conn_state_->emplace<absl::monostate>();
    return Status::OK;
  }

  return Status::OK;
}

auto GCS::OpenSequentialInternal(Request* req, ReusableParser* parser) -> OpenSeqResult {
  Status err_st;
  for (unsigned iters = 0; iters < 3; ++iters) {
    VLOG(1) << "OpenSequentialInternal" << iters << ": socket " << native_handle();

    parser->emplace();
    parser->value().body_limit(kuint64max);

    auto status_obj = SendRequestIterative(req, &parser->value());
    if (!status_obj.ok()) {
      err_st = status_obj.status;
      continue;
    }
    bool success = status_obj.obj;

    if (success) {
      const auto& msg = (*parser)->get();
      auto content_len_it = msg.find(h2::field::content_length);
      size_t content_sz = 0;
      if (content_len_it != msg.end()) {
        CHECK(absl::SimpleAtoi(detail::absl_sv(content_len_it->value()), &content_sz));
      }
      return content_sz;
    }
  }

  return err_st;
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
