// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/asio/ssl/error.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/strip.h"

#include "base/logging.h"
#include "strings/escaping.h"

#include "util/aws/aws.h"
#include "util/aws/s3.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

using file::ReadonlyFile;
using http::HttpsClientPool;

using namespace boost;
namespace h2 = beast::http;
using std::string;

using bb_str_view = ::boost::beast::string_view;

namespace {

constexpr char kS3Url[] = "s3://";

// TODO: the same like in gcs_utils.h
inline Status ToStatus(const ::boost::system::error_code& ec) {
  return ec ? Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()))
            : Status::OK;
}

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << std::endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << std::endl;
  }
  os << "-------------------------";

  return os;
}

// TODO: the same as in GCS. Can be implemented in terms of static 64 bytes buffer.
inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

inline const char* as_char(const xmlChar* var) {
  return reinterpret_cast<const char*>(var);
}

class S3ReadFile : public ReadonlyFile {
 public:
  using error_code = ::boost::system::error_code;
  using Parser = h2::response_parser<h2::buffer_body>;

  // does not own pool object, only wraps it with ReadonlyFile interface.
  S3ReadFile(const AWS& aws, HttpsClientPool* pool, string read_obj_url)
      : aws_(aws), pool_(pool), read_obj_url_(std::move(read_obj_url)) {
  }

  virtual ~S3ReadFile() final;

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) final;

  // releases the system handle for this file.
  Status Close() final;

  size_t Size() const final {
    return size_;
  }

  int Handle() const final {
    return -1;
  }

  Status Open();

 private:
  Parser* parser() {
    return &parser_;
  }

  const AWS& aws_;
  HttpsClientPool* pool_;

  const string read_obj_url_;
  HttpsClientPool::ClientHandle https_handle_;

  Parser parser_;
  size_t size_ = 0, offs_ = 0;
};

S3ReadFile::~S3ReadFile() {
}

Status S3ReadFile::Open() {
  string url = absl::StrCat("/", read_obj_url_);
  h2::request<h2::empty_body> req{h2::verb::get, url, 11};

  if (offs_)
    SetRange(offs_, kuint64max, &req);

  VLOG(1) << "Unsigned request: " << req;

  aws_.Sign(pool_->domain(), &req);

  // TODO: to wrap IO operations with retryable mechanism like in GCS.
  HttpsClientPool::ClientHandle handle = pool_->GetHandle();

  system::error_code ec = handle->Send(req);

  if (ec) {
    return ToStatus(ec);
  }

  parser_.body_limit(kuint64max);
  ec = handle->ReadHeader(&parser_);
  if (ec) {
    return ToStatus(ec);
  }

  CHECK(parser_.keep_alive()) << "TBD";
  const auto& msg = parser_.get();

  if (msg.result() != h2::status::ok) {
    LOG(INFO) << "OpenError: " << msg;

    return Status(StatusCode::IO_ERROR, string(msg.reason()));
  }
  VLOG(1) << "HeaderResp(" << handle->native_handle() << "): " << msg;

  auto content_len_it = msg.find(h2::field::content_length);
  if (content_len_it != msg.end()) {
    size_t content_sz = 0;
    CHECK(absl::SimpleAtoi(absl_sv(content_len_it->value()), &content_sz));

    if (size_) {
      CHECK_EQ(size_, content_sz + offs_) << "File size has changed underneath during reopen";
    } else {
      size_ = content_sz;
    }
  }
  https_handle_ = std::move(handle);
  return Status::OK;
}

StatusObject<size_t> S3ReadFile::Read(size_t offset, const strings::MutableByteRange& range) {
  CHECK(!range.empty());

  if (offset != offs_) {
    return Status(StatusCode::INVALID_ARGUMENT, "Only sequential access supported");
  }

  // We can not cache parser() into local var because Open() below recreates the parser instance.
  if (parser_.is_done()) {
    return 0;
  }

  size_t read_sofar = 0;
  while (read_sofar < range.size()) {
    // We keep body references inside the loop because Open() that might be called here,
    // will recreate the parser from the point the connections disconnected.
    auto& body = parser()->get().body();
    auto& left_available = body.size;
    body.data = range.data() + read_sofar;
    left_available = range.size() - read_sofar;

    error_code ec = https_handle_->Read(parser());  // decreases left_available.
    size_t http_read = (range.size() - read_sofar) - left_available;

    if (!ec || ec == h2::error::need_buffer) {  // Success
      DVLOG(2) << "Read " << http_read << " bytes from " << offset << " with capacity "
               << range.size() << "ec: " << ec;

      // This check does not happen. See here why: https://github.com/boostorg/beast/issues/1662
      // DCHECK_EQ(sz_read, http_read) << " " << range.size() << "/" << left_available;
      offs_ += http_read;

      CHECK(left_available == 0 || !ec);
      return http_read + read_sofar;
    }

    if (ec == h2::error::partial_message) {
      offs_ += http_read;
      VLOG(1) << "Got partial_message, socket status: "
              << https_handle_->client()->next_layer().status() << ", socket "
              << https_handle_->native_handle();

      // advance the destination buffer as well.
      read_sofar += http_read;
      ec = asio::ssl::error::stream_truncated;
    }

    if (ec == asio::ssl::error::stream_truncated) {
      VLOG(1) << "Stream " << read_obj_url_ << " truncated at " << offs_ << "/" << size_;
      https_handle_.reset();

      RETURN_IF_ERROR(Open());
      VLOG(1) << "Reopened the file, new size: " << size_;
      // TODO: to validate that file version has not been changed between retries.
      continue;
    } else {
      LOG(ERROR) << "ec: " << ec << "/" << ec.message() << " at " << offset << "/" << size_;
      LOG(ERROR) << "FiberSocket status: " << https_handle_->client()->next_layer().status();

      return ToStatus(ec);
    }
  }

  return read_sofar;
}

// releases the system handle for this file.
Status S3ReadFile::Close() {
  if (https_handle_ && parser()) {
    if (!parser()->is_done()) {
      // We prefer closing the connection to draining.
      https_handle_->schedule_reconnect();
    }
  }
  https_handle_.reset();

  return Status::OK;
}

}  // namespace


const char* S3Bucket::kRootDomain = "s3.amazonaws.com";

S3Bucket::S3Bucket(const AWS& aws, http::HttpsClientPool* pool) : aws_(aws), pool_(pool) {
}

auto S3Bucket::List(absl::string_view glob, bool fs_mode, ListObjectCb cb) -> ListObjectResult {
  HttpsClientPool::ClientHandle handle = pool_->GetHandle();

  string url{"/?"};

  if (!glob.empty()) {
    url.append("&prefix=");
    strings::AppendEncodedUrl(glob, &url);
  }

  if (fs_mode) {
    url.append("&delimeter=");
    strings::AppendEncodedUrl("/", &url);
  }

  h2::request<h2::empty_body> req{h2::verb::get, url, 11};
  h2::response<h2::string_body> resp;

  aws_.Sign(pool_->domain(), &req);
  VLOG(1) << "Req: " << req;

  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    return ToStatus(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(INFO) << "ListError: " << resp;

    return Status(StatusCode::IO_ERROR, string(resp.reason()));
  }

  VLOG(1) << "ListResp: " << resp;
  return Status::OK;
}

bool S3Bucket::SplitToBucketPath(absl::string_view input, absl::string_view* bucket,
                                 absl::string_view* path) {
  if (!absl::ConsumePrefix(&input, kS3Url))
    return false;

  auto pos = input.find('/');
  *bucket = input.substr(0, pos);
  *path = (pos == absl::string_view::npos) ? absl::string_view{} : input.substr(pos + 1);
  return true;
}

string S3Bucket::ToFullPath(absl::string_view bucket, absl::string_view key_path) {
  return absl::StrCat(kS3Url, bucket, "/", key_path);
}

ListS3BucketResult ListS3Buckets(const AWS& aws, http::HttpsClientPool* pool) {
  HttpsClientPool::ClientHandle handle = pool->GetHandle();

  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  h2::response<h2::string_body> resp;

  aws.Sign(S3Bucket::kRootDomain, &req);

  VLOG(1) << "Req: " << req;

  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    return ToStatus(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(INFO) << "Error: " << resp;

    return Status(StatusCode::IO_ERROR, string(resp.reason()));
  }

  VLOG(1) << "ListS3Buckets: " << resp;

  std::vector<std::string> res;

  xmlDocPtr doc = xmlReadMemory(resp.body().data(), resp.body().size(), NULL, NULL, 0);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto register_res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                         BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(register_res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(
      BAD_CAST "/NS:ListAllMyBucketsResult/NS:Buckets/NS:Bucket/NS:Name", xpathCtx);
  CHECK(xpathObj);
  xmlNodeSetPtr nodes = xpathObj->nodesetval;
  if (nodes) {
    int size = nodes->nodeNr;
    for (int i = 0; i < size; ++i) {
      xmlNodePtr cur = nodes->nodeTab[i];
      CHECK_EQ(XML_ELEMENT_NODE, cur->type);
      CHECK(cur->ns);
      CHECK(nullptr == cur->content);

      if (cur->children && cur->last == cur->children && cur->children->type == XML_TEXT_NODE) {
        CHECK(cur->children->content);
        res.push_back(as_char(cur->children->content));
      }
    }
  }

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);

  return res;
}

StatusObject<file::ReadonlyFile*> OpenS3ReadFile(absl::string_view key_path, const AWS& aws,
                                                 http::HttpsClientPool* pool,
                                                 const file::ReadonlyFile::Options& opts) {
  CHECK(opts.sequential && pool);

  absl::string_view bucket, obj_path;

  string read_obj_url{key_path};
  std::unique_ptr<S3ReadFile> fl(new S3ReadFile(aws, pool, std::move(read_obj_url)));
  RETURN_IF_ERROR(fl->Open());

  return fl.release();
}


bool IsS3Path(absl::string_view path) {
  return absl::StartsWith(path, kS3Url);
}

}  // namespace util
