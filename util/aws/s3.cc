// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/aws/s3.h"

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/asio/ssl/error.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/strip.h"
#include "absl/types/optional.h"
#include "base/logging.h"
#include "strings/escaping.h"
#include "util/asio/io_context.h"
#include "util/aws/aws.h"
#include "util/http/http_common.h"
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

class S3WriteFile : public file::WriteFile {
 public:
  /**
   * @brief Construct a new S3 Write File object.
   *
   * @param name - aka "gs://somebucket/path_to_obj"
   * @param aws - initialized AWS object.
   * @param pool - https connection pool connected to google api server.
   */
  S3WriteFile(absl::string_view name, const AWS& aws, string upload_id, HttpsClientPool* pool);

  bool Close() final;

  bool Open() final;

  Status Write(const uint8* buffer, uint64 length) final;

 private:
  size_t FillBuf(const uint8* buffer, size_t length);

  Status Upload();

  const AWS& aws_;
  uint32_t part_num_ = 1;

  string upload_id_;
  beast::multi_buffer body_mb_;
  size_t uploaded_ = 0;
  HttpsClientPool* pool_;
  std::vector<string> parts_;
};

S3ReadFile::~S3ReadFile() {
}

Status S3ReadFile::Open() {
  string url = absl::StrCat("/", read_obj_url_);
  h2::request<h2::empty_body> req{h2::verb::get, url, 11};

  if (offs_)
    SetRange(offs_, kuint64max, &req);

  VLOG(1) << "Unsigned request: " << req;

  aws_.SignEmpty(pool_->domain(), &req);

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

S3WriteFile::S3WriteFile(absl::string_view name, const AWS& aws, string upload_id,
                         HttpsClientPool* pool)
    : file::WriteFile(name), aws_(aws), upload_id_(std::move(upload_id)),
    body_mb_(5 * (1<< 20)), pool_(pool) {
}

bool S3WriteFile::Close() {
  CHECK(pool_->io_context().InContextThread());

  auto status = Upload();
  if (!status.ok()) {
    LOG(ERROR) << "Error uploading " << status;
    return false;
  }

  string url("/");

  strings::AppendEncodedUrl(create_file_name_, &url);

  // Signed params must look like key/value pairs. Instead of handling key-only params
  // in the signing code we just pass empty value here.
  absl::StrAppend(&url, "?uploadId=", upload_id_);

  h2::request<h2::string_body> req{h2::verb::post, url, 11};
  h2::response<h2::string_body> resp;

  req.set(h2::field::content_type, http::kXmlMime);
  auto& body = req.body();
  body = R"(<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">)";

  for (size_t i = 0; i < parts_.size(); ++i) {
    absl::StrAppend(&body, "<Part><ETag>\"", parts_[i], "\"</ETag><PartNumber>", i + 1);
    absl::StrAppend(&body, "</PartNumber></Part>\n");
  }
  body.append("</CompleteMultipartUpload>");

  req.prepare_payload();
  char sha256[65];

  detail::Sha256String(req.body(), sha256);
  aws_.Sign(pool_->domain(), absl::string_view{sha256, 64}, &req);

  HttpsClientPool::ClientHandle handle = pool_->GetHandle();
  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    VLOG(1) << "Error sending to socket " << handle->native_handle() << " " << ec;
    return false;
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "S3WriteFile::Close: " << req << "/ " << resp;

    return false;
  }
  return true;
}

bool S3WriteFile::Open() {
  LOG(FATAL) << "Should not be called";

  return true;
}

Status S3WriteFile::Write(const uint8* buffer, uint64 length) {
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

size_t S3WriteFile::FillBuf(const uint8* buffer, size_t length) {
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

Status S3WriteFile::Upload() {
  size_t body_size = body_mb_.size();
  if (body_size == 0)
    return Status::OK;

  string url("/");
  char sha256[65];

  detail::Sha256String(body_mb_, sha256);
  strings::AppendEncodedUrl(create_file_name_, &url);
  absl::StrAppend(&url, "?uploadId=", upload_id_);
  absl::StrAppend(&url, "&partNumber=", part_num_);

  h2::request<h2::dynamic_body> req{h2::verb::put, url, 11};
  req.set(h2::field::content_type, http::kBinMime);

  req.body() = std::move(body_mb_);
  req.prepare_payload();

  aws_.Sign(pool_->domain(), absl::string_view{sha256, 64}, &req);

  HttpsClientPool::ClientHandle handle = pool_->GetHandle();
  h2::response<h2::string_body> resp;
  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    VLOG(1) << "Error sending to socket " << handle->native_handle() << " " << ec;
    return ToStatus(ec);
  }
  VLOG(1) << "Upload: " << resp;
  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "S3WriteFile::Upload: " << resp;

    return Status(StatusCode::IO_ERROR, string(resp.reason()));;
  }
  auto it = resp.find(h2::field::etag);
  CHECK(it != resp.end());

  parts_.emplace_back(it->value());
  ++part_num_;

  if (!resp.keep_alive()) {
    handle->schedule_reconnect();
  }

  return Status::OK;
}

// ******************** Helper utilities

inline xmlDocPtr XmlRead(absl::string_view xml) {
  return xmlReadMemory(xml.data(), xml.size(), NULL, NULL, XML_PARSE_COMPACT | XML_PARSE_NOBLANKS);
}

std::pair<size_t, absl::string_view> ParseXmlObjContents(xmlNodePtr node) {
  std::pair<size_t, absl::string_view> res;

  for (xmlNodePtr child = node->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;

      if (!strcmp(as_char(child->name), "Key")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        res.second = absl::string_view(as_char(grand->content));
      } else if (!strcmp(as_char(child->name), "Size")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        CHECK(absl::SimpleAtoi(as_char(grand->content), &res.first));
      }
    }
  }
  return res;
}

void ParseXmlStartUpload(absl::string_view xml_resp, string* upload_id) {
  xmlDocPtr doc = XmlRead(xml_resp);
  CHECK(doc);

  xmlNodePtr root = xmlDocGetRootElement(doc);
  CHECK_STREQ("InitiateMultipartUploadResult", as_char(root->name));

  for (xmlNodePtr child = root->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;
      if (!strcmp(as_char(child->name), "UploadId")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        upload_id->assign(as_char(grand->content));
      }
    }
  }
  xmlFreeDoc(doc);
}

}  // namespace

namespace detail {

std::vector<std::string> ParseXmlListBuckets(absl::string_view xml_resp) {
  xmlDocPtr doc = XmlRead(xml_resp);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto register_res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                         BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(register_res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(
      BAD_CAST "/NS:ListAllMyBucketsResult/NS:Buckets/NS:Bucket/NS:Name", xpathCtx);
  CHECK(xpathObj);
  xmlNodeSetPtr nodes = xpathObj->nodesetval;
  std::vector<std::string> res;
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

void ParseXmlListObj(absl::string_view xml_obj, S3Bucket::ListObjectCb cb) {
  xmlDocPtr doc = XmlRead(xml_obj);
  CHECK(doc);

  xmlNodePtr root = xmlDocGetRootElement(doc);
  CHECK_STREQ("ListBucketResult", as_char(root->name));

  for (xmlNodePtr child = root->children; child; child = child->next) {
    if (child->type == XML_ELEMENT_NODE) {
      xmlNodePtr grand = child->children;
      if (!strcmp(as_char(child->name), "IsTruncated")) {
        CHECK(grand && grand->type == XML_TEXT_NODE);
        CHECK_STREQ("false", as_char(grand->content)) << "TBD";
      } else if (!strcmp(as_char(child->name), "Marker")) {
      } else if (!strcmp(as_char(child->name), "Contents")) {
        auto sz_name = ParseXmlObjContents(child);
        cb(sz_name.first, sz_name.second);
      }
    }
  }
  xmlFreeDoc(doc);
}

}  // namespace detail

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
    url.append("&delimiter=");
    strings::AppendEncodedUrl("/", &url);
  }

  h2::request<h2::empty_body> req{h2::verb::get, url, 11};
  h2::response<h2::string_body> resp;

  aws_.SignEmpty(pool_->domain(), &req);
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
  detail::ParseXmlListObj(resp.body(), std::move(cb));

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

  aws.SignEmpty(S3Bucket::kRootDomain, &req);

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

  return detail::ParseXmlListBuckets(resp.body());
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

StatusObject<file::WriteFile*> OpenS3WriteFile(absl::string_view key_path, const AWS& aws,
                                               http::HttpsClientPool* pool) {
  string url("/");

  strings::AppendEncodedUrl(key_path, &url);

  // Signed params must look like key/value pairs. Instead of handling key-only params
  // in the signing code we just pass empty value here.
  absl::StrAppend(&url, "?uploads=");

  h2::request<h2::empty_body> req{h2::verb::post, url, 11};
  h2::response<h2::string_body> resp;

  aws.SignEmpty(pool->domain(), &req);

  HttpsClientPool::ClientHandle handle = pool->GetHandle();
  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    return ToStatus(ec);
  }

  if (resp.result() != h2::status::ok) {
    LOG(ERROR) << "OpenWriteFile Error: " << resp;

    return Status(StatusCode::IO_ERROR, string(resp.reason()));
  }
  string upload_id;
  ParseXmlStartUpload(resp.body(), &upload_id);

  VLOG(1) << "OpenS3WriteFile: " << req << "/" << resp << "UploadId: " << upload_id;

  return new S3WriteFile(key_path, aws, std::move(upload_id), pool);
}

bool IsS3Path(absl::string_view path) {
  return absl::StartsWith(path, kS3Url);
}

}  // namespace util
