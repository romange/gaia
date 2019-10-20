// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/gce/gcs.h"

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/parser.hpp>

#include "base/logging.h"
#include "strings/escaping.h"

#include "util/gce/detail/gcs_utils.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

using namespace boost;
using namespace http;
using namespace ::std;
namespace h2 = beast::http;
using file::ReadonlyFile;

namespace {

string BuildGetObjUrl(absl::string_view bucket, absl::string_view obj_path) {
  string read_obj_url{"/storage/v1/b/"};
  absl::StrAppend(&read_obj_url, bucket, "/o/");
  strings::AppendEncodedUrl(obj_path, &read_obj_url);
  absl::StrAppend(&read_obj_url, "?alt=media");

  return read_obj_url;
}

inline void SetRange(size_t from, size_t to, h2::fields* flds) {
  string tmp = absl::StrCat("bytes=", from, "-");
  if (to < kuint64max) {
    absl::StrAppend(&tmp, to - 1);
  }
  flds->set(h2::field::range, std::move(tmp));
}

class GcsReadFile : public ReadonlyFile, private detail::ApiSenderBufferBody {
 public:
  using error_code = ::boost::system::error_code;

  // does not own gcs object, only wraps it with ReadonlyFile interface.
  GcsReadFile(const GCE& gce, HttpsClientPool* pool, string read_obj_url)
      : detail::ApiSenderBufferBody("read", gce, pool), read_obj_url_(std::move(read_obj_url)) {}

  virtual ~GcsReadFile() final;

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) final;

  // releases the system handle for this file.
  Status Close() final;

  size_t Size() const final { return size_; }

  int Handle() const final { return -1; }

  Status Open();

 private:
  const string read_obj_url_;
  HttpsClientPool::ClientHandle https_handle_;

  size_t size_;
  size_t offs_ = 0;
};

GcsReadFile::~GcsReadFile() {}

Status GcsReadFile::Open() {
  string token = gce_.access_token();

  auto req = detail::PrepareGenericRequest(h2::verb::get, read_obj_url_, token);
  if (offs_)
    SetRange(offs_, kuint64max, &req);
  auto handle_res = SendGeneric(3, req);
  if (!handle_res.ok())
    return handle_res.status;

  const auto& msg = parser()->get();
  auto content_len_it = msg.find(h2::field::content_length);
  if (content_len_it != msg.end()) {
    CHECK(absl::SimpleAtoi(detail::absl_sv(content_len_it->value()), &size_));
  }
  https_handle_ = std::move(handle_res.obj);
  return Status::OK;
}

StatusObject<size_t> GcsReadFile::Read(size_t offset, const strings::MutableByteRange& range) {
  if (offset != offs_) {
    return Status(StatusCode::INVALID_ARGUMENT, "Only sequential access supported");
  }

  if (parser()->is_done()) {
    return 0;
  }

  for (unsigned iters = 0; iters < 3; ++iters) {
    auto& body = parser()->get().body();
    body.data = range.data();
    auto& left_available = body.size;
    left_available = range.size();

    error_code ec = https_handle_->Read(parser());
    if (!ec || ec == h2::error::need_buffer || ec == h2::error::partial_message) {  // Success
      size_t http_read = range.size() - left_available;
      DVLOG(2) << "Read " << http_read << " bytes from " << offset << " with capacity "
               << range.size();

      // This check does not happen. See here why: https://github.com/boostorg/beast/issues/1662
      // DCHECK_EQ(sz_read, http_read) << " " << range.size() << "/" << left_available;
      offs_ += http_read;
      return http_read;
    }

    if (ec == asio::ssl::error::stream_truncated) {
      LOG(WARNING) << "Stream " << read_obj_url_ << " truncated at " << offset << "/" << size_;
      https_handle_.reset();

      RETURN_IF_ERROR(Open());
      VLOG(1) << "Reopened the file, new size: " << size_;
      // I do not change seq_file_->offset,file_size fields.
      // TODO: to validate that file version has not been changed between retries.
      continue;
    } else {
      LOG(ERROR) << "ec: " << ec << "/" << ec.message() << " at " << offset << "/" << size_;
      LOG(ERROR) << "FiberSocket status: " << https_handle_->client()->next_layer().status();

      return detail::ToStatus(ec);
    }
  }

  return Status(StatusCode::INTERNAL_ERROR, "Maximum iterations reached");
}

// releases the system handle for this file.
Status GcsReadFile::Close() {
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

StatusObject<ReadonlyFile*> OpenGcsReadFile(absl::string_view full_path, const GCE& gce,
                                            HttpsClientPool* pool,
                                            const ReadonlyFile::Options& opts) {
  CHECK(opts.sequential && pool);
  CHECK(IsGcsPath(full_path));

  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  string read_obj_url = BuildGetObjUrl(bucket, obj_path);

  std::unique_ptr<GcsReadFile> fl(new GcsReadFile(gce, pool, std::move(read_obj_url)));
  RETURN_IF_ERROR(fl->Open());

  return fl.release();
}

}  // namespace util
