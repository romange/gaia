// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/future.hpp>

#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"

#include "util/gce/gce.h"
#include "util/gce/gcs.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(bucket, "", "");
DEFINE_string(read_path, "", "");
DEFINE_string(prefix, "", "");
DEFINE_string(download, "", "");
DEFINE_string(access_token, "", "");
DEFINE_string(upload, "", "");

using FileQ = fibers::buffered_channel<string>;

string ca_cert;

void DownloadFile(StringPiece bucket, StringPiece obj_path, const GCE& gce,
                  http::HttpsClientPool* pool) {
  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  string full_path = GCS::ToGcsPath(bucket, obj_path);

  StatusObject<file::ReadonlyFile*> st_file = OpenGcsReadFile(full_path, gce, pool);
  CHECK_STATUS(st_file.status);
  std::unique_ptr<file::ReadonlyFile> file{st_file.obj};

  LOG(INFO) << "Opened file " << full_path << " with size " << file->Size();

  size_t ofs = 0;
  strings::MutableByteRange mbr(buf.get(), kBufSize);
  while (true) {
    auto res = file->Read(ofs, mbr);
    CHECK_STATUS(res.status);
    ofs += res.obj;
    if (res.obj < mbr.size()) {
      break;
    }
  }

  CHECK_EQ(0, file->Read(ofs, mbr).obj);
  file->Close();
  LOG(INFO) << "Read " << ofs << " bytes from " << obj_path;
}

void DownloadConsumer(const GCE& gce, IoContext* io_context, FileQ* q) {
  string obj_path;

  asio::ssl::context ssl_cntx = GCE::CheckedSslContext();
  http::HttpsClientPool api_pool(GCE::kApiDomain, &ssl_cntx, io_context);
  api_pool.set_connect_timeout(2000);

  while (true) {
    fibers::channel_op_status st = q->pop(obj_path);
    if (st == fibers::channel_op_status::closed)
      break;
    CHECK_EQ(fibers::channel_op_status::success, st);

    DownloadFile(FLAGS_bucket, obj_path, gce, &api_pool);
  }
}

void Download(const GCE& gce, IoContextPool* pool) {
  FileQ file_q(64);

  fibers::future<void> consumer_future = fibers::async([&] {
    pool->AwaitFiberOnAll(
        [&](IoContext& io_context) { DownloadConsumer(gce, &io_context, &file_q); });
  });
  IoContext& io_context = pool->GetNextContext();
  asio::ssl::context ssl_cntx = GCE::CheckedSslContext();

  auto producer = [&] {
    GCS gcs(gce, &ssl_cntx, &io_context);
    CHECK_STATUS(gcs.Connect(2000));
    auto status = gcs.List(FLAGS_bucket, FLAGS_download, true,
                           [&](size_t sz, absl::string_view name) {
                             file_q.push(string(name)); });
    CHECK_STATUS(status);
  };

  io_context.AwaitSafe(producer);
  file_q.close();
  consumer_future.wait();
}

void Run(const GCE& gce, IoContext* context) {
  asio::ssl::context ssl_cntx = GCE::CheckedSslContext();

  GCS gcs(gce, &ssl_cntx, context);
  CHECK_STATUS(gcs.Connect(2000));

  if (!FLAGS_upload.empty()) {
    string upload_path = GCS::ToGcsPath(FLAGS_bucket, FLAGS_upload);
    LOG(INFO) << "Uploading data to " << upload_path;

    asio::ssl::context ssl_cntx = GCE::CheckedSslContext();
    http::HttpsClientPool api_pool(GCE::kApiDomain, &ssl_cntx, context);
    api_pool.set_connect_timeout(2000);

    file::WriteFile* wfile = CHECKED_GET(OpenGcsWriteFile(upload_path, gce, &api_pool));

    string contents(1 << 16, 'a');
    for (size_t i = 0; i < 100; ++i) {
      auto status = wfile->Write(contents);
      CHECK_STATUS(status);
    }
    CHECK(wfile->Close());
    return;
  }

  auto res = gcs.ListBuckets();
  CHECK_STATUS(res.status);
  for (const auto& s : res.obj) {
    cout << s << endl;
  }

  if (!FLAGS_read_path.empty()) {
    CHECK(!FLAGS_bucket.empty());
    string contents(6400, '\0');
    strings::MutableByteRange range(reinterpret_cast<uint8_t*>(&contents.front()), contents.size());
    auto res = gcs.Read(FLAGS_bucket, FLAGS_read_path, 0, range);
    CHECK_STATUS(res.status);
    contents.resize(res.obj);

    cout << FLAGS_read_path << ": " << res.obj << ":\n";
    cout << contents << "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
  }

  if (!FLAGS_prefix.empty()) {
    auto cb = [](size_t sz, absl::string_view name) {
                             cout << "Object: " << name << ", size: " << sz << endl; };
    auto status = gcs.List(FLAGS_bucket, FLAGS_prefix, true, cb);
    CHECK_STATUS(status);
  }
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  file_util::ReadFileToStringOrDie("/etc/ssl/certs/ca-certificates.crt", &ca_cert);

  GCE gce;
  CHECK_STATUS(gce.Init());
  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  if (!FLAGS_access_token.empty()) {
    gce.Test_InjectAcessToken(FLAGS_access_token);
  }

  if (!FLAGS_download.empty()) {
    Download(gce, &pool);
  } else {
    io_context.AwaitSafe([&] { Run(gce, &io_context); });
  }
  return 0;
}
