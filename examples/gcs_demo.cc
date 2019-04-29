// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include "base/init.h"
#include "base/logging.h"
#include "util/gce/gce.h"
#include "util/gce/gcs.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(bucket, "", "");
DEFINE_string(read_path, "", "");
DEFINE_string(prefix, "", "");
DEFINE_string(download, "", "");
DEFINE_string(access_token, "", "");

using FileQ = fibers::buffered_channel<string>;

void DownloadFile(StringPiece bucket, StringPiece obj_path, GCS* gcs) {
  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  CHECK_STATUS(gcs->OpenSequential(bucket, obj_path));

  size_t ofs = 0;
  strings::MutableByteRange mbr(buf.get(), kBufSize);

  while (true) {
    auto res = gcs->ReadSequential(mbr);
    CHECK_STATUS(res.status);
    ofs += res.obj;
    if (res.obj < mbr.size()) {
      break;
    }
  }

  CHECK_EQ(0, gcs->ReadSequential(mbr).obj);

  LOG(INFO) << "Read " << ofs << " bytes from " << obj_path;
}

void DownloadConsumer(const GCE& gce, IoContext* io_context, FileQ* q) {
  string obj_path;
  GCS gcs(gce, io_context);
  CHECK_STATUS(gcs.Connect(2000));
  while (true) {
    fibers::channel_op_status st = q->pop(obj_path);
    if (st == fibers::channel_op_status::closed)
      break;
    CHECK_EQ(fibers::channel_op_status::success, st);

    DownloadFile(FLAGS_bucket, obj_path, &gcs);
  }
}

void Download(const GCE& gce, IoContextPool* pool) {
  FileQ file_q(64);

  fibers::future<void> consumer_future = fibers::async([&] {
    pool->AwaitFiberOnAll(
        [&](IoContext& io_context) { DownloadConsumer(gce, &io_context, &file_q); });
  });
  IoContext& io_context = pool->GetNextContext();

  auto producer = [&] {
    GCS gcs(gce, &io_context);
    CHECK_STATUS(gcs.Connect(2000));
    auto status = gcs.List(FLAGS_bucket, FLAGS_download,
                           [&](absl::string_view name) {
                             file_q.push(string(name));
                            });
    CHECK_STATUS(status);
  };

  io_context.AwaitSafe(producer);
  file_q.close();
  consumer_future.wait();
}

void Run(const GCE& gce, IoContext* context) {
  GCS gcs(gce, context);
  CHECK_STATUS(gcs.Connect(2000));
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
    auto status = gcs.List(FLAGS_bucket, FLAGS_prefix,
                           [](absl::string_view name) { cout << "Object: " << name << endl; });
    CHECK_STATUS(status);
  }
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

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
