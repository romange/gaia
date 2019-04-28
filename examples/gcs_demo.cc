// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

#include "base/init.h"
#include "base/logging.h"
#include "util/gce/gce.h"
#include "util/gce/gcs.h"

#include "util/asio/io_context_pool.h"
#include "util/asio/accept_server.h"
#include "util/http/http_client.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_string(bucket, "", "");
DEFINE_string(read_path, "", "");
DEFINE_string(prefix, "", "");


/*std::string Stringify(const rj::Value& value) {
  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  value.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
}*/




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

    cout << FLAGS_read_path << ": " << res.obj << ":\n";
    cout << contents << "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
  }

  if (!FLAGS_prefix.empty()) {
    auto status = gcs.List(FLAGS_bucket, FLAGS_prefix, [](absl::string_view name) {
      cout << "Object: " << name << endl;
    });
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

  io_context.AwaitSafe([&] { Run(gce, &io_context); });

  return 0;
}
