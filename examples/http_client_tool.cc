// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "absl/strings/str_split.h"
#include "strings/stringpiece.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"

using namespace util;
using namespace std;

DEFINE_string(connect, "localhost:8080", "");

class HttpClientFiber : public IoContext::Cancellable {
  IoContext& cntx_;
  http::Client client_;
  bool cancelled_ = false;

 public:
  HttpClientFiber(IoContext& cntx) : cntx_(cntx), client_(cntx_) {}

  void Run() final {
    auto ec = client_.Connect("www.walla.com", "http");
    if (ec) {
      LOG(INFO) << "Did not connect " << ec.message();
      return;
    }

    while (!cancelled_) {
      http::Client::Response resp;
      ec = client_.Get("/", &resp);
      LOG_IF(WARNING, ec) << ec.message();
      if (ec)
        break;
    }
    LOG(INFO) << "Finished the run";
  }

  void Cancel() final {
    cancelled_ = true;
    client_.Cancel();
  };
};

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  IoContextPool pool;
  pool.Run();
  vector<StringPiece> parts = absl::StrSplit(FLAGS_connect, ":");
  CHECK_EQ(2, parts.size());

  IoContext& cntx = pool.GetNextContext();
  cntx.AttachCancellable(new HttpClientFiber(cntx));

  SleepForMilliseconds(500);
  LOG(INFO) << "Stopping pool";
  pool.Stop();

  return 0;
}
