// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "absl/strings/str_cat.h"

#include "base/init.h"
#include "base/logging.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "mr/mr_executor.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

#include "util/status.h"

using namespace std;
using namespace boost;
using namespace util;

DEFINE_uint32(http_port, 8080, "Port number.");
DEFINE_uint32(mr_threads, 0, "Number of mr threads");

using namespace mr3;
using namespace util;

string ShardNameFunc(const std::string& line) {
  char buf[32];
  snprintf(buf, sizeof buf, "shard-%04d", base::Fingerprint32(line) % 10);
  return buf;
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  std::vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!inputs.empty());

  IoContextPool pool(FLAGS_mr_threads);
  pool.Run();

  std::unique_ptr<util::AcceptServer> server(new AcceptServer(&pool));
  util::http::Listener<> http_listener;
  uint16_t port = server->AddListener(FLAGS_http_port, &http_listener);
  LOG(INFO) << "Started http server on port " << port;
  server->Run();

  Pipeline p;

  Executor executor("/tmp/mr3", &pool);
  executor.Init();

  // TODO: Should return Input<string> or something which can apply an operator.
  StringStream& ss = p.ReadText("inp1", inputs);
  ss.Apply([](std::string&& inp, DoContext<std::string>* context) {
      context->Write(inp.substr(0, 5));
    })
      .Write("outp1", pb::WireFormat::TXT)
      .AndCompress(pb::Output::GZIP)
      .WithSharding(ShardNameFunc);

  executor.Run(&p.input("inp1"), &ss);
  executor.Shutdown();

  server->Stop(true);
  pool.Stop();

  return 0;
}
