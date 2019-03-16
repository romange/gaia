// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
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
DEFINE_bool(compress, false, "");

using namespace mr3;
using namespace util;

string ShardNameFunc(const std::string& line) {
  return absl::StrFormat("shard-%04d", base::Fingerprint32(line) % 10);
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

  server->CallOnStopSignal([&] { executor.Stop();});

  /*
1. How do we allow flush hooks ?
2. How do we allow joining of several collections of different types into a single
joining object?

mr3::Input<rapidjson::Document> inp1 = p.ReadText("inp1", input1.json.gz").AsJson();
     mr3::Input<rapidjson::Document> inp2 = p.ReadText("inp2", "input2.json.gz").AsJson();

     mr3::PCollection<pb::AddressBook> col1 = inp1.Apply("MyApplyFunc",
        [] (rapidjson::Document&& doc, mr3::DoContext<pb::AddressBook>* ctx) {
          pb::AddressBook res;
          ctx->Write(std::move(res));
        });

  col1.ShardByKey([](const pb::AddressBook& ab) { return ab.user_id();});
  col1.Write(...);

  mr3::PCollection<pb::UserInfo> col2 = inp2.Apply("ReadUsers",
        [] (rapidjson::Document&& doc, mr3::DoContext<pb::UserInfo>* ctx) {
          pb::AddressBook res;
          ctx->Write(std::move(res));
        });

  template <typename TOwner, void(TOwner::*func)()>
void Call(TOwner *p) {
    (p->*func)();
}

  MyJoinerToC()  {
 public:
   typedef pb::AddressBook OutputType;  // Must tell the joiner type.

   void OnA(A&& a, mr3::DoContext<OutputType>* ctx) {}
   void OnB(B&& b, mr3::DoContext<OutputType>* ctx) {}

   void Flush(mr3::DoContext<OutputType>* ctx);
}


  mr3::PCollection<pb::AddressBook> merged = p.JoinWith<MyJoiner>().On(col1,
    &MyJoiner::OnA).On(col2, &MyJoiner::OnB});

  merged.
*/

  // TODO: Should return Input<string> or something which can apply an operator.
  StringStream& ss = p.ReadText("inp1", inputs);
  auto& outp =
  ss/*.Apply([](std::string&& inp, DoContext<std::string>* context) {
      context->Write(inp.substr(0, 5));
    })*/
      .Write("outp1", pb::WireFormat::TXT).WithSharding(ShardNameFunc);
  if (FLAGS_compress) {
    outp.AndCompress(pb::Output::GZIP);
  }

  executor.Run(&p.input("inp1"), &ss);
  executor.Shutdown();

  server->Stop(true);
  pool.Stop();

  return 0;
}
