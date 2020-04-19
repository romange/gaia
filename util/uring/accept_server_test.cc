// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/uring/accept_server.h"

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio_stream_adapter.h"
#include "util/uring/proactor_pool.h"
// #include "util/uring/uring_fiber_algo.h"

namespace util {
namespace uring {

using namespace boost;
using namespace std;
namespace h2 = beast::http;
using fibers_ext::BlockingCounter;

class TestConnection : public Connection {
 protected:
  void HandleRequests() final;
};

void TestConnection::HandleRequests() {
  CHECK(socket_.IsOpen());
  char buf[128];
  system::error_code ec;

  AsioStreamAdapter<FiberSocket> asa(socket_);

  while (true) {
    asa.read_some(asio::buffer(buf), ec);
    if (ec == std::errc::connection_aborted)
      break;
    CHECK(!ec) << ec << "/" << ec.message();

    asa.write_some(asio::buffer(buf), ec);

    if (FiberSocket::IsConnClosed(ec))
      break;

    CHECK(!ec);
  }
  VLOG(1) << "TestConnection exit";
}

class TestListener : public ListenerInterface {
 public:
  virtual Connection* NewConnection(Proactor* context) {
    return new TestConnection;
  }
};

class AcceptServerTest : public testing::Test {
 protected:
  void SetUp() override {
    pp_.reset(new ProactorPool(1));
    pp_->Run(16);
  }

  void TearDown() {
    pp_->Stop();
  }

  static void SetUpTestCase() {
  }

  using IoResult = Proactor::IoResult;

  std::unique_ptr<ProactorPool> pp_;
};

void RunClient(Proactor* proactor, BlockingCounter* bc) {
  LOG(INFO) << ": Ping-client started";
  FiberSocket fs;
  fs.set_proactor(proactor);
  AsioStreamAdapter<FiberSocket> asa(fs);

  FiberSocket::error_code ec;
  auto address = asio::ip::make_address("127.0.0.1");
  asio::ip::tcp::endpoint ep{address, 1234};

  ec = fs.Connect(ep);
  ASSERT_FALSE(ec) << ec;
  ASSERT_TRUE(fs.IsOpen());

  h2::request<h2::string_body> req(h2::verb::get, "/", 11);
  req.body().assign("foo");
  req.prepare_payload();
  h2::write(asa, req);

  bc->Dec();

  LOG(INFO) << ": echo-client stopped";
}

TEST_F(AcceptServerTest, Basic) {
  AcceptServer as(pp_.get());
  as.AddListener(1234, new TestListener);
  as.Run();

  Proactor client_proactor;
  thread t2([&] { client_proactor.Run(256); });

  fibers_ext::BlockingCounter bc(1);
  client_proactor.AsyncFiber(&RunClient, &client_proactor, &bc);

  bc.Wait();
  client_proactor.Stop();
  t2.join();

  as.Stop(true);
}

}  // namespace uring
}  // namespace util
