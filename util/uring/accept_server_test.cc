// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/uring/accept_server.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "util/uring/proactor.h"
#include "util/uring/uring_fiber_algo.h"

namespace util {
namespace uring {

using namespace boost;
using namespace std;

class TestConnection : public Connection {
  protected:
    void HandleRequests(Proactor* proactor) final;
};

void TestConnection::HandleRequests(Proactor* proactor) {
  CHECK(socket_.IsOpen());
  while (false) {
    // TBD.
  }
}

class TestListener : public ListenerInterface {
public:
  virtual Connection* NewConnection(Proactor* context) { return new TestConnection; }
};

class AcceptServerTest : public testing::Test {
 protected:
  void SetUp() override {
    proactor_ = std::make_unique<Proactor>(16);
    proactor_thread_ = thread{[&] { proactor_->Run(); }};
  }

  void TearDown() {
    proactor_->Stop();
    proactor_thread_.join();
    proactor_.reset();
  }

  static void SetUpTestCase() {
  }

  using IoResult = Proactor::IoResult;

  std::unique_ptr<Proactor> proactor_;
  std::thread proactor_thread_;
};

TEST_F(AcceptServerTest, Basic) {
  AcceptServer as(proactor_.get());
  as.AddListener(1234, new TestListener);
  as.Run();

  system::error_code ec;
  auto address = asio::ip::make_address("127.0.0.1", ec);
  asio::ip::tcp::endpoint ep{address, 1234};

  Proactor client_proactor(256);
  thread t2([&] {client_proactor.Run(); });
  client_proactor.AsyncFiber([&] {
    FiberSocket fs;

    error_code ec = fs.Connect(&client_proactor, ep);
    ASSERT_FALSE(ec) << ec;
    ASSERT_TRUE(fs.IsOpen());
  });

  usleep(2000);
  client_proactor.Stop();
  t2.join();

  as.Stop(true);
}

}  // namespace uring
}  // namespace util
