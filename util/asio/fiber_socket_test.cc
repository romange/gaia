// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/fiber_socket.h"

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/http/http_testing.h"

namespace util {

using namespace boost;
using namespace std;
namespace h2 = beast::http;

class SocketTest : public HttpBaseTest {

 protected:
  void SetUp() final {
    HttpBaseTest::SetUp();
    sock_.reset(new FiberSyncSocket("localhost", std::to_string(port_), &pool_->GetNextContext()));
    auto ec = sock_->ClientWaitToConnect(1000);
    CHECK(!ec) << ec.message();
  }

  void TearDown() final {
    LOG(INFO) << "TearDown";
    sock_.reset();

    HttpBaseTest::TearDown();
  }

  size_t ReadResp(system::error_code& ec) {
    beast::flat_buffer buffer;
    h2::response<h2::dynamic_body> resp;

    return h2::read(*sock_, buffer, resp, ec);
  }

  std::unique_ptr<FiberSyncSocket> sock_;
};

using namespace asio::ip;
using namespace http;

TEST_F(SocketTest, Normal) {
  system::error_code ec;
  h2::request<h2::string_body> req{h2::verb::get, "/", 11};
  size_t written = h2::write(*sock_, req, ec);
  EXPECT_GT(written, 0);

  sock_->context().AwaitSafe([&] {
    size_t sz = ReadResp(ec);
    EXPECT_FALSE(ec);
    EXPECT_GT(sz, 0);
  });

  FiberSyncSocket fss("localhost", std::to_string(port_), &pool_->GetNextContext());
}

TEST_F(SocketTest, Move) {
  std::unique_ptr<FiberSyncSocket> local_socket;
  system::error_code ec;
  h2::request<h2::string_body> req{h2::verb::get, "/", 11};

  sock_->context().AwaitSafe([&] {
    local_socket.reset(new FiberSyncSocket(std::move(*sock_)));
    ASSERT_FALSE(sock_->is_open());

    size_t written = h2::write(*local_socket, req, ec);
    EXPECT_GT(written, 0);
  });
  

}

TEST_F(SocketTest, StarvedRead) {
  fibers_ext::Done done;
  listener_.RegisterCb("/null", false,  // does not send anything.
                       [&](const http::QueryArgs& args, HttpHandler::SendFunction* send) {
                         StringResponse resp = MakeStringResponse(h2::status::ok);
                         done.Wait();
                         LOG(INFO) << "NullCb run";
                         return send->Invoke(std::move(resp));
                       });

  h2::request<h2::string_body> req{h2::verb::get, "/null", 11};
  system::error_code ec;
  size_t written = h2::write(*sock_, req, ec);
  EXPECT_GT(written, 0);

  fibers::fiber fb = sock_->context().LaunchFiber([&] {
      size_t sz = ReadResp(ec);
      EXPECT_FALSE(ec);
      EXPECT_GT(sz, 0);
  });

  this_fiber::sleep_for(10ms);
  done.Notify();
  fb.join();
  LOG(INFO) << "After fb.join";
}

TEST_F(SocketTest, Reconnect) {
  server_.reset();
  this_fiber::sleep_for(5ms);
  system::error_code ec = sock_->status();
  EXPECT_TRUE(ec);

  size_t read_sz = sock_->context().AwaitSafe([&] {
    char buf[4];
    size_t res = sock_->read_some(asio::buffer(buf), ec);
    EXPECT_TRUE(ec);
    return res;
  });
  EXPECT_EQ(0, read_sz);

  // Restart server and setup connection.
  server_.reset(new AcceptServer(pool_.get()));
  server_->AddListener(port_, &listener_);
  server_->Run();

  ec = sock_->ClientWaitToConnect(100);
  EXPECT_FALSE(ec) << ec << "/" << ec.message();

  h2::request<h2::string_body> req{h2::verb::get, "/", 11};
  size_t written = h2::write(*sock_, req, ec);
  EXPECT_GT(written, 0);

  sock_->context().AwaitSafe([&] {
    size_t sz = ReadResp(ec);
    EXPECT_FALSE(ec);
    EXPECT_GT(sz, 0);
  });
}

}  // namespace util
