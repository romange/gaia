// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/reconnectable_socket.h"

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
    sock_.reset(new detail::FiberClientSocket(&pool_->GetNextContext(), 1 << 16));
    sock_->Initiate("localhost", std::to_string(port_));
  }

  void TearDown() final {
    sock_.reset();

    HttpBaseTest::TearDown();
  }

  size_t ReadResp(system::error_code& ec) {
    beast::flat_buffer buffer;
    h2::response<h2::dynamic_body> resp;

    return h2::read(*sock_, buffer, resp, ec);
  }

  std::unique_ptr<detail::FiberClientSocket> sock_;
};

using namespace asio::ip;
using namespace http;

TEST_F(SocketTest, Normal) {
  auto ec = sock_->WaitToConnect(1000);
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

TEST_F(SocketTest, StarvedRead) {
  fibers_ext::Done done;

  listener_.RegisterCb("/null", false,  // does not send anything.
                       [&](const http::QueryArgs& args, HttpHandler::SendFunction* send) {
                         StringResponse resp = MakeStringResponse(h2::status::ok);
                         done.Wait();
                         LOG(INFO) << "NullCb run";
                         return send->Invoke(std::move(resp));
                       });

  auto ec = sock_->WaitToConnect(1000);

  EXPECT_FALSE(ec) << ec << "/" << ec.message();
  h2::request<h2::string_body> req{h2::verb::get, "/null", 11};
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
  auto ec = sock_->WaitToConnect(1000);
  EXPECT_FALSE(ec) << ec << "/" << ec.message();

  server_.reset();
  this_fiber::sleep_for(5ms);
  ec = sock_->status();
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

  ec = sock_->WaitToConnect(100);
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
