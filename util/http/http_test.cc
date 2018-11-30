// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/connect.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

namespace util {

using namespace boost;

class HttpTest : public testing::Test {
 protected:
  void SetUp() final;

  void TearDown() final;

  std::unique_ptr<AcceptServer> server_;
  std::unique_ptr<IoContextPool> pool_;
  http::Listener<> listener_;
  uint16_t port_ = 0;
};

void HttpTest::SetUp() {
  pool_.reset(new IoContextPool);
  pool_->Run();

  server_.reset(new AcceptServer(pool_.get()));
  port_ = server_->AddListener(0, &listener_);
  server_->Run();
}

void HttpTest::TearDown() {
  LOG(INFO) << "HttpTest::TearDown";

  server_.reset();
  VLOG(1) << "After server reset";
  pool_->Stop();
}

using namespace asio::ip;
namespace h2 = beast::http;

TEST_F(HttpTest, Client) {
  IoContext& io_context = pool_->GetNextContext();

  tcp::resolver resolver{io_context.get_context()};
  tcp::socket socket{io_context.get_context()};
  tcp::endpoint ep(tcp::v4(), port_);

  auto results = resolver.resolve(ep);
  system::error_code ec;

  // Make the connection on the IP address we get from a lookup
  asio::connect(socket, results.begin(), results.end(), ec);
  ASSERT_FALSE(ec);

  // Set the URL
  h2::request<h2::string_body> req{h2::verb::get, "/foo", 11};

  // Optional headers
  req.set(h2::field::user_agent, "mytest");

  // Send the HTTP request to the remote host.
  size_t sz = h2::write(socket, req, ec);
  ASSERT_FALSE(ec);
  EXPECT_GT(sz, 0);

  // This buffer is used for reading and must be persisted
  beast::flat_buffer buffer;

  // Declare a container to hold the response
  h2::response<h2::dynamic_body> res;

  // Receive the HTTP response
  sz = h2::read(socket, buffer, res, ec);
  ASSERT_FALSE(ec);
  EXPECT_GT(sz, 0);

  // Write the message to standard out
  VLOG(1) << res;
}

}  // namespace util
