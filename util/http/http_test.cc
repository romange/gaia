// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"
#include "util/http/http_testing.h"

namespace util {
namespace http {
using namespace boost;
using namespace std;

class HttpTest : public HttpBaseTest {
 protected:
};

using namespace asio::ip;
namespace h2 = beast::http;

TEST_F(HttpTest, Client) {
  IoContext& io_context = pool_->GetNextContext();

  Client client(&io_context);

  system::error_code ec = client.Connect("localhost", std::to_string(port_));
  ASSERT_FALSE(ec) << ec << " " << ec.message();

  ASSERT_TRUE(client.IsConnected());

  Client::Response res;
  ec = client.Send(h2::verb::get, "/", &res);
  ASSERT_FALSE(ec) << ec;

  // Write the message to standard out
  VLOG(1) << res;

  server_->Stop();
  server_->Wait();  // To wait till server stops.

  ec = client.Send(h2::verb::get, "/", &res);
  EXPECT_TRUE(ec);
  this_thread::sleep_for(10ms);
  
  EXPECT_FALSE(client.IsConnected());
}

}  // namespace http
}  // namespace util
