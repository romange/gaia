// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_testing.h"
#include "util/http/http_client.h"

namespace util {

using namespace boost;
using namespace std;

class HttpTest : public HttpBaseTest {
 protected:
};

using namespace asio::ip;

TEST_F(HttpTest, Client) {
  IoContext& io_context = pool_->GetNextContext();

  http::Client client(&io_context);

  system::error_code ec = client.Connect("localhost", std::to_string(port_));
  ASSERT_FALSE(ec) << ec << " " << ec.message();

  ASSERT_TRUE(client.IsConnected());

  http::Client::Response res;
  ec = client.Get("/", &res);
  ASSERT_FALSE(ec) << ec;

  // Write the message to standard out
  VLOG(1) << res;

  server_->Stop();
  ec = client.Get("/", &res);
  ASSERT_FALSE(client.IsConnected());
  ASSERT_TRUE(ec);
}

}  // namespace util
