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
};

using namespace asio::ip;

TEST_F(SocketTest, Client) {
  detail::FiberClientSocket sock(1 << 16, &pool_->GetNextContext());

  sock.Initiate("localhost", std::to_string(port_));

  auto ec = sock.WaitToConnect(1000);
  EXPECT_FALSE(ec) << ec << "/" << ec.message();
  h2::request<h2::string_body> req{h2::verb::get, "/", 11};
  size_t written = h2::async_write(sock.socket(), req, fibers_ext::yield[ec]);

  beast::flat_buffer buffer;
  h2::response<h2::dynamic_body> resp;
  LOG(INFO) << "Before async read";
  size_t sz = h2::async_read(sock.socket(), buffer, resp, fibers_ext::yield[ec]);
  LOG(INFO) << "After async read";

}

}  // namespace util
