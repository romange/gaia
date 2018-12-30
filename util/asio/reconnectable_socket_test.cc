// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/reconnectable_socket.h"

#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/http/http_testing.h"

namespace util {

using namespace boost;
using namespace std;

class SocketTest : public HttpBaseTest {
 protected:
};

using namespace asio::ip;

TEST_F(SocketTest, Client) {
  detail::FiberClientSocket sock(1 << 16, &pool_->GetNextContext());

  sock.Initiate("localhost", std::to_string(port_));

  auto ec = sock.WaitToConnect(1000);
  EXPECT_FALSE(ec) << ec << "/" << ec.message();
}

}  // namespace util
