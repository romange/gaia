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
#include "util/asio/reconnectable_socket.h"
#include "util/http/http_client.h"
#include "util/http/http_testing.h"

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
  server_->Wait();  // To wait till server stops.

  ec = client.Get("/", &res);
  EXPECT_TRUE(ec);
  EXPECT_FALSE(client.IsConnected());
}

TEST_F(HttpTest, ReconnectSocket) {
  fibers_ext::Done done;
  namespace h2 = beast::http;

  listener_.RegisterCb("/cb", false,
                       [&](const http::QueryArgs& args, http::HttpHandler::SendFunction* send) {
                         http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
                         done.Wait();
                         return send->Invoke(std::move(resp));
                       });

  ReconnectableSocket socket("localhost", std::to_string(port_), &pool_->GetNextContext());
  system::error_code ec = socket.Connect(100);
  ASSERT_FALSE(ec);

  auto read_cb = [s = &socket.socket()](const system::error_code& ec) {
    LOG(INFO) << "Got AsyncWait: " << ec << " " << ec.message();
    char buf[1];
    system::error_code rec;
    size_t val = s->receive(make_buffer_seq(buf), ReconnectableSocket::socket_t::message_peek, rec);
    LOG(INFO) << "Received " << rec.message() << " / " << val;
  };

  socket.socket().async_wait(tcp::socket::wait_read, read_cb);

  string message(1024, 'A');
  message.append("\r\n\r\n\r\n");
  ec = socket.Write(make_buffer_seq(message));
  ASSERT_FALSE(ec);
  LOG(INFO) << "After socket write";
  ec = socket.Read(make_buffer_seq(message));

  LOG(INFO) << "After readsome " << ec.message() << " "
            << " status: " << socket.status();
  this_fiber::sleep_for(10ms);
  LOG(INFO) << "After sleep status: " << socket.status();

  h2::request<h2::string_body> req{h2::verb::get, "/", 11};
  h2::response<h2::dynamic_body> resp;
  socket.socket().async_wait(tcp::socket::wait_read, read_cb);

  h2::async_write(socket.socket(), req, fibers_ext::yield[ec]);
  ASSERT_FALSE(ec);
  beast::flat_buffer buffer;
  h2::async_read(socket.socket(), buffer, resp, fibers_ext::yield[ec]);
  ASSERT_FALSE(ec);
}

}  // namespace util
