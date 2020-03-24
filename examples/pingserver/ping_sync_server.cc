#include <boost/asio/io_context.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <thread>

#include <iostream>
#include <memory>

#include "base/init.h"
#include "examples/pingserver/ping_command.h"

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");

using namespace boost;
using asio::ip::tcp;
using std::string;

inline bool IsExpectedFinish(system::error_code ec) {
  using namespace boost::asio;
  return ec == error::eof || ec == error::operation_aborted || ec == error::connection_reset ||
         ec == error::not_connected;
}

void RedisConnection(tcp::socket sock) {
  PingCommand cmd;

  while (true) {
    system::error_code ec;
    size_t length = sock.read_some(cmd.read_buffer(), ec);
    if (ec) {
      LOG_IF(ERROR, !IsExpectedFinish(ec)) << ec;
      break;
    }

    if (cmd.Decode(length)) {
      asio::write(sock, cmd.reply());
    }
  }
}

void ServePing(boost::asio::io_context& io_context, const tcp::endpoint& endpoint) {
  tcp::acceptor a(io_context, endpoint);

  for (;;) {
    tcp::socket sock(io_context);
    a.accept(sock);

    LOG(INFO) << "Accepted socket from " << sock.remote_endpoint();

    std::thread{&RedisConnection, std::move(sock)}.detach();
  }
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  tcp::endpoint endpoint(tcp::v4(), FLAGS_port);
  boost::asio::io_context io_context;

  try {
    ServePing(io_context, endpoint);
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
