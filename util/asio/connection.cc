// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/connection.h"

#include <boost/asio/write.hpp>

#include "base/logging.h"
#include "util/asio/yield.h"
#include "util/http/varz_stats.h"

http::VarzQps qps("echo-qps");


using boost::fibers::condition_variable;
using boost::fibers::fiber;
using namespace boost;
namespace util {

Connection::Connection(io_context* io_svc, condition_variable* cv )
    : socket_(*io_svc), on_exit_(*CHECK_NOTNULL(cv)) {}

void Connection::Run() {
  auto& cntx = socket_.get_io_context();
  cntx.post([this] {
    fiber(&Connection::Session, this).detach();
  });
}

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
void Connection::Session() {
  try {
    while (true) {
      system::error_code ec = HandleOne();
      if (ec) {
        if (ec != asio::error::eof) {
          LOG(WARNING) << "Error : " << ec;
        }
        break;
      }
    }
    VLOG(1) << ": connection closed";
  } catch ( std::exception const& ex) {
    ex.what();
  }

  socket_.close();
  hook_.unlink();   // We unlink manually because we delete 'this' later.
  on_exit_.notify_one();

  delete this;
}

boost::system::error_code Connection::HandleOne() {
  constexpr unsigned max_length = 1024;

  char data[max_length];
  system::error_code ec;

  std::size_t length = socket_.async_read_some(asio::buffer(data), fibers::yield[ec]);

  if (ec) {
    return ec;
  }
  VLOG(1) << ": handled: " << std::string(data, length);
  qps.Inc();

  asio::async_write(socket_, asio::buffer(data, length), fibers::yield[ec]);
  return ec;
}

}  // namespace util
