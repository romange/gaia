// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/connection_handler.h"

#include <boost/asio/write.hpp>

#include "base/logging.h"

using boost::fibers::condition_variable;
using boost::fibers::fiber;
using namespace boost;
namespace util {

ConnectionHandler::ConnectionHandler(io_context* io_svc, condition_variable* cv )
    : socket_(*io_svc), on_exit_(*CHECK_NOTNULL(cv)) {
}

ConnectionHandler::~ConnectionHandler() {
}

void ConnectionHandler::Run() {
  auto& cntx = socket_.get_io_context();
  cntx.post([this] {
    fiber(&ConnectionHandler::RunInIOThread, this).detach();
  });
}

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
void ConnectionHandler::RunInIOThread() {
  VLOG(1) << "ConnectionHandler::RunInIOThread";
  try {
    while (true) {
      system::error_code ec = HandleRequest();
      if (ec) {
        if (ec != asio::error::eof) {
          LOG(WARNING) << "Error : " << ec.message();
        }
        break;
      }
    }
    VLOG(1) << ": ConnectionHandler closed";
  } catch ( std::exception const& ex) {
    ex.what();
  }

  socket_.close();
  hook_.unlink();   // We unlink manually because we delete 'this' later.
  on_exit_.notify_one();

  delete this;
}

}  // namespace util
