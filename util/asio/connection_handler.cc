// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/connection_handler.h"

#include <boost/asio/write.hpp>

#include "base/logging.h"

using namespace boost;
using namespace boost::asio;
using fibers::condition_variable;
using fibers::fiber;

namespace util {


void ConnectionServerNotifier::Unlink(detail::connection_hook* hook) {
  {
    auto lock = Lock();
    hook->unlink();   // We unlink manually because we delete 'this' later.
  }
  cnd_.notify_one();
}

ConnectionHandler::ConnectionHandler(io_context* io_svc, ConnectionServerNotifier* notifier)
    : socket_(*io_svc), notifier_(CHECK_NOTNULL(notifier)) {
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
  system::error_code ec;
  try {
    while (socket_.is_open()) {
      ec = HandleRequest();
      if (ec) {
        if (ec != error::eof && ec != error::operation_aborted) {
          LOG(WARNING) << "Error : " << ec.message();
        }
        break;
      }
    }
    VLOG(1) << ": ConnectionHandler closed";
  } catch ( std::exception const& ex) {
    ex.what();
  }

  if (socket_.is_open())
    socket_.close(ec);

  notifier_->Unlink(&hook_);
  delete this;
}

void ConnectionHandler::Close() {
  auto& cntx = socket_.get_executor().context();
  cntx.post([this] {
    system::error_code ec;
    socket_.close(ec);
    LOG_IF(INFO, !ec) << "Error closing socket " << ec.message();
  });
}


}  // namespace util
