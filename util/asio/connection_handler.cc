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


void ConnectionHandler::Notifier::Unlink(ConnectionHandler* item) noexcept {
  bool is_empty = false;
  {
    auto lock = Lock();
    list_->erase(ListType::s_iterator_to(*item));
    is_empty = list_->empty();
  }
  if (is_empty)
    cnd_.notify_one();
}

ConnectionHandler::ConnectionHandler() noexcept {
}

ConnectionHandler::~ConnectionHandler() {
}

void ConnectionHandler::Run() {
  CHECK(notifier_);

  auto& cntx = socket_->get_io_context();

  cntx.post([guard = ptr_t(this)] {
    // As long as fiber is running, 'this' is protected from deletion.
    fiber(&ConnectionHandler::RunInIOThread, std::move(guard)).detach();
  });
}

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
void ConnectionHandler::RunInIOThread() {
  CHECK(socket_);

  VLOG(1) << "ConnectionHandler::RunInIOThread";
  system::error_code ec;
  try {
    while (socket_->is_open()) {
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
    LOG(ERROR) << ex.what();
  }

  if (socket_->is_open())
    socket_->close(ec);

  notifier_->Unlink(this);

  // RunInIOThread runs as lambda packaged with ptr_t guard on this. Once the lambda finishes,
  // it releases the ownership over this.
}

void ConnectionHandler::Close() {
  auto& cntx = socket_->get_executor().context();

  // We close asynchronously via the thread that owns the socket to ensure thread-safety
  // for that connection.

  // We use intrusive guard to increment the reference of this in order to allow
  // safe callback execution even if RunInIOThread released the ownership.
  cntx.post([guard = ptr_t(this)] {
    system::error_code ec;
    if (guard->socket_->is_open())
      guard->socket_->close(ec);
    LOG_IF(INFO, ec) << "Error closing socket " << ec.message();
  });
}


}  // namespace util
