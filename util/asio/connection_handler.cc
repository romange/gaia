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

bool IsExpectedFinish(system::error_code ec) {
  return ec == error::eof || ec == error::operation_aborted || ec == error::connection_reset;
}

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

void ConnectionHandler::Init(socket_t&& sock, Notifier* notifier) {
  CHECK(!socket_);

  socket_.emplace(std::move(sock));
  notifier_ = notifier;

  ip::tcp::no_delay nd(true);
  system::error_code ec;
  socket_->set_option(nd, ec);
  if (ec)
    LOG(ERROR) << "Could not see socket option " << ec.message() << " " << ec;
  socket_->non_blocking(true, ec);

  if (ec)
    LOG(ERROR) << "Could not make socket nonblocking " << ec.message() << " " << ec;
}

void ConnectionHandler::Run() {
  CHECK(notifier_);

  asio::post(socket_->get_executor(), [guard = ptr_t(this)] {
    // As long as fiber is running, 'this' is protected from deletion.
    fiber(&ConnectionHandler::RunInIOThread, std::move(guard)).detach();
  });
}

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
void ConnectionHandler::RunInIOThread() {
  CHECK(socket_);

  VLOG(1) << "ConnectionHandler::RunInIOThread. socket " << socket_->native_handle();
  system::error_code ec;
  is_open_ = socket_->is_open();
  try {
    while (is_open_) {
      ec = HandleRequest();
      if (ec) {
        if (!IsExpectedFinish(ec)) {
          LOG(WARNING) << "Error : " << ec.message() << ", "
                       << ec.category().name() << "/" << ec.value();
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
  is_open_ = false;

  // We close asynchronously via the thread that owns the socket to ensure thread-safety
  // for that connection.
  // We use intrusive ptr to increment the reference of this in order to allow
  // safe callback execution even if RunInIOThread released the ownership.
  asio::post(socket_->get_executor(), [me = ptr_t(this)] {
    // The socket might already be closed if RunInIOThread has finished running.
    if (me->socket_->is_open()) {
      system::error_code ec;

      me->socket_->cancel(ec);
      me->socket_->shutdown(socket_t::shutdown_receive, ec);

      LOG_IF(INFO, ec) << "Error closing socket " << me->socket_->native_handle()
                       << ": " << ec.message();
    }

  });
}


}  // namespace util
