// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/accept_server.h"

#include <boost/fiber/mutex.hpp>

#include "base/logging.h"
#include "util/asio/io_context_pool.h"
#include "util/asio/yield.h"

namespace util {

using namespace boost;
using asio::ip::tcp;

AcceptServer::AcceptServer(unsigned short port, IoContextPool* pool, ConnectionFactory cf)
     :pool_(pool), io_cntx_(pool->GetNextContext()),
      acceptor_(io_cntx_),
      signals_(io_cntx_, SIGINT, SIGTERM),
      cf_(cf) {
  tcp::endpoint endpoint(tcp::v4(), port);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);

  constexpr int kMaxBacklogPendingConnections = 64;
  acceptor_.listen(kMaxBacklogPendingConnections);
  tcp::endpoint le = acceptor_.local_endpoint();
  port_ = le.port();

  LOG(INFO) << "AcceptServer - listening on port " << port_;

  signals_.async_wait(
  [this](system::error_code /*ec*/, int /*signo*/) {
    // The server is stopped by cancelling all outstanding asynchronous
    // operations. Once all operations have finished the io_context::run()
    // call will exit.
    acceptor_.close();
  });
}

AcceptServer::~AcceptServer() {
  Stop();
  Wait();
}

void AcceptServer::RunInIOThread() {
  // We assume here that intrusive list is thread-safe, i.e. I can safely push_back from one
  // thread and unlink from another different items. Based on scarce documentation it seems to be fine.
  util::ConnectionHandlerList clist;
  DCHECK(clist.empty());
  ConnectionServerNotifier notifier;
  system::error_code ec;
  util::ConnectionHandler* handler = nullptr;
  try {
    for (;;) {
       std::tie(handler,ec) = AcceptFiber(&notifier);
       if (ec) {
         CHECK(!handler);
         break; // TODO: To refine it.
      } else {
        VLOG(1) << "Accepted socket " << handler->socket().remote_endpoint();
        CHECK_NOTNULL(handler);
        clist.push_back(*handler);
        DCHECK(!clist.empty());

        DCHECK(handler->hook_.is_linked());
        handler->Run();
      }
    }
  } catch (std::exception const& ex) {
    LOG(WARNING) << ": caught exception : " << ex.what();
  }

  // We protect clist because we iterate over it and other threads could potentialy change it.
  auto lock = notifier.Lock();

  if (!clist.empty()) {
    VLOG(1) << "Closing " << clist.size() << " connections";

    for (auto it = clist.begin(); it != clist.end(); ++it) {
      it->Close();
    }

    VLOG(1) << "Waiting for connections to close";
    notifier.Wait(lock, [&clist] { return clist.empty(); });
  }

  // Notify that AcceptThread has stopped.
  done_.notify();

  LOG(INFO) << ": Accept server stopped";
}

auto AcceptServer::AcceptFiber(ConnectionServerNotifier* notifier) -> AcceptResult {
  auto& io_cntx = pool_->GetNextContext();

  std::unique_ptr<util::ConnectionHandler> conn(cf_(&io_cntx, notifier));
  system::error_code ec;
  acceptor_.async_accept(conn->socket(), fibers_ext::yield[ec]);

  if (ec)
    return AcceptResult(nullptr, ec);
  else
    return AcceptResult(conn.release(), ec);
}


void AcceptServer::Run() {
  asio::post(io_cntx_, [this] {
      fibers::fiber srv_fb(&AcceptServer::RunInIOThread, this);
      srv_fb.detach();
    });
  was_run_ = true;
}

void AcceptServer::Wait() {
  if (was_run_)
    done_.wait();
}

}  // namespace util
