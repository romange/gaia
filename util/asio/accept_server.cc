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

AcceptServer::ListenerWrapper::ListenerWrapper(const endpoint& ep, IoContext* io_context,
                                               ListenerInterface* si)
    : io_context(*io_context), acceptor(io_context->raw_context(), ep), listener(si) {
  port = acceptor.local_endpoint().port();
}

AcceptServer::AcceptServer(IoContextPool* pool)
    : pool_(pool), signals_(pool->GetNextContext().raw_context(), SIGINT, SIGTERM), bc_(1) {
  signals_.async_wait([this](system::error_code /*ec*/, int /*signo*/) {
    // The server is stopped by cancelling all outstanding asynchronous
    // operations. Once all operations have finished the io_context::run()
    // call will exit.
    for (auto& l : listeners_) {
      if (l.acceptor.is_open()) {
        asio::post(l.acceptor.get_executor(), [acc = &l.acceptor]() mutable { acc->close(); });
      }
    }

    bc_.Dec();
  });
}

AcceptServer::~AcceptServer() {
  Stop();
  Wait();
}

unsigned short AcceptServer::AddListener(unsigned short port, ListenerInterface* si) {
  CHECK(si);

  // We can not allow dynamic listener additions because listeners_ might reallocate.
  CHECK(!was_run_);

  si->RegisterPool(pool_);

  tcp::endpoint endpoint(tcp::v4(), port);
  IoContext& io_context = pool_->GetNextContext();
  listeners_.emplace_back(endpoint, &io_context, si);
  auto& listener = listeners_.back();

  LOG(INFO) << "AcceptServer - listening on port " << listener.port;

  return listener.port;
}

void AcceptServer::RunInIOThread(ListenerWrapper* wrapper) {
  CHECK(wrapper->io_context.InContextThread());

  ConnectionHandler::ListType clist;

  this_fiber::properties<IoFiberProperties>().SetNiceLevel(4);

  fibers::condition_variable clist_empty_cnd;
  system::error_code ec;
  util::ConnectionHandler* handler = nullptr;

  // We release intrusive pointer in our thread by delegating the code to accpt_cntxt.
  // Please note that since we update clist in the same thread, we do not need mutex
  // to protect the state.
  auto clean_cb = [&, &accpt_cntxt = wrapper->io_context](ConnectionHandler::ptr_t p) {
    accpt_cntxt.Async([&, p = std::move(p)]() mutable {
      p.reset();
      if (clist.empty()) {
        clist_empty_cnd.notify_one();
      }
    });
  };

  try {
    for (;;) {
      std::tie(handler, ec) = AcceptConnection(wrapper);
      if (ec) {
        CHECK(!handler);
        if (ec == asio::error::try_again)
          continue;
        break;  // TODO: To refine it.
      } else {
        CHECK_NOTNULL(handler);
        clist.push_front(*handler);

        DCHECK(!clist.empty());
        DCHECK(handler->hook_.is_linked());

        handler->context().AsyncFiber(
            [clean_cb](ConnectionHandler::ptr_t guard) {
              guard->RunInIOThread();
              clean_cb(std::move(guard));
            }, handler);
      }
    }
  } catch (std::exception const& ex) {
    LOG(WARNING) << ": caught exception : " << ex.what();
  }

  wrapper->listener->PreShutdown();

  if (!clist.empty()) {
    VLOG(1) << "Starting closing connections";
    auto it = clist.begin();
    unsigned cnt = 0;
    while (it != clist.end()) {
      // guarding the current item, preserving it for getting the next item.
      // The reason for this is it->Close() is interruptable.
      ConnectionHandler::ptr_t guard(&*it);
      it->Close();
      ++it;
      ++cnt;
    }

    VLOG(1) << "Closed " << cnt << " connections";

    // lk is really redundant but is required by cv-interface:
    // We update clist only in this thread so the protection is not needed.
    fibers::mutex empty_mu;
    std::unique_lock<fibers::mutex> lk(empty_mu);
    clist_empty_cnd.wait(lk, [&] { return clist.empty(); });
  }

  wrapper->listener->PostShutdown();

  LOG(INFO) << "Accept server stopped for port " << wrapper->port;

  // Notify that AcceptThread is about to exit.
  bc_.Dec();
  // Here accessing wrapper might be unsafe.
}

auto AcceptServer::AcceptConnection(ListenerWrapper* wrapper) -> AcceptResult {
  IoContext& io_cntx = pool_->GetNextContext();

  system::error_code ec;
  tcp::socket sock(io_cntx.raw_context());

  wrapper->acceptor.async_accept(sock, fibers_ext::yield[ec]);
  if (!ec && !sock.is_open())
    ec = asio::error::try_again;
  if (ec)
    return AcceptResult(nullptr, ec);
  DCHECK(sock.is_open()) << sock.native_handle();
  VLOG(1) << "Accepted socket " << sock.remote_endpoint() << "/" << sock.native_handle();

  ConnectionHandler* conn = wrapper->listener->NewConnection(io_cntx);
  conn->Init(std::move(sock));

  return AcceptResult(conn, ec);
}

void AcceptServer::Run() {
  CHECK(!listeners_.empty());

  bc_.Add(listeners_.size());

  for (auto& listener : listeners_) {
    ListenerWrapper* ptr = &listener;
    io_context& io_cntx = ptr->acceptor.get_executor().context();

    asio::post(io_cntx, [this, ptr] {
      fibers::fiber srv_fb(&AcceptServer::RunInIOThread, this, ptr);
      srv_fb.detach();
    });
  }
  was_run_ = true;
}

void AcceptServer::Wait() {
  if (was_run_) {
    bc_.Wait();
    VLOG(1) << "AcceptServer::Wait completed";
  } else {
    CHECK(listeners_.empty()) << "Must Call AcceptServer::Run() after adding listeners";
  }
}

}  // namespace util
