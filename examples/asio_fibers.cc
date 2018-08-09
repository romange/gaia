// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"
#include "strings/strcat.h"

#include <experimental/optional>
#include <boost/asio.hpp>
#include <boost/optional.hpp>
#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>  // for this_fiber.
#include <boost/fiber/scheduler.hpp>

#include "util/asio/io_context_pool.h"
#include "util/asio/connection.h"
#include "util/asio/yield.h"
#include "util/fibers_done.h"
#include "util/http/http_server.h"
#include "util/http/varz_stats.h"

using namespace boost;
using namespace std;

DEFINE_int32(http_port, 8080, "Port number.");
DEFINE_string(connect, "", "");
DEFINE_int32(count, 10, "");
DEFINE_int32(num_connections, 1, "");

using asio::ip::tcp;
using util::IoContextPool;

class AcceptServer {
 public:
  AcceptServer(unsigned short port, IoContextPool* pool);

  void Run();
  void Wait();

 private:
  void AcceptFiber();

  IoContextPool* pool_;
  asio::io_context& io_cntx_;
  tcp::acceptor acceptor_;
  asio::signal_set signals_;
  util::fibers::Done done_;
};


AcceptServer::AcceptServer(unsigned short port, IoContextPool* pool)
     :pool_(pool), io_cntx_(pool->GetNextContext()),
      acceptor_(io_cntx_, tcp::endpoint(tcp::v4(), port)),
      signals_(io_cntx_, SIGINT, SIGTERM) {
  signals_.async_wait(
  [this](system::error_code /*ec*/, int /*signo*/) {
    // The server is stopped by cancelling all outstanding asynchronous
    // operations. Once all operations have finished the io_context::run()
    // call will exit.
    acceptor_.close();
  });
}

void AcceptServer::AcceptFiber() {
  util::ConnectionList clist;
  DCHECK(clist.empty());
  fibers::condition_variable empty_list;
  using util::Connection;

  try {
    for (;;) {
        auto& io_cntx = pool_->GetNextContext();
        std::unique_ptr<Connection> conn(new Connection(&io_cntx, &empty_list));
        system::error_code ec;
        acceptor_.async_accept(conn->socket(), util::fibers::yield[ec]);

        if (ec) {
          throw system::system_error(ec); //some other error
        } else {
          clist.push_back(*conn);
          DCHECK(!clist.empty());

          DCHECK(conn->hook_.is_linked());
          conn.release()->Run();
        }
    }
  } catch (std::exception const& ex) {
    LOG(WARNING) << ": caught exception : " << ex.what();
  }

  LOG(INFO) << "Cleaning " << clist.size() << " connections";

  for (auto it = clist.begin(); it != clist.end(); ++it) {
    it->socket().close();
  }

  LOG(INFO) << "Waiting for connections to close";

  fibers::mutex mtx;
  std::unique_lock<fibers::mutex> lk(mtx);
  empty_list.wait(lk, [&clist] { return clist.empty(); });

  done_.notify();

  LOG(INFO) << ": echo-server stopped";
}

void AcceptServer::Run() {
  asio::post(io_cntx_, [this] {
    fibers::fiber srv_fb(&AcceptServer::AcceptFiber, this);
    srv_fb.detach();
  }
  );
}

void AcceptServer::Wait() {
  done_.wait();
}

/*****************************************************************************
*   fiber function per client
*****************************************************************************/
void client(boost::asio::io_context& io_svc,
            unsigned iterations, unsigned msg_count, util::fibers::Done* done) {
  LOG(INFO) << ": echo-client started";
  constexpr unsigned max_length = 1024;
  for (unsigned count = 0; count < iterations; ++count) {
    tcp::resolver resolver(io_svc);
    tcp::resolver::query query(tcp::v4(), FLAGS_connect, "9999");
    tcp::resolver::iterator iterator = resolver.resolve(query);
    tcp::socket s(io_svc);
    boost::asio::connect(s, iterator);
    char msgbuf[512];
    char reply[max_length];

    for (unsigned msg = 0; msg < msg_count; ++msg) {
        char* next = StrAppend(msgbuf, sizeof(msgbuf), {count, ".", msg});

        VLOG(1) << ": Sending: " << msgbuf;

        boost::system::error_code ec;
        asio::async_write(
                s, asio::buffer(msgbuf, next - msgbuf),
                util::fibers::yield[ec]);
        if ( ec == asio::error::eof) {
          return; //connection closed cleanly by peer
        } else if ( ec) {
          throw system::system_error( ec); //some other error
        }

        size_t reply_length = s.async_read_some(
                boost::asio::buffer(reply, max_length), util::fibers::yield[ec]);
        if ( ec == boost::asio::error::eof) {
          return; //connection closed cleanly by peer
        } else if ( ec) {
          throw boost::system::system_error( ec); //some other error
        }
        VLOG(1) << "Reply: " << std::string(reply, reply_length) << " :" << reply_length;
      }
  }

  done->notify();
  LOG(INFO) << ": echo-client stopped";
}

void client_pool(IoContextPool* pool) {
  const uint32 kNumClients = FLAGS_num_connections;

  vector<util::fibers::Done> done_arr(kNumClients);
  for (unsigned i = 0; i < kNumClients; ++i) {
    auto& cntx = pool->GetNextContext();
    cntx.post([&cntx, done = &done_arr[i]] {
      fibers::fiber(client, std::ref(cntx), 1, FLAGS_count, done).detach();
    });
  }
  for (auto& f : done_arr)
    f.wait();
}



int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  std::unique_ptr<http::Server> http_server;
  std::unique_ptr<AcceptServer> accept_server;

  IoContextPool pool(4);
  pool.Run();

  if (FLAGS_connect.empty()) {
    http_server.reset(new http::Server(FLAGS_http_port));
    util::Status status = http_server->Start();
    CHECK(status.ok()) << status;
    accept_server.reset(new AcceptServer(4999, &pool));
    accept_server->Run();

    accept_server->Wait();
    accept_server.reset();
  } else {
    client_pool(&pool);
  }

  pool.Stop();
  pool.Join();

  return 0;
}
