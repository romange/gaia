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

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/asio/connection_handler.h"
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
DEFINE_int32(io_threads, 0, "");

using asio::ip::tcp;
using util::IoContextPool;
using util::fibers_ext::yield;
using util::ConnectionServerNotifier;

http::VarzQps qps("echo-qps");


class PingConnectionHandler : public util::ConnectionHandler {
 public:
  PingConnectionHandler(asio::io_context* io_svc, ConnectionServerNotifier* done);

  boost::system::error_code HandleRequest() final override;
};

PingConnectionHandler::PingConnectionHandler(
  asio::io_context* io_svc, ConnectionServerNotifier* notifier)
    : util::ConnectionHandler(io_svc, notifier) {
}

boost::system::error_code PingConnectionHandler::HandleRequest() {
  constexpr unsigned max_length = 1024;

  char data[max_length];
  system::error_code ec;

  std::size_t length = socket().async_read_some(asio::buffer(data), yield[ec]);

  if (ec) {
    return ec;
  }
  VLOG(1) << ": handled: " << std::string(data, length);
  qps.Inc();

  asio::async_write(socket(), asio::buffer(data, length), yield[ec]);
  return ec;
}


/*****************************************************************************
*   fiber function per client
*****************************************************************************/
void RunClient(boost::asio::io_context& io_svc,
            unsigned iterations, unsigned msg_count, util::fibers_ext::Done* done) {
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
                util::fibers_ext::yield[ec]);
        if ( ec == asio::error::eof) {
          return; //connection closed cleanly by peer
        } else if ( ec) {
          throw system::system_error( ec); //some other error
        }

        size_t reply_length = s.async_read_some(
                boost::asio::buffer(reply, max_length), yield[ec]);
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

  vector<util::fibers_ext::Done> done_arr(kNumClients);
  for (unsigned i = 0; i < kNumClients; ++i) {
    auto& cntx = pool->GetNextContext();
    cntx.post([&cntx, done = &done_arr[i]] {
      fibers::fiber(RunClient, std::ref(cntx), 1, FLAGS_count, done).detach();
    });
  }
  for (auto& f : done_arr)
    f.wait();
}



int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  std::unique_ptr<http::Server> http_server;
  std::unique_ptr<util::AcceptServer> accept_server;

  unsigned io_threads = FLAGS_io_threads;
  if (io_threads == 0)
    io_threads = thread::hardware_concurrency();
  LOG(INFO) << "Running with " << io_threads << " threads";

  IoContextPool pool(io_threads);
  pool.Run();

  if (FLAGS_connect.empty()) {
    http_server.reset(new http::Server(FLAGS_http_port));
    util::Status status = http_server->Start();
    CHECK(status.ok()) << status;
    auto cf = [](asio::io_context* cntx, ConnectionServerNotifier* done) {
      return new PingConnectionHandler(cntx, done);
    };
    accept_server.reset(new util::AcceptServer(9999, &pool, cf));
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
