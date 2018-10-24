// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"
#include "strings/strcat.h"

#include <boost/asio.hpp>

#include "util/asio/io_context_pool.h"
#include "util/asio/yield.h"
#include "util/http/http_conn_handler.h"
#include "util/rpc/client_base.h"
#include "util/rpc/rpc_connection.h"
#include "util/stats/varz_stats.h"

using namespace boost;
using namespace std;
using namespace util;

DEFINE_int32(http_port, 8080, "Port number.");
DEFINE_string(connect, "", "");
DEFINE_int32(count, 10, "");
DEFINE_int32(num_connections, 1, "");
DEFINE_int32(io_threads, 0, "");
DEFINE_uint32(deadline_msec, 10, "");

using asio::ip::tcp;
using rpc::ClientBase;
using util::IoContextPool;
using util::fibers_ext::yield;

VarzQps qps("echo-qps");

class PingBridge final : public rpc::ConnectionBridge {
 public:
  // header and letter are input/output parameters.
  // HandleEnvelope reads first the input and if everything is parsed fine, it sends
  // back another header, letter pair.
  void HandleEnvelope(uint64_t rpc_id, rpc::Envelope* input,
                      EnvelopeWriter writer) override {
    qps.Inc();
    VLOG(1) << "RpcId: " << rpc_id;
    writer(std::move(*input));
  }
};

class PingInterface final : public rpc::ServiceInterface {
 public:
  rpc::ConnectionBridge* CreateConnectionBridge() override {
    return new PingBridge{};
  }
};

std::atomic_ulong latency_usec(0);
std::atomic_ulong latency_count(0);
std::atomic_ulong timeouts(0);

void Driver(ClientBase* client, size_t index, unsigned msg_count) {
  rpc::Envelope envelope;
  envelope.letter.resize(64);

  char msgbuf[64];

  char* start = reinterpret_cast<char*>(envelope.letter.data());
  auto tp = chrono::steady_clock::now();
  for (unsigned msg = 0; msg < msg_count; ++msg) {
    char* next = StrAppend(start, envelope.letter.size(), index, ".", msg);
    envelope.letter.resize(next - start);

    VLOG(1) << ": Sending: " << msgbuf;

    system::error_code ec = client->SendSync(FLAGS_deadline_msec, &envelope);
    if (ec == asio::error::timed_out) {
      timeouts.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    auto last = chrono::steady_clock::now();
    chrono::microseconds usec = chrono::duration_cast<chrono::microseconds>(last - tp);

    latency_count.fetch_add(1, std::memory_order_relaxed);
    latency_usec.fetch_add(usec.count(), std::memory_order_relaxed);
    tp = last;

    if (ec == asio::error::eof || ec == asio::error::connection_reset) {
      break;  // connection closed by peer
    } else if (ec) {
      LOG(ERROR) << "Error: " << ec << " " << ec.message();
      break;
    }
  }
}

/*****************************************************************************
 *   fiber function per client
 *****************************************************************************/
void RunClient(util::IoContext& context, unsigned msg_count, util::fibers_ext::Done* done) {
  LOG(INFO) << ": echo-client started";
  {
    std::unique_ptr<ClientBase> client(new ClientBase(context, FLAGS_connect, "9999"));
    system::error_code ec = client->Connect(100);
    CHECK(!ec) << ec.message();

    std::vector<fibers::fiber> drivers(FLAGS_num_connections);
    for (size_t i = 0; i < drivers.size(); ++i) {
      drivers[i] = fibers::fiber(&Driver, client.get(), i, msg_count);
    }
    for (auto& f : drivers)
      f.join();

    client->Shutdown();
  }
  done->Notify();
  LOG(INFO) << ": echo-client stopped";
}


void client_pool(IoContextPool* pool) {
  vector<util::fibers_ext::Done> done_arr(pool->size());
  {
    for (unsigned i = 0; i < pool->size(); ++i) {
      util::IoContext& cntx = pool->at(i);
      cntx.Post([&cntx, done = &done_arr[i]] {
        fibers::fiber(RunClient, std::ref(cntx), FLAGS_count, done).detach();
      });
    }
    for (auto& f : done_arr)
      f.Wait();
  }
  LOG(INFO) << "client_pool ended";
  cout << "Average latency(ms) is " << double(latency_usec.load()) / (latency_count + 1) / 1000 << endl;
  cout << "Timeouts " << timeouts.load() << endl;
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);
  unsigned io_threads = FLAGS_io_threads;
  if (io_threads == 0)
    io_threads = thread::hardware_concurrency();
  LOG(INFO) << "Running with " << io_threads << " threads";

  IoContextPool pool(io_threads);
  pool.Run();

  if (FLAGS_connect.empty()) {
    std::unique_ptr<util::AcceptServer> server(new AcceptServer(&pool));
    util::http::Listener<> http_listener;
    uint16_t port = server->AddListener(FLAGS_http_port, &http_listener);
    LOG(INFO) << "Started http server on port " << port;


    PingInterface pi;
    server->AddListener(9999, &pi);

    server->Run();
    server->Wait();
  } else {
    client_pool(&pool);
    // server.reset();
  }

  pool.Stop();

  return 0;
}
