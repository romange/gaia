// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"

#include "strings/strcat.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"
#include "util/rpc/channel.h"
#include "util/rpc/rpc_connection.h"
#include "util/sentry/sentry.h"
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
using rpc::Channel;
using util::IoContextPool;
using util::fibers_ext::yield;

VarzQps qps("echo-qps");

class PingBridge final : public rpc::ConnectionBridge {
 public:
  void HandleEnvelope(uint64_t rpc_id, rpc::Envelope* input, EnvelopeWriter writer) override {
    qps.Inc();
    VLOG(1) << "RpcId: " << rpc_id;
    writer(std::move(*input));   // Just write back the envelope we received.
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

void Driver(Channel* channel, size_t index, unsigned msg_count) {
  rpc::Envelope envelope;
  envelope.letter.resize(64);

  char* start = reinterpret_cast<char*>(envelope.letter.data());
  auto tp = chrono::steady_clock::now();
  for (unsigned msg = 0; msg < msg_count; ++msg) {
    char* next = StrAppend(start, envelope.letter.size(), index, ".", msg);
    envelope.letter.resize(next - start);

    system::error_code ec = channel->SendSync(FLAGS_deadline_msec, &envelope);
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
void RunClient(util::IoContext& context, unsigned msg_count) {
  LOG(INFO) << ": echo-client started";
  {
    std::unique_ptr<Channel> client(new Channel(FLAGS_connect, "9999", &context));
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
  LOG(INFO) << ": echo-client stopped";
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);
  unsigned io_threads = FLAGS_io_threads;
  if (io_threads == 0)
    io_threads = thread::hardware_concurrency();
  LOG(INFO) << "Running with " << io_threads << " threads";

  IoContextPool pool(io_threads);
  pool.Run();
  util::EnableSentry(&pool.GetNextContext());

  LOG(ERROR) << "AsioFibers sentry test!";

  if (FLAGS_connect.empty()) {
    // Server-side flow.
    std::unique_ptr<util::AcceptServer> server(new AcceptServer(&pool));
    util::http::Listener<> http_listener;
    uint16_t port = server->AddListener(FLAGS_http_port, &http_listener);
    LOG(INFO) << "Started http server on port " << port;

    PingInterface pi;
    server->AddListener(9999, &pi);

    server->Run();
    server->Wait();
  } else {
    // Dispatches asynchronously RunClient on every pool-thread in a dedicated fiber and
    // wait for them to finish.
    pool.AwaitFiberOnAll([](util::IoContext& cntx) { RunClient(cntx, FLAGS_count); });

    LOG(INFO) << "ClientLoadTest ended";
    cout << "Average latency(ms) is " << double(latency_usec.load()) / (latency_count + 1) / 1000
         << endl;
    cout << "Timeouts " << timeouts.load() << endl;
  }

  pool.Stop();

  return 0;
}
