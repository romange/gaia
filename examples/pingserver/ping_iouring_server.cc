// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "examples/pingserver/ping_command.h"

#include "util/stats/varz_stats.h"
#include "util/uring/accept_server.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/proactor.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/http_handler.h"
#include "util/asio_stream_adapter.h"


using namespace boost;
using namespace util;
using uring::FiberSocket;
using uring::Proactor;
using uring::SubmitEntry;

using IoResult = Proactor::IoResult;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");
DEFINE_uint32(queue_depth, 256, "");
DEFINE_bool(linked_sqe, false, "If true, then no-op events are linked to the next ones");

VarzQps ping_qps("ping-qps");

class PingConnection : public uring::Connection {
 public:
  PingConnection() {}

  void Handle(IoResult res, int32_t payload, Proactor* mgr);

  void StartPolling(int fd, Proactor* mgr);

 private:
  void HandleRequests() final;

  PingCommand cmd_;
};


void PingConnection::HandleRequests() {
  system::error_code ec;

  AsioStreamAdapter<FiberSocket> asa(socket_);
  while (true) {
    size_t res = asa.read_some(cmd_.read_buffer(), ec);
    if (FiberSocket::IsConnClosed(ec))
      break;

    CHECK(!ec) << ec << "/" << ec.message();
    VLOG(1) << "Read " << res << " bytes";

    if (cmd_.Decode(res)) {  // The flow has a bug in case of pipelined requests.
      ping_qps.Inc();
      asa.write_some(cmd_.reply(), ec);
      CHECK(!ec) << ec << "/" << ec.message();
    }
  }
  socket_.Shutdown(SHUT_RDWR);
}

class PingListener : public uring::ListenerInterface {
 public:
  virtual uring::Connection* NewConnection(Proactor* context) {
    return new PingConnection;
  }
};

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0);

  Proactor proactor{FLAGS_queue_depth};
  std::thread t1([&] { proactor.Run(); });

  uring::AcceptServer uring_acceptor(&proactor, false);
  uring_acceptor.AddListener(FLAGS_port, new PingListener);
  uring::HttpListener<> http;

  if (FLAGS_http_port >= 0) {
    uint16_t port = uring_acceptor.AddListener(FLAGS_http_port, &http);
    LOG(INFO) << "Started http server on port " << port;
  }

  uring_acceptor.Run();

  /*accept_server.TriggerOnBreakSignal([&] {
    uring_acceptor.Stop(true);
    proactor.Stop();
  });*/

  t1.join();
  uring_acceptor.Stop(true);
  proactor.Stop();


  // accept_server.Stop(true);

  return 0;
}
