// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "examples/pingserver/ping_command.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"
#include "util/stats/varz_stats.h"
#include "util/uring/fiber_socket.h"
#include "util/uring/proactor.h"
#include "util/uring/uring_fiber_algo.h"

using namespace boost;
using namespace util;
using namespace uring;
using IoResult = Proactor::IoResult;

DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 6380, "Redis port");
DEFINE_uint32(queue_depth, 256, "");
DEFINE_bool(linked_sqe, false, "If true, then no-op events are linked to the next ones");

VarzQps ping_qps("ping-qps");

static bool has_fast_poll = false;

class RedisConnection : public std::enable_shared_from_this<RedisConnection> {
 public:
  RedisConnection() {
    for (unsigned i = 0; i < 2; ++i) {
      memset(msg_hdr_ + i, 0, sizeof(msghdr));
      msg_hdr_[i].msg_iovlen = 1;
    }
    msg_hdr_[0].msg_iov = &io_rvec_;
    msg_hdr_[1].msg_iov = &io_wvec_;
  }

  void Handle(IoResult res, int32_t payload, Proactor* mgr);

  void StartPolling(int fd, Proactor* mgr);

 private:
  SubmitEntry GetEntry(int32_t socket, Proactor* mgr) {
    auto ptr = shared_from_this();
    auto cb = [ptr](IoResult res, int32_t payload, Proactor* proactor) {
      ptr->Handle(res, payload, proactor);
    };
    return mgr->GetSubmitEntry(std::move(cb), socket);
  }

  enum State { WAIT_READ, READ, WRITE } state_ = WAIT_READ;
  void InitiateRead(Proactor* mgr);
  void InitiateWrite(Proactor* mgr);

  int fd_ = -1;
  PingCommand cmd_;
  struct iovec io_rvec_, io_wvec_;
  msghdr msg_hdr_[2];
};

void RedisConnection::StartPolling(int fd, Proactor* mgr) {
  fd_ = fd;
  if (has_fast_poll) {
    InitiateRead(mgr);
  } else {
    SubmitEntry sqe = GetEntry(fd, mgr);
    sqe.PrepPollAdd(fd, POLLIN);
    if (FLAGS_linked_sqe) {
      LOG(FATAL) << "TBD";
      // sqe->flags |= IOSQE_IO_LINK;
      // sqe->user_data = 0;
      InitiateRead(mgr);
    } else {
      state_ = WAIT_READ;
    }
  }
}

void RedisConnection::InitiateRead(Proactor* mgr) {
  SubmitEntry se = GetEntry(fd_, mgr);
  auto rb = cmd_.read_buffer();
  io_rvec_.iov_base = rb.data();
  io_rvec_.iov_len = rb.size();

  se.PrepRecvMsg(fd_, &msg_hdr_[0], 0);
  state_ = READ;
}

void RedisConnection::InitiateWrite(Proactor* mgr) {
  auto rb = cmd_.reply();
  io_wvec_.iov_base = const_cast<void*>(rb.data());
  io_wvec_.iov_len = rb.size();
  SubmitEntry se = GetEntry(fd_, mgr);
#if 0
  CHECK_EQ(rb.size(), sendmsg(fd_, msg_hdr_ + 1, 0));
  se.PrepPollAdd(fd_, POLLIN);
  state_ = WAIT_READ;
#else
  // On my tests io_uring_prep_sendmsg is much faster than io_uring_prep_writev and
  // subsequently sendmsg is faster than write.
  se.PrepSendMsg(fd_, msg_hdr_ + 1, 0);
  if (FLAGS_linked_sqe) {
    LOG(FATAL) << "TBD";
    /*sqe->flags |= IOSQE_IO_LINK;
    sqe->user_data = 0;

    event->AddPollin(mgr);
    state_ = WAIT_READ;*/
  } else {
    state_ = WRITE;
  }
#endif
}

void RedisConnection::Handle(IoResult res, int32_t payload, Proactor* proactor) {
  int socket = payload;
  DVLOG(1) << "RedisConnection::Handle [" << socket << "] state/res: " << state_ << "/" << res;

  switch (state_) {
    case WAIT_READ:
      CHECK_GT(res, 0) << strerror(-res) << socket;
      InitiateRead(proactor);
      break;
    case READ:
      if (res > 0) {
        if (cmd_.Decode(res)) {  // The flow has a bug in case of pipelined requests.
          DVLOG(1) << "Sending PONG to " << socket;
          ping_qps.Inc();
          InitiateWrite(proactor);
        } else {
          InitiateRead(proactor);
        }
      } else {          // res <= 0
        if (res < 0) {  // 0 means EOF (socket closed).
          char* str = strerror(-res);
          LOG(WARNING) << "Socket error " << -res << "/" << str;
        }

        // In any case we close the connection.
        VLOG(1) << "Closing client socket " << socket;
        shutdown(socket, SHUT_RDWR);  // To send FYN as gentlemen would do.
        close(socket);
      }
      break;
    case WRITE:
      CHECK_GT(res, 0);
      if (has_fast_poll) {
        InitiateRead(proactor);
      } else {
        SubmitEntry se = GetEntry(fd_, proactor);
        se.PrepPollAdd(fd_, POLLIN);
        state_ = WAIT_READ;
      }
      break;
  }
}

void ManageAcceptions(FiberSocket* fs, Proactor* proactor) {
  fibers::context* me = fibers::context::active();
  UringFiberProps* props = reinterpret_cast<UringFiberProps*>(me->get_properties());
  CHECK(props);
  props->set_name("Acceptions");

  while (true) {
    FiberSocket peer;
    std::error_code ec = fs->Accept(&peer);
    if (ec == std::errc::connection_aborted)
      break;
    if (ec) {
      LOG(FATAL) << "Error calling accept " << ec << "/" << ec.message();
    }
    VLOG(2) << "Accepted " << peer.native_handle();
    std::shared_ptr<RedisConnection> connection(new RedisConnection);
    connection->StartPolling(peer.native_handle(), proactor);
    peer.Detach();
  }
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0);

  IoContextPool pool{1};
  pool.Run();
  AcceptServer accept_server(&pool);
  http::Listener<> http_listener;

  if (FLAGS_http_port >= 0) {
    uint16_t port = accept_server.AddListener(FLAGS_http_port, &http_listener);
    LOG(INFO) << "Started http server on port " << port;
    accept_server.Run();
  }

  Proactor proactor{FLAGS_queue_depth};

  has_fast_poll = proactor.HasFastPoll();

  FiberSocket server_sock;
  auto ec = server_sock.Listen(FLAGS_port, 128);
  CHECK(!ec) << ec;

  accept_server.TriggerOnBreakSignal([&] {
    shutdown(server_sock.native_handle(), SHUT_RDWR);
    proactor.Stop();
  });

  std::thread t1([&] { proactor.Run(); });
  server_sock.set_proactor(&proactor);

  proactor.AsyncFiber(&ManageAcceptions, &server_sock, &proactor);
  t1.join();
  accept_server.Stop(true);

  return 0;
}
