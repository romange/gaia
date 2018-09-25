// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/fiber/future.hpp>

#include "util/rpc/client_base.h"

#include "base/logging.h"
#include "util/asio/asio_utils.h"
#include "util/rpc/frame_format.h"

namespace util {
namespace rpc {

using namespace boost;
using asio::ip::tcp;

namespace {

template<typename R> fibers::future<std::decay_t<R>> make_ready(R&& r) {
  fibers::promise<std::decay_t<R>> p;
  fibers::future<std::decay_t<R>> res = p.get_future();
  p.set_value(std::forward<R>(r));

  return res;
}

}  // namespace


ClientBase::~ClientBase() {
  Shutdown();

  VLOG(1) << "Before ReadFiberJoin";
  CHECK(read_fiber_.joinable());
  read_fiber_.join();
}

void ClientBase::Shutdown() {
  channel_.Shutdown();
}

auto ClientBase::Connect(uint32_t ms) -> error_code {
  CHECK(!read_fiber_.joinable());
  error_code ec = channel_.Connect(ms);
  read_fiber_ = ::boost::fibers::fiber(&ClientBase::ReadFiber, this);
  return ec;
}

auto ClientBase::Send(Envelope* envelope) -> future_code_t {
  CHECK(read_fiber_.joinable());

  // ----
  fibers::promise<error_code> p;
  fibers::future<error_code> res = p.get_future();
  if (channel_.is_shut_down()) {
    p.set_value(asio::error::shut_down);
    return res;
  }

  if (channel_.status()) {
    p.set_value(channel_.status());
    return res;
  }

  // This section must be atomic so that rpc ids will be sent in increasing order.
  auto cb = [this, envelope, p = std::move(p)] (tcp::socket& sock) mutable -> error_code {
    Frame frame(rpc_id_++, envelope->header.size(), envelope->letter.size());
    uint8_t buf[Frame::kMaxByteSize];
    size_t bsz = frame.Write(buf);


    calls_.emplace_back(frame.rpc_id, std::move(p), envelope);
    error_code ec;
    asio::async_write(sock, make_buffer_seq(asio::buffer(buf, bsz), envelope->header,
                      envelope->letter), fibers_ext::yield[ec]);
    return ec;
  };

  error_code ec = channel_.Write(std::move(cb));
  if (ec) {
    FlushPendingCalls(ec);
  }
  return res;
}

void ClientBase::ReadFiber() {
  VLOG(1) << "Start ReadFiber on socket " << channel_.handle();

  error_code ec;
  while (!channel_.is_shut_down()) {
    ec = channel_.WaitForReadAvailable();
    if (!ec) {
      ec = channel_.Read([this](tcp::socket& sock) {
        return ReadEnvelope(&sock);
      });
    }
    if (ec) {
      // Handle error state.
      FlushPendingCalls(ec);
    }
  }
  FlushPendingCalls(ec);
  VLOG(1) << "Finish ReadFiber on socket " << channel_.handle();
}

auto ClientBase::ReadEnvelope(ClientChannel::socket_t* sock) -> error_code {
  Frame f;
  error_code ec = f.Read(sock);
  if (ec) return ec;

  VLOG(2) << "Got rpc_id " << f.rpc_id << " from socket " << sock->native_handle();
  if (calls_.empty() || calls_.front().rpc_id != f.rpc_id) {
    LOG(WARNING) << "Unexpected id " << f.rpc_id;
    LOG_IF(WARNING, !calls_.empty()) << "Expecting " << calls_.front().rpc_id;

    base::PODArray<uint8_t> buf;
    buf.resize(f.header_size);
    asio::async_read(*sock, asio::buffer(buf), fibers_ext::yield[ec]);
    if (ec) return ec;
    buf.resize(f.letter_size);
    asio::async_read(*sock, asio::buffer(buf), fibers_ext::yield[ec]);
    return ec;
  }

  PendingCall call = std::move(calls_.front());
  calls_.pop_front();
  DCHECK_EQ(call.rpc_id, f.rpc_id);
  call.envelope->Resize(f.header_size, f.letter_size);
  asio::async_read(*sock, make_buffer_seq(call.envelope->header, call.envelope->letter),
                   fibers_ext::yield[ec]);

  if (ec) return ec;

  call.promise.set_value(error_code{});

  return error_code{};
}

void ClientBase::FlushPendingCalls(error_code ec) {
  for (auto& c : calls_) {
    c.promise.set_value(ec);
  }
  calls_.clear();
}

}  // namespace rpc
}  // namespace util
