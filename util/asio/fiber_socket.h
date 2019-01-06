// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"

namespace util {

class IoContext;

class FiberSyncSocket {
 public:
  using error_code = ::boost::system::error_code;
  using socket_t = ::boost::asio::ip::tcp::socket;

  // C'tor can be called from any thread.
  FiberSyncSocket(socket_t&& sock, size_t rbuf_size = 1 << 12);

  // Creates a client socket.
  FiberSyncSocket(const std::string& hname, const std::string& port,
                  IoContext* cntx, size_t rbuf_size = 1 << 12);

  // FiberSyncSocket can not be moveable due to attached fiber.
  FiberSyncSocket(FiberSyncSocket&& other) = delete;

  ~FiberSyncSocket();


  // Waits for client socket to become connected. Can be called from any thread.
  // Please note that connection status might be stale if called from a foreigh thread.
  error_code ClientWaitToConnect(uint32_t ms);

  // Read/Write functions should be called from IoContext thread.
  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec);

  // To calm SyncReadStream compile-checker we provide exception-enabled interface without
  // implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec);

  // To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  // implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  auto native_handle() { return sock_.native_handle(); }

  bool is_open() const { return is_open_; }

  // Closes the socket and shuts down its background processes if needed.
  // For client socket it's thread-safe but for non-client it should be called
  // from the socket thread.
  void Shutdown(error_code& ec);

  socket_t::endpoint_type remote_endpoint(error_code& ec) const {
    return sock_.remote_endpoint(ec);
  }

  // For debugging.
  socket_t& next_layer() { return sock_; }

  // For debugging/testing.
  IoContext& context();

 private:
  // Asynchronous function that make this socket a client socket and initiates client-flow
  // connection process. Should be called only once. Can be called from any thread.
  void InitiateConnection(const std::string& hname, const std::string& port, IoContext* cntx);
  void Worker(const std::string& hname, const std::string& service);
  void WakeWorker();
  error_code Reconnect(const std::string& hname, const std::string& service);

  error_code status_;
  bool is_open_ = true;
  enum State { IDLE, READ_CALL_ACTIVE } state_ = IDLE;

  size_t rbuf_size_;
  socket_t sock_;
  std::unique_ptr<uint8_t[]> rbuf_;
  ::boost::asio::mutable_buffer rslice_;

  // Stuff related to client sockets.
  struct ClientData;
  std::unique_ptr<ClientData> clientsock_data_;
};

template <typename MBS> size_t FiberSyncSocket::read_some(const MBS& bufs, error_code& ec) {
  using namespace boost;
  if (rslice_.size()) {
    size_t copied = asio::buffer_copy(bufs, rslice_);
    if (rslice_.size() == copied) {
      rslice_ = asio::mutable_buffer(rbuf_.get(), 0);
    } else {
      rslice_ += copied;
    }
    if (clientsock_data_) {
      WakeWorker(); // For client socket case.
    }
    return copied;
  }

  if (status_) {
    ec = status_;
    return 0;
  }

  size_t user_size = asio::buffer_size(bufs);
  auto new_seq = make_buffer_seq(bufs, asio::mutable_buffer(rbuf_.get(), rbuf_size_));

  size_t read_size = sock_.read_some(new_seq, ec);
  if (ec == asio::error::would_block) {
    state_ = READ_CALL_ACTIVE;
    read_size = sock_.async_read_some(new_seq, fibers_ext::yield[ec]);
    state_ = IDLE;
  }
  status_ = ec;

  if (clientsock_data_) {
    WakeWorker(); // For client socket case.
  }
  if (read_size > user_size) {
    rslice_ = asio::mutable_buffer(rbuf_.get(), read_size - user_size);
    read_size = user_size;
  }
  return read_size;
}

template <typename BS> size_t FiberSyncSocket::write_some(const BS& bufs, error_code& ec) {
  size_t res = sock_.write_some(bufs, ec);
  if (ec == ::boost::asio::error::would_block) {
    return sock_.async_write_some(bufs, fibers_ext::yield[ec]);
  }
  return res;
}

}  // namespace util
