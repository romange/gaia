// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "util/asio/yield.h"

namespace util {
class IoContext;

namespace detail {

class FiberSocketImpl {
 public:
  using error_code = ::boost::system::error_code;
  using next_layer_type = ::boost::asio::ip::tcp::socket;

  // C'tor can be called from any thread.
  FiberSocketImpl(next_layer_type&& sock, size_t rbuf_size);

  // Creates a client socket.
  FiberSocketImpl(const std::string& hname, const std::string& port,
                  IoContext* cntx, size_t rbuf_size);

  // FiberSocketImpl can not be moveable due to attached fiber.
  FiberSocketImpl(FiberSocketImpl&& other) = delete;

  ~FiberSocketImpl();


  // Waits for client socket to become connected. Can be called from any thread.
  // Please note that connection status might be stale if called from a foreigh thread.
  error_code ClientWaitToConnect(uint32_t ms);

  // Read/Write functions should be called from IoContext thread.
  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec);


  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec);

  next_layer_type::native_handle_type native_handle() { return sock_.native_handle(); }

  bool is_open() const { return is_open_; }

  // Closes the socket and shuts down its background processes if needed.
  // For client socket it's thread-safe but for non-client it should be called
  // from the socket thread.
  void Shutdown(error_code& ec);

  next_layer_type::endpoint_type remote_endpoint(error_code& ec) const {
    return sock_.remote_endpoint(ec);
  }

  error_code status() const { return status_;}

  // For debugging.
  next_layer_type& next_layer() { return sock_; }

  // For debugging/testing.
  IoContext& context();

  bool keep_alive() const { return keep_alive_;}
  void set_keep_alive(bool flag) { keep_alive_ = flag;}

 private:
  // Asynchronous function that make this socket a client socket and initiates client-flow
  // connection process. Should be called only once. Can be called from any thread.
  void InitiateConnection();

  void ClientWorker();

  void WakeWorker();
  error_code Reconnect(const std::string& hname, const std::string& service);
  void SetStatus(const error_code& ec, const char* where);

  error_code status_;

  // socket.is_open() is unreliable and does not reflect close() status even if is called
  // from the same thread.
  bool is_open_ = true, keep_alive_ = false;
  enum State { READ_IDLE, READ_ACTIVE } read_state_ = READ_IDLE;

  size_t rbuf_size_;
  next_layer_type sock_;
  std::unique_ptr<uint8_t[]> rbuf_;
  ::boost::asio::mutable_buffer rslice_;

  std::string hname_, port_;
  IoContext* io_cntx_ = nullptr;

  // Stuff related to client sockets.
  struct ClientData;
  std::unique_ptr<ClientData> clientsock_data_;
};

template <typename MBS> size_t FiberSocketImpl::read_some(const MBS& bufs, error_code& ec) {
  using namespace boost;
  if (rslice_.size()) {
    size_t copied = asio::buffer_copy(bufs, rslice_);
    if (rslice_.size() == copied) {
      rslice_ = asio::mutable_buffer(rbuf_.get(), 0);
    } else {
      rslice_ += copied;
    }
    if (clientsock_data_) {
      WakeWorker();  // For client socket case.
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
    read_state_ = READ_ACTIVE;
    read_size = sock_.async_read_some(new_seq, fibers_ext::yield[ec]);
    read_state_ = READ_IDLE;
  }
  if (ec) {
    SetStatus(ec, "read_some");
  }
  if (clientsock_data_) {
    WakeWorker();  // For client socket case.
  }
  if (read_size > user_size) {
    rslice_ = asio::mutable_buffer(rbuf_.get(), read_size - user_size);
    read_size = user_size;
  }
  return read_size;
}

template <typename BS> size_t FiberSocketImpl::write_some(const BS& bufs, error_code& ec) {
  size_t res = sock_.write_some(bufs, ec);
  if (ec == ::boost::asio::error::would_block) {
    return sock_.async_write_some(bufs, fibers_ext::yield[ec]);
  }
  return res;
}

}  // namespace detail


}  // namespace util
