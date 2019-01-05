// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/ip/tcp.hpp>

#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"

namespace util {

class FiberSyncSocket {
 public:
  using error_code = ::boost::system::error_code;
  using socket_t = ::boost::asio::ip::tcp::socket;

  // C'tor can be called from any thread.
  FiberSyncSocket(socket_t&& sock, size_t rbuf_size = 1 << 12)
      : rbuf_size_(rbuf_size), sock_(std::move(sock)), rbuf_(new uint8_t[rbuf_size]) {}

  FiberSyncSocket(::boost::asio::io_context* cntx, size_t rbuf_size = 1 << 12)
      : FiberSyncSocket(socket_t(*cntx, ::boost::asio::ip::tcp::v4()), rbuf_size) {}

  ~FiberSyncSocket() {}

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

  auto handle() { return sock_.native_handle(); }

  bool is_open() const { return sock_.is_open(); }

  void Shutdown(error_code& ec) {
    sock_.cancel(ec);
    sock_.shutdown(socket_t::shutdown_both, ec);
  }

  socket_t::endpoint_type remote_endpoint(error_code& ec) const {
    return sock_.remote_endpoint(ec);
  }

 protected:
  template <typename MBS> size_t FetchFromRBuf(const MBS& bufs) {
    using namespace ::boost;
    if (rslice_.size() == 0)
      return 0;
    size_t copied = asio::buffer_copy(bufs, rslice_);
    if (rslice_.size() == copied) {
      rslice_ = asio::mutable_buffer(rbuf_.get(), 0);
    } else {
      rslice_ += copied;
    }
    return copied;
  }

  error_code status_;

  size_t rbuf_size_;
  socket_t sock_;
  std::unique_ptr<uint8_t[]> rbuf_;
  ::boost::asio::mutable_buffer rslice_;

  enum State { IDLE, READ_CALL_ACTIVE } state_ = IDLE;
};

template <typename MBS> size_t FiberSyncSocket::read_some(const MBS& bufs, error_code& ec) {
  using namespace boost;
  size_t copied = FetchFromRBuf(bufs);
  if (copied)
    return copied;

  size_t user_size = asio::buffer_size(bufs);
  auto new_seq = make_buffer_seq(bufs, asio::mutable_buffer(rbuf_.get(), rbuf_size_));
  size_t read_size = sock_.read_some(new_seq, ec);
  if (ec == system::errc::resource_unavailable_try_again) {
    read_size = sock_.async_read_some(new_seq, fibers_ext::yield[ec]);
  }
  if (read_size > user_size) {
    rslice_ = asio::mutable_buffer(rbuf_.get(), read_size - user_size);
    read_size = user_size;
  }
  return read_size;
}

template <typename BS> size_t FiberSyncSocket::write_some(const BS& bufs, error_code& ec) {
  return sock_.async_write_some(bufs, fibers_ext::yield[ec]);
}

}  // namespace util
