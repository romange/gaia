// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "util/asio/yield.h"

namespace util {

namespace detail {
using namespace boost;
using asio::is_const_buffer_sequence;
using asio::is_mutable_buffer_sequence;
using asio::ip::tcp;

class ClientChannelImpl {
  tcp::resolver resolver_;

  template <typename T>
  using is_cbuf_t = typename std::enable_if<is_const_buffer_sequence<T>::value>::type;

  template <typename T>
  using is_mbuf_t = typename std::enable_if<is_mutable_buffer_sequence<T>::value>::type;

  template <typename Func>
  using func_return_t = decltype(std::declval<Func>()(std::declval<tcp::socket&>()));

  template <typename T>
  using check_ec_t =
      typename std::enable_if<std::is_same<system::error_code, T>::value, system::error_code>::type;

  using mutex = boost::fibers::mutex;

 public:
  using ResolveResults =
      decltype(resolver_.async_resolve(tcp::v4(), {}, {}, fibers_ext::yield_t{}));

  template <typename Func>
  using socket_callable_t = check_ec_t<func_return_t<Func>>;

  ClientChannelImpl(asio::io_context& cntx, const std::string& hname, const std::string& s)
      : resolver_(cntx), hostname_(hname), service_(s), sock_(cntx, tcp::v4()),
      handle_(sock_.native_handle()) {
    sock_.non_blocking(true);
  }

  ~ClientChannelImpl();

  tcp::socket::native_handle_type handle() const {
    return handle_;
  }

  system::error_code status() const {
    return status_;
  }

  bool shutting_down() const {
    return shutting_down_;
  }

  system::error_code Connect(uint32_t ms);

  // Shuts down and waits synchronously until reconnect callback finishes.
  void Shutdown();

  system::error_code WaitForReadAvailable() {
    system::error_code ec;

    if (status_) {
      sock_.async_wait(tcp::socket::wait_read, fibers_ext::yield[ec]);
    }
    return ec;
  }

  template <typename BufferSequence>
  system::error_code Write(const BufferSequence& seq, is_cbuf_t<BufferSequence>* = 0) {
    std::lock_guard<mutex> guard(wmu_);
    if (status_)
      return status_;

    asio::async_write(sock_, seq, fibers_ext::yield[status_]);
    if (status_)
      HandleErrorStatus();
    return status_;
  }

  // 'f' must be callable on 'socket&'. Returns error_code, the function is guaranteed to call 'f'.
  template <typename Func> socket_callable_t<Func> Write(Func&& f) {
    std::lock_guard<mutex> guard(wmu_);
    status_ = f(sock_);
    if (status_)
      HandleErrorStatus();
    return status_;
  }

  template <typename BufferSequence>
  system::error_code Read(const BufferSequence& seq, is_mbuf_t<BufferSequence>* = 0) {
    std::lock_guard<mutex> guard(rmu_);
    if (status_)
      return status_;

    asio::async_read(sock_, seq, fibers_ext::yield[status_]);
    if (status_)
      HandleErrorStatus();
    return status_;
  }

  // The function is guaranteed to call 'f'.
  template <typename Func> socket_callable_t<Func> Read(Func&& f) {
    std::lock_guard<mutex> guard(rmu_);
    status_ = f(sock_);
    if (status_)
      HandleErrorStatus();
    return status_;
  }

  asio::io_context& get_io_context() {
    return sock_.get_io_context();
  }

 private:
  using time_point = std::chrono::steady_clock::time_point;

  using fiber_t = ::boost::fibers::fiber;

  ResolveResults Resolve(fibers_ext::yield_t&& token) {
    return resolver_.async_resolve(tcp::v4(), hostname_, service_, std::move(token));
  }

  void ReconnectAsync() {
    sock_.get_executor().context().post(
        [this] { fiber_t(&ClientChannelImpl::ReconnectFiber, this).detach(); });
  }

  void ResolveAndConnect(const time_point& until);
  void HandleErrorStatus();
  void ReconnectFiber();

  std::string hostname_, service_;
  tcp::socket sock_;
  system::error_code status_ = asio::error::not_connected;

  mutex wmu_, rmu_;
  mutex shd_mu_;
  bool shutting_down_{false}, reconnect_active_{false};

  ::boost::fibers::condition_variable shd_cnd_;
  tcp::socket::native_handle_type handle_;
};

}  // namespace detail

class ClientChannel {
 public:
  using error_code = boost::system::error_code;
  using socket_t = detail::tcp::socket;

  // since we allow moveable semantics we should support default c'tor as well.
  ClientChannel() {}

  // "service" - port to which to connect.
  ClientChannel(::boost::asio::io_context& cntx, const std::string& hostname,
                const std::string& service)
      : impl_(new detail::ClientChannelImpl(cntx, hostname, service)) {
  }

  ClientChannel(ClientChannel&&) noexcept = default;
  ~ClientChannel();

  ClientChannel& operator=(ClientChannel&&) noexcept = default;

  // Should be called at most once to trigger the connection process. Should not be called more
  // than once because ClientChannel handles reconnects by itself.
  error_code Connect(uint32_t ms) {
    return impl_->Connect(ms);
  }

  // impl_ might be null due to object move.
  // Blocks the calling fiber until impl_ is shut down.
  void Shutdown() {
    if (impl_) impl_->Shutdown();
  }

  error_code WaitForReadAvailable() {
    return impl_->WaitForReadAvailable();
  }

  // impl_ might be null due to object move.
  bool is_shut_down() const {
    return !impl_ || impl_->shutting_down();
  }

  template <typename Writeable>
  error_code Write(Writeable&& wr) {
    return impl_->Write(std::forward<Writeable>(wr));
  }

  template <typename Readable>
  error_code Read(Readable&& rd) {
    return impl_->Read(std::forward<Readable>(rd));
  }

  error_code status() const {
    return impl_->status();
  }

  socket_t::native_handle_type handle() const {
    return impl_->handle();
  }

  ::boost::asio::io_context& get_io_context() {
    return impl_->get_io_context();
  }
 private:
  // Factor out most fields into Impl struct to allow moveable semantics for the channel.
  std::unique_ptr<detail::ClientChannelImpl> impl_;
};

}  // namespace util
