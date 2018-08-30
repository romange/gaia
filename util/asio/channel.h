// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "util/asio/yield.h"

namespace util {

namespace basio = ::boost::asio;

class ClientChannel {
 public:
  using io_context = basio::io_context;
  using socket_t = basio::ip::tcp::socket;
  using mutex = boost::fibers::mutex;
  using error_code = boost::system::error_code;
  using time_point = std::chrono::steady_clock::time_point;

  ClientChannel(io_context& cntx, const std::string& hostname, const std::string& service)
    : sock_(cntx, basio::ip::tcp::v4()), impl_(new Impl(cntx, hostname, service)) {
  }

  ClientChannel(const ClientChannel&) = delete;
  ClientChannel(ClientChannel&&) = default;

  ~ClientChannel();

  void operator=(const ClientChannel&) = delete;
  ClientChannel& operator=(ClientChannel&&) = default;

  error_code Connect(uint32_t ms);

  socket_t& socket() { return sock_; }

  template<typename BufferSequence> error_code Write(const BufferSequence& seq) {
    if (status_)
      return status_;
    std::lock_guard<mutex> guard(impl_->wmu_);
    basio::async_write(sock_, seq, fibers_ext::yield[status_]);
    if (status_) HandleErrorStatus();
    return status_;
  }

  template<typename BufferSequence> error_code Read(const BufferSequence& seq) {
    if (status_)
      return status_;
    std::lock_guard<mutex> guard(impl_->rmu_);
    basio::async_read(sock_, seq, fibers_ext::yield[status_]);
    if (status_) HandleErrorStatus();
    return status_;
  }

  template<typename Func> error_code Read(Func&& f) {
    if (status_)
      return status_;
    std::lock_guard<mutex> guard(impl_->rmu_);
    status_ = f(sock_);
    if (status_) HandleErrorStatus();
    return status_;
  }

  io_context& context() { return sock_.get_executor().context(); }
 private:
  void ResolveAndConnect(const time_point& until);
  void HandleErrorStatus();
  void ReconnectFiber();

  struct Impl {
    std::string hostname_, service_;
    basio::ip::tcp::resolver resolver_;
    mutex wmu_, rmu_;
    mutex shd_mu_;
    bool shutting_down_{false}, reconnect_active_{false};
    using ResolveResults = decltype(resolver_.async_resolve(basio::ip::tcp::v4(), {}, {},
                                                            fibers_ext::yield_t{}));

    ::boost::fibers::condition_variable shd_cnd_;

    Impl(io_context& cntx, const std::string& hostname, const std::string& service) :
      hostname_(hostname), service_(service), resolver_(cntx) {}

    ~Impl();

    ResolveResults Resolve(fibers_ext::yield_t&& token) {
      return resolver_.async_resolve(basio::ip::tcp::v4(), hostname_, service_, std::move(token));
    }

    bool UpdateDisconnect() {
      std::lock_guard<mutex> l(shd_mu_);
      reconnect_active_ = false;
      return shutting_down_;
    }
  };

  socket_t sock_;
  error_code status_ = basio::error::not_connected;
  std::unique_ptr<Impl> impl_;
};



}  // namespace util
