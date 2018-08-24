// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include "util/asio/yield.h"

namespace util {

namespace basio = ::boost::asio;

class Channel {
 public:
  using io_context = basio::io_context;
  using socket_t = basio::ip::tcp::socket;
  using mutex = boost::fibers::mutex;

  explicit Channel(io_context& cntx) : sock_(cntx, basio::ip::tcp::v4()) {}

  boost::system::error_code Connect(const basio::ip::tcp::endpoint& endpoint, uint32_t ms);

  socket_t& socket() { return sock_; }

  template<typename ConstBufferSequence> boost::system::error_code
    Write(const ConstBufferSequence& seq) {
    if (status_)
      return status_;
    std::lock_guard<mutex> guard(mu_);

    basio::async_write(sock_, seq, fibers_ext::yield[status_]);
    return status_;
  }

 private:
  socket_t sock_;
  boost::fibers::mutex mu_;
  ::boost::system::error_code status_ = basio::error::not_connected;
};

}  // namespace util
