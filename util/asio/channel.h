// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio/basic_stream_socket.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace util {

class Channel {
 public:
  explicit Channel(boost::asio::io_context& cntx) : sock_(cntx, boost::asio::ip::tcp::v4()) {}
  boost::system::error_code Connect(const boost::asio::ip::tcp::endpoint& endpoint, uint32_t ms);

 private:
  boost::asio::ip::tcp::socket sock_;
  bool is_connected_ = false;
};

}  // namespace util
