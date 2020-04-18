// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/detail/buffer_sequence_adapter.hpp>
#include <boost/system/error_code.hpp>

namespace util {

class SyncStreamInterface {
public:
  virtual ~SyncStreamInterface() {}
  virtual size_t Send(const iovec* ptr, size_t len, std::error_code* ec) = 0;
  virtual size_t Recv(iovec* ptr, size_t len, std::error_code* ec) = 0;
};

template <typename Socket> class AsioStreamAdapter {
  Socket& s_;

 public:
  using error_code = ::boost::system::error_code;

  explicit AsioStreamAdapter(Socket& s) : s_(s) {
  }

  // Read/Write functions should be called from IoContext thread.
  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec);

  // To calm SyncReadStream compile-checker we provide exception-enabled
  // interface without implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  // SyncWrite interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec);

  // To calm SyncWriteStream compile-checker we provide exception-enabled
  // interface without implementing it.
  template <typename BS> size_t write_some(const BS& bufs);
};

template <typename Socket>
template <typename MBS>
size_t AsioStreamAdapter<Socket>::read_some(const MBS& bufs, error_code& ec) {
  using badapter = ::boost::asio::detail::buffer_sequence_adapter<
      boost::asio::mutable_buffer, const MBS&>;
  badapter bsa(bufs);

  std::error_code lec;
  auto res = s_.Recv(bsa.buffers(), bsa.count(), &lec);
  ec = error_code(lec.value(), boost::system::system_category());

  return res;
}

template <typename Socket>
template <typename BS>
size_t AsioStreamAdapter<Socket>::write_some(const BS& bufs, error_code& ec) {
  using badapter =
      ::boost::asio::detail::buffer_sequence_adapter<boost::asio::const_buffer,
                                                     const BS&>;
  badapter bsa(bufs);

  std::error_code lec;
  auto res = s_.Send(bsa.buffers(), bsa.count(), &lec);
  ec = error_code(lec.value(), boost::system::system_category());

  return res;
}

}  // namespace util
