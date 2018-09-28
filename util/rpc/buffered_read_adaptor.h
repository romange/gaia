// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "util/asio/asio_utils.h"

namespace util {
namespace rpc {

template<typename Stream> class BufferedReadAdaptor {
 public:
  using error_code = ::boost::system::error_code;
  using mutable_buffer = ::boost::asio::mutable_buffer;

  BufferedReadAdaptor(Stream& s, size_t buf_size) : sock_(s), buf_(buf_size),
      mbuf_(buf_.data(), 0) {}

  template <typename BufferSequence> error_code Read(const BufferSequence& bufs) {
    if (mbuf_.size() == 0) {
      return ReadAndLoad(bufs);
    }
    using namespace boost::asio;

    size_t total_size = buffer_size(bufs);

    // Copy from mbuf_ into bufs.
    size_t sz = buffer_copy(bufs, mbuf_);
    saved_ += sz;
    if (sz == total_size) {
      mbuf_ += sz;
      return error_code{};
    }

    auto local_bufs = bufs;
    assert(sz == mbuf_.size());
    auto strip_seq = StripSequence(sz, &local_bufs);
    return ReadAndLoad(strip_seq, total_size - sz);
  }

  size_t saved() const { return saved_; }

 private:
  template <typename BufferSequence>
    error_code ReadAndLoad(const BufferSequence& bufs, size_t total_size) {
    error_code ec;
    auto new_seq = make_buffer_seq(bufs, mutable_buffer(buf_.data(), buf_.size()));
    auto strip_seq = StripSequence(0, &new_seq);

    size_t read = sock_.read_some(new_seq, ec);

    while (true) {
      if (read >= total_size) {
        mbuf_ = mutable_buffer(buf_.data(), read - total_size);
        return error_code{};
      }

      strip_seq = StripSequence(read, &strip_seq);
      total_size -= read;

      read = sock_.async_read_some(strip_seq, fibers_ext::yield[ec]);
      if (ec)
        return ec;
    }
    return ec;
  }

  template <typename MutableBufferSequence>
      error_code ReadAndLoad(const MutableBufferSequence& bufs) {
      return ReadAndLoad(bufs, ::boost::asio::buffer_size(bufs));
  }

  Stream& sock_;
  std::vector<uint8_t> buf_;
  mutable_buffer mbuf_;
  size_t saved_ = 0;
};


}  // namespace rpc
}  // namespace util
