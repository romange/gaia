// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"

namespace util {
namespace rpc {

template <typename Stream> class BufferedReadAdaptor {
 public:
  using error_code = ::boost::system::error_code;
  using mutable_buffer = ::boost::asio::mutable_buffer;

  BufferedReadAdaptor(Stream& s, size_t buf_size)
      : sock_(s), buf_(buf_size), mbuf_(buf_.data(), 0) {}

  template <typename BufferSequence> error_code Read(const BufferSequence& bufs) {
    if (mbuf_.size() == 0) {
      // If the intermediate buffer is empty, just read and load directly into bufs.
      // Fill mbuf_ on the way if possible.
      return ReadAndLoad(bufs);
    }
    size_t total_size = buffer_size(bufs);

    // Copy from mbuf_ into bufs.
    size_t sz = buffer_copy(bufs, mbuf_);
    saved_ += sz;
    if (sz == total_size) {  // If it's enough, update the remaining size and exit.
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
    using namespace ::boost;
    error_code ec;
    // assert: buf_ does not hold any useful data.

    // new_seq combines the required bufs with the desired buf_ so that we would
    // fill in one system call as much as possible.
    auto new_seq = make_buffer_seq(bufs, mutable_buffer(buf_.data(), buf_.size()));
    auto strip_seq = StripSequence(0, &new_seq);

    // First - try non-blocking call to check if there is data pending to read.
    size_t read = sock_.read_some(new_seq, ec);

    if (ec && ec != system::errc::resource_unavailable_try_again) {
      return ec;
    }
    ec.clear();

    while (!ec) {
      if (read >= total_size) {
        // Store the overflow in buf_.
        mbuf_ = mutable_buffer(buf_.data(), read - total_size);
        return error_code{};
      }

      strip_seq = StripSequence(read, &strip_seq);
      total_size -= read;

      // Continue with the asynchronous fiber-blocking call.
      read = sock_.async_read_some(strip_seq, fibers_ext::yield[ec]);
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
