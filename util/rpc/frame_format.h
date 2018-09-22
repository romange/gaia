// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <boost/asio/ip/tcp.hpp>

#include "base/integral_types.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"

namespace util {
namespace rpc {

template<typename Stream> class BufferedSocketReadAdaptor {
 public:
  using error_code = ::boost::system::error_code;
  using mutable_buffer = ::boost::asio::mutable_buffer;

  BufferedSocketReadAdaptor(Stream& s, size_t buf_size) : sock_(s), buf_(buf_size),
      mbuf_(buf_.data(), 0) {}

  template <typename MutableBufferSequence>
    error_code Read(const MutableBufferSequence& bufs) {
    if (mbuf_.size() == 0) {
      return ReadAndLoad(bufs);
    }
    using namespace boost::asio;

    size_t total_size = buffer_size(bufs);
    size_t sz = buffer_copy(bufs, mbuf_);
    if (sz == total_size) {
      mbuf_ += sz;
      return error_code{};
    }

    auto local_bufs = bufs;
    assert(sz == mbuf_.size());
    auto strip_seq = StripSequence(sz, local_bufs);
    return ReadAndLoad(strip_seq, total_size - sz);
  }

 private:
  template <typename MutableBufferSequence>
    error_code ReadAndLoad(const MutableBufferSequence& bufs, size_t total_size) {
    error_code ec;
    auto new_seq = make_buffer_seq(bufs, mutable_buffer(buf_.data(), buf_.size()));
    auto strip_seq = StripSequence(0, new_seq);

    size_t read = sock_.read_some(new_seq, ec);

    while (true) {
      if (read >= total_size) {
        mbuf_ = mutable_buffer(buf_.data(), read - total_size);
        return error_code{};
      }

      strip_seq = StripSequence(read, strip_seq);
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
};

/*
  Frame structure:
    header str ("URPC") - 4 bytes
    uint8 version + control size length + message size length 1 byte (4bits + 2bits + 2bits)
    uint56 rpc_id - LE56
    header_size - LE of control size length
    message size - LE on message size length
    BLOB char[header_size + message_size]:
      PB - control packet of size header_size
      PB - message request of size message_size
*/

class Frame {
  static const uint32 kHeaderVal;

 public:
  typedef ::boost::asio::ip::tcp::socket socket_t;

  uint64 rpc_id;
  uint32 header_size;
  uint32 letter_size;

  Frame() : rpc_id(1), header_size(0), letter_size(0) {
  }
  Frame(uint64 r, uint32 cs, uint32 ms) : rpc_id(r), header_size(cs), letter_size(ms) {
  }

  enum { kMinByteSize = 4 + 1 + 7 + 2, kMaxByteSize = 4 + 1 + 7 + 4 * 2 };

  bool operator==(const Frame& other) const {
    return other.rpc_id == rpc_id && other.header_size == header_size &&
           other.letter_size == letter_size;
  }

  // friend std::ostream& operator<<(std::ostream& o, const Frame& frame);

  uint32 total_size() const {
    return header_size + letter_size;
  }

  // dest must be at least kMaxByteSize size.
  // Returns the exact number of bytes written to the buffer (less or equal to kMaxByteSize).
  unsigned Write(uint8* dest) const;

  ::boost::system::error_code Read(socket_t* input);
  ::boost::system::error_code Read(BufferedSocketReadAdaptor<socket_t>* input);
};

}  // namespace rpc
}  // namespace util
