// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>

#include "base/integral_types.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"
#include "util/rpc/buffered_read_adaptor.h"

namespace util {
class FiberSyncSocket;

namespace rpc {

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

// Also defined in rpc_connection.h. Seems to work.
typedef uint64_t RpcId;

class Frame {
  static const uint32 kHeaderVal;

 public:
  typedef ::boost::asio::ip::tcp::socket socket_t;

  RpcId rpc_id;
  uint32_t header_size;
  uint32_t letter_size;

  Frame() : rpc_id(1), header_size(0), letter_size(0) {}
  Frame(RpcId r, uint32_t cs, uint32_t ms) : rpc_id(r), header_size(cs), letter_size(ms) {}

  enum { kMinByteSize = 4 + 1 + 7 + 2, kMaxByteSize = 4 + 1 + 7 + 4 * 2 };

  bool operator==(const Frame& other) const {
    return other.rpc_id == rpc_id && other.header_size == header_size &&
           other.letter_size == letter_size;
  }

  // friend std::ostream& operator<<(std::ostream& o, const Frame& frame);

  uint32 total_size() const { return header_size + letter_size; }

  // dest must be at least kMaxByteSize size.
  // Returns the exact number of bytes written to the buffer (less or equal to kMaxByteSize).
  unsigned Write(uint8* dest) const;

  template <typename SyncReadStream>::boost::system::error_code Read(SyncReadStream* input) {
    static_assert(!std::is_same<SyncReadStream, socket_t>::value, "");

    uint8 buf[kMaxByteSize + /* a little extra */ 8];
    using namespace boost;

    system::error_code ec;
    size_t read = asio::read(*input, asio::buffer(buf, kMinByteSize), ec);
    if (ec)
      return ec;
    uint8_t code = DecodeStart(buf, ec);
    if (ec)
      return ec;

    const uint8 header_sz_len_minus1 = code & 3;
    const uint8 msg_sz_len_minus1 = code >> 2;

    // We stored 2 necessary bytes of boths lens, if it was not enough lets fill em up.
    if (code) {
      size_t to_read = header_sz_len_minus1 + msg_sz_len_minus1;
      auto mbuf = asio::buffer(buf + kMinByteSize, to_read);
      read = asio::read(*input, mbuf, ec);
      if (ec)
        return ec;
    }

    DecodeEnd(buf + 12, header_sz_len_minus1, msg_sz_len_minus1);

    return ec;
  }

  ::boost::system::error_code Read(socket_t* input);
  ::boost::system::error_code Read(BufferedReadAdaptor<socket_t>* input);

 private:
  uint8_t DecodeStart(const uint8_t* src, ::boost::system::error_code& ec);
  void DecodeEnd(const uint8_t* src, uint8_t hsz_len, uint8_t lsz_len);
};

}  // namespace rpc
}  // namespace util
