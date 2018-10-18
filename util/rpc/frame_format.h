// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <boost/asio/ip/tcp.hpp>

#include "base/integral_types.h"
#include "util/asio/asio_utils.h"
#include "util/asio/yield.h"
#include "util/rpc/buffered_read_adaptor.h"

namespace util {
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

  Frame() : rpc_id(1), header_size(0), letter_size(0) {
  }
  Frame(RpcId r, uint32_t cs, uint32_t ms) : rpc_id(r), header_size(cs), letter_size(ms) {
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
  ::boost::system::error_code Read(BufferedReadAdaptor<socket_t>* input);
};

}  // namespace rpc
}  // namespace util
