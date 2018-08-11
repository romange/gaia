// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include "base/integral_types.h"
#include "util/status.h"

#include <boost/asio/ip/tcp.hpp>


namespace google {
namespace protobuf {
class MessageLite;
}  // namespace protobuf
}  // namespace google

namespace util {
namespace rpc {
/*
  Frame structure:
    header str ("URPC") - 4 bytes
    uint8 version + control size length + message size length 1 byte (4bits + 2bits + 2bits)
    uint56 rpc_id - LE56
    control_size - LE of control size length
    message size - LE on message size length
    BLOB char[control_size + message_size]:
      PB - control packet of size control_size
      PB - message request of size message_size
*/

class Frame {
  static const uint32 kHeaderVal;

  unsigned WriteNoHeader(uint8* dest) const;
  void LoadSizes(const uint8* input, uint8 cs, uint8 ms);
public:
  typedef ::boost::asio::ip::tcp::socket socket_t;

  uint64 rpc_id;
  uint32 control_size;
  uint32 msg_size;

  Frame() {}
  Frame(uint64 r, uint32 cs, uint32 ms) : rpc_id(r), control_size(cs), msg_size(ms) {}

  enum { kMinByteSize = 4 + 1 + 7 + 2, kMaxByteSize = 4 + 1 + 7 + 4*2 };

  util::Status Write(socket_t* outp) const;
  util::Status Read(socket_t* input);

  bool operator==(const Frame& other) const {
    return other.rpc_id == rpc_id && other.control_size == control_size &&
        other.msg_size == msg_size;
  }

  friend std::ostream& operator<<(std::ostream& o, const Frame& frame);

  uint32 total_size() const { return control_size + msg_size; }
};

util::Status ReadPacket(Frame::socket_t* input, Frame* frame,
                        std::string* control, std::string* msg);

}  // namespace rpc
}  // namespace util
