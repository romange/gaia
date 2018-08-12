// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/rpc/frame_format.h"

#include <boost/asio/completion_condition.hpp>
#include <boost/asio/read.hpp>

#include "base/bits.h"
#include "base/endian.h"
#include "base/logging.h"
#include "absl/strings/escaping.h"
#include "util/asio/yield.h"

namespace util {
using fibers_ext::yield;

namespace rpc {
using util::Status;
using util::StatusCode;
using namespace boost;
using namespace system;

namespace gpb = ::google::protobuf;

namespace {

constexpr uint8 kHeader[] = "URPC";

inline uint8 SizeByteCountMinus1(uint32 size) {
  if (size > 255) {
    return Bits::FindMSBSetNonZero(size) / 8;
  }
  return 0;
}

}  // namespace

std::ostream& operator<<(std::ostream& o, const Frame& frame) {
  o << "{ rpc_id " << frame.rpc_id << ", control_size: " << frame.control_size
    << ", msg_size: " << frame.msg_size << " }";
  return o;
}

const uint32 Frame::kHeaderVal =  LittleEndian::Load32(kHeader);


inline constexpr uint32 byte_mask(uint8 n) { return (1UL << (n + 1) * 8) - 1;}

error_code Frame::Read(socket_t* input) {
  uint8 buf[kMaxByteSize + /* a little extra */ 8];

  error_code ec;
  size_t read = asio::async_read(*input, asio::buffer(buf), asio::transfer_exactly(kMinByteSize),
                                 yield[ec]);
  if (ec) {
    return ec;
  }

  DCHECK_EQ(kMinByteSize, read);
  if (kHeaderVal !=  LittleEndian::Load32(buf)) {
    return errc::make_error_code(errc::illegal_byte_sequence);
  }
  if (buf[4] >> 4 != 0) { // version check
    return errc::make_error_code(errc::illegal_byte_sequence);
  }

  rpc_id = UNALIGNED_LOAD64(buf + 4);
  rpc_id >>= 8;

  const uint8 sz_len = buf[4] & 15;
  const uint8 control_sz_len_minus1 = sz_len & 3;
  const uint8 msg_sz_len_minus1 = sz_len >> 2;

  // We stored 2 necessary bytes of boths lens, if it was not enough lets fill em up.
  if (sz_len) {
    asio::async_read(*input, asio::buffer(buf + kMinByteSize, kMaxByteSize - kMinByteSize),
                     asio::transfer_exactly(control_sz_len_minus1 + msg_sz_len_minus1),
                     yield[ec]);
    if (ec) {
      return ec;
    }
  }

  VLOG(2) << "Frame::Read " << kMinByteSize + control_sz_len_minus1 + msg_sz_len_minus1;

  control_size = LittleEndian::Load32(buf + 12) & byte_mask(control_sz_len_minus1);
  msg_size = LittleEndian::Load32(buf + 12 + control_sz_len_minus1 + 1) &
      byte_mask(msg_sz_len_minus1);

  return error_code{};
}

#if 0
Status ReadPacket(Frame::socket_t* input,
                  Frame* frame, std::string* control, std::string* msg) {
  Status st = frame->Read(input);
  if (!st.ok()) {
    VLOG(1) << "Bad frame header " << st;
    return st; // If we get EOF here it is not an error, that's why we just return st.
  }

  VLOG(1) << "ReadPacket. Frame: " << *frame;
  control->resize(frame->control_size);
  msg->resize(frame->msg_size);

  if (!(st = input->readAll((uint8*)&control->front(), frame->control_size)).ok() ||
      !(st = input->readAll((uint8*)&msg->front(), frame->msg_size)).ok()) {
    if (st.code() == StatusCode::IO_END_OF_FILE)
      return StatusCode::IO_ERROR; // EOF in the middle of the message is an error
    return st;
  }

  VLOG(2) << "Control read: " <<  absl
  ::CEscape(*control);
  VLOG(2) << "msg read: " <<  absl::CEscape(*msg);


  return Status::OK;
}
#endif

}  // namespace rpc

}  // namespace util
