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

unsigned Frame::WriteNoHeader(uint8* dest) const {
  const uint8 msg_bytes_minus1 = SizeByteCountMinus1(msg_size);
  const uint8 cntrl_bytes_minus1 = SizeByteCountMinus1(control_size);

  DCHECK_LT(msg_bytes_minus1, 4);
  DCHECK_LT(cntrl_bytes_minus1, 4);

  uint8 version = cntrl_bytes_minus1 | (msg_bytes_minus1 << 2) | (0 << 4);
  *dest++ = version;

  LittleEndian::Store64(dest, rpc_id); dest += 7;
  LittleEndian::Store32(dest, control_size); dest += cntrl_bytes_minus1 + 1;
  LittleEndian::Store32(dest, msg_size);

  return 1 /* version */ + 7 /* rpc_id */ + cntrl_bytes_minus1 + msg_bytes_minus1 + 2;
}


inline constexpr uint32 byte_mask(uint8 n) { return (1UL << (n + 1) * 8) - 1;}

Status Frame::Read(socket_t* input) {
  uint8 buf[kMinByteSize + 4 + 4 + /* a little extra */ 8];

  system::error_code ec;
  size_t read = asio::async_read(*input, asio::buffer(buf), asio::transfer_exactly(kMinByteSize),
                                 yield[ec]);
  if (ec) {
    goto fail;
  }
  DCHECK_EQ(kMinByteSize, read);
  if (kHeaderVal !=  LittleEndian::Load32(buf)) {
    return Status("Bad header");
  }
  if (buf[4] >> 4 != 0) { // version check
    return Status("Unsupported version");
  }

  rpc_id = UNALIGNED_LOAD64(buf + 4);
  rpc_id >>= 8;
  {
    const uint8 sz_len = buf[4] & 15;
    const uint8 control_sz_len_minus1 = sz_len & 3;
    const uint8 msg_sz_len_minus1 = sz_len >> 2;

    // We stored 2 necessary bytes of boths lens, if it was not enough lets fill em up.
    if (sz_len) {
      asio::async_read(*input, asio::buffer(buf + kMinByteSize, kMaxByteSize),
                       asio::transfer_exactly(control_sz_len_minus1 + msg_sz_len_minus1),
                       yield[ec]);
      if (ec) {
        goto fail;
      }
    }

    VLOG(2) << "Frame::Read " << kMinByteSize + control_sz_len_minus1 + msg_sz_len_minus1;

    control_size = LittleEndian::Load32(buf + 12) & byte_mask(control_sz_len_minus1);
    msg_size = LittleEndian::Load32(buf + 12 + control_sz_len_minus1 + 1) &
        byte_mask(msg_sz_len_minus1);

  return Status::OK;
  }
fail:
  return Status(StatusCode::IO_ERROR, ec.message());
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
