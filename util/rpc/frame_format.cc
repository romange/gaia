// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/rpc/frame_format.h"

#include <boost/asio/read.hpp>

#include "absl/strings/escaping.h"
#include "base/bits.h"
#include "base/endian.h"
#include "base/logging.h"
#include "util/asio/yield.h"

namespace util {
using fibers_ext::yield;

namespace rpc {
using namespace boost;
using namespace system;

namespace {

constexpr uint8 kHeader[] = "URPC";

inline uint8 SizeByteCountMinus1(uint32 size) {
  return size <= 255 ? 0 : Bits::FindMSBSetNonZero(size) / 8;
}

inline constexpr uint32 byte_mask(uint8 n) {
    return (1UL << (n + 1) * 8) - 1;
}

}  // namespace

std::ostream& operator<<(std::ostream& o, const Frame& frame) {
  o << "{ rpc_id " << frame.rpc_id << ", header_size: " << frame.header_size
    << ", letter_size: " << frame.letter_size << " }";
  return o;
}

const uint32 Frame::kHeaderVal = LittleEndian::Load32(kHeader);

uint8_t Frame::DecodeStart(const uint8_t* src, ::boost::system::error_code& ec) {
  if (kHeaderVal != LittleEndian::Load32(src)) {
    ec = errc::make_error_code(errc::illegal_byte_sequence);
    return 0;
  }

  if (src[4] >> 4 != 0) {  // version check
    ec = errc::make_error_code(errc::illegal_byte_sequence);
    return 0;
  }
  rpc_id = UNALIGNED_LOAD64(src + 4);
  rpc_id >>= 8;

  return src[4] & 15;
}

void Frame::DecodeEnd(const uint8_t* src, uint8_t hsz_len, uint8_t lsz_len) {
  header_size = LittleEndian::Load32(src) & byte_mask(hsz_len);
  letter_size = LittleEndian::Load32(src + hsz_len + 1) & byte_mask(lsz_len);
}

unsigned Frame::Write(uint8* dest) const {
  LittleEndian::Store32(dest, kHeaderVal);
  dest += 4;
  const uint8 msg_bytes_minus1 = SizeByteCountMinus1(letter_size);
  const uint8 cntrl_bytes_minus1 = SizeByteCountMinus1(header_size);

  DCHECK_LT(msg_bytes_minus1, 4);
  DCHECK_LT(cntrl_bytes_minus1, 4);

  uint64_t version = cntrl_bytes_minus1 | (msg_bytes_minus1 << 2) | (0 << 4);

  LittleEndian::Store64(dest, (rpc_id << 8) | version);
  dest += 8;

  LittleEndian::Store32(dest, header_size);
  dest += (cntrl_bytes_minus1 + 1);
  LittleEndian::Store32(dest, letter_size);

  return 4 + 1 /* version */ + 7 /* rpc_id */ + cntrl_bytes_minus1 + msg_bytes_minus1 + 2;
}

// TODO: to remove it.
::boost::system::error_code Frame::Read(BufferedReadAdaptor<socket_t>* input) {
  uint8 buf[kMaxByteSize + /* a little extra */ 8];

  error_code ec = input->Read(asio::mutable_buffer(buf, kMinByteSize));
  if (ec)
    return ec;


  if (kHeaderVal != LittleEndian::Load32(buf)) {
    return errc::make_error_code(errc::illegal_byte_sequence);
  }

  if (buf[4] >> 4 != 0) {  // version check
    return errc::make_error_code(errc::illegal_byte_sequence);
  }

  rpc_id = UNALIGNED_LOAD64(buf + 4);
  rpc_id >>= 8;

  const uint8 sz_len = buf[4] & 15;
  const uint8 header_sz_len_minus1 = sz_len & 3;
  const uint8 msg_sz_len_minus1 = sz_len >> 2;
  size_t to_read = header_sz_len_minus1 + msg_sz_len_minus1;

  // We stored 2 necessary bytes of boths lens, if it was not enough lets fill em up.
  if (sz_len) {
    auto mbuf = asio::mutable_buffer(buf + kMinByteSize, to_read);
    ec = input->Read(mbuf);
    if (ec)
      return ec;
  }

  VLOG(2) << "Frame::Read " << kMinByteSize + to_read;

  header_size = LittleEndian::Load32(buf + 12) & byte_mask(header_sz_len_minus1);
  letter_size =
      LittleEndian::Load32(buf + 12 + header_sz_len_minus1 + 1) & byte_mask(msg_sz_len_minus1);

  return ec;
}

}  // namespace rpc

}  // namespace util
