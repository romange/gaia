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
#include "util/asio/error.h"

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
    ec = make_error_code(gaia_error::bad_header);
    return 0;
  }

  if (src[4] >> 4 != 0) {  // version check
    ec = make_error_code(gaia_error::invalid_version);
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

}  // namespace rpc

}  // namespace util
