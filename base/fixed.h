// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef CODING_H
#define CODING_H

#include <string>
#include "base/endian.h"

namespace coding {

// const uint8 kMaxVarintBytes = 10;
// const uint8 kMaxVarint32Bytes = 5;
const uint8 kFixed32Bytes = 4;
const uint8 kFixed64Bytes = 8;

inline uint8* EncodeFixed32(uint32 value, uint8* buf) {
  LittleEndian::Store32(buf, value);
  return buf + kFixed32Bytes;
}

inline uint8* EncodeFixed64(uint64 value, uint8* buf) {
  LittleEndian::Store64(buf, value);
  return buf + kFixed64Bytes;
}

inline void AppendFixed32(uint32 value, std::string* dest) {
  uint8 buf[kFixed32Bytes];
  EncodeFixed32(value, buf);
  dest->append(reinterpret_cast<char*>(buf), kFixed32Bytes);
}

inline void AppendFixed64(uint64 value, std::string* dest) {
  uint8 buf[kFixed64Bytes];
  EncodeFixed64(value, buf);
  dest->append(reinterpret_cast<char*>(buf), kFixed64Bytes);
}

inline uint32 DecodeFixed32(const uint8* buf) {
  return LittleEndian::Load32(reinterpret_cast<const char*>(buf));
}

inline const uint8* DecodeFixed64(const uint8* buf, uint64* val) {
  *val = LittleEndian::Load64(reinterpret_cast<const char*>(buf));
  return buf + kFixed64Bytes;
}


}  // namespace coding

#endif  // CODING_H
