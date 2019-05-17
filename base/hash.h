// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef BASE_HASH_H
#define BASE_HASH_H

#include <cstdint>
#include <string>

namespace base {

uint32_t MurmurHash3_x86_32(const uint8_t* data, uint32_t len, uint32_t seed);

inline uint32_t Murmur32(uint64_t val, uint32_t seed = 10) {
  return MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(&val), sizeof val, seed);
}

inline uint32_t Murmur32(const std::string& str, uint32_t seed = 10) {
  if (str.empty())
    return seed;
  return MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(str.data()), str.size(), seed);
}


uint64_t Fingerprint(const char* str, uint32_t len);

inline uint64_t Fingerprint(const std::string& str) {
  return Fingerprint(str.c_str(), str.size());
}

inline uint32_t Fingerprint32(const std::string& str) {
  uint64_t res = Fingerprint(str);
  return uint32_t(res >> 32) ^ uint32_t(res);
}

}  // namespace base

#endif  // BASE_HASH_H

