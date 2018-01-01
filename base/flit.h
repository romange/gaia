// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "base/macros.h"
#include "base/bits.h"
#include "base/port.h"


#if !defined(__amd64__) || !defined(IS_LITTLE_ENDIAN)
#error "Only for  AMD64 little endian architecture"
#endif

namespace base {

// From protobuf documentation:
// ZigZag Transform:  Encodes signed integers so that they can be
// effectively used with varint encoding.
//
// varint operates on unsigned integers, encoding smaller numbers into
// fewer bytes.  If you try to use it on a signed integer, it will treat
// this number as a very large unsigned integer, which means that even
// small signed numbers like -1 will take the maximum number of bytes
// (10) to encode.  ZigZagEncode() maps signed integers to unsigned
// in such a way that those with a small absolute value will have smaller
// encoded values, making them appropriate for encoding using varint.
//
//       int32 ->     uint32
// -------------------------
//           0 ->          0
//          -1 ->          1
//           1 ->          2
//          -2 ->          3
//         ... ->        ...
//  2147483647 -> 4294967294
// -2147483648 -> 4294967295
//


// For unsigned types ZigZag is just an identity.
template<typename T> constexpr typename std::enable_if_t<std::is_unsigned<T>::value, T>
  ZigZagEncode(T t) {

  return t;
}

// For signed types ZigZag transforms numbers into their unsigned type.
template<typename T> constexpr typename std::enable_if_t<std::is_signed<T>::value,
                                               typename std::make_unsigned_t<T>>
  ZigZagEncode(T t) {
  using UT = typename std::make_unsigned_t<T>;

  // puts MSB bit of input into LSB of the unsigned result.
  return (UT(t) << 1) ^ (t >> (sizeof(t) * 8 - 1));
}

static_assert(ZigZagEncode<int32_t>(1) == 2, "");
static_assert(ZigZagEncode<int32_t>(-2) == 3, "");   // (1110 << 1) ^ 1111

template<typename T> typename std::enable_if_t<std::is_unsigned<T>::value, T>
  ZigZagDecode(T t) {

  return t;
}

template<typename T> typename std::enable_if_t<std::is_signed<T>::value, T>
  ZigZagDecode(typename std::make_unsigned_t<T> t) {

  return (t >> 1) ^ -static_cast<T>(t & 1);
}



namespace flit {

// Decodes buf into v and returns number of bytes needed to encode the number.
// Requires that src points to buffer that has at least 8 bytes and
// it assumes that the input is valid.
// The generated code is verified with https://godbolt.org/
inline unsigned Parse64Fast(const uint8_t* src, uint64_t* v) {
  // It's better use local variables then reuse output argument - compiler can substitute them
  // with registers.
  uint64_t val;
  // compiler is smart enough to replace it with a single load instruction.
  std::memcpy(&val, src, sizeof(val));

  if ((val & 0xff) == 0) {
    ++src;
    std::memcpy(v, src, sizeof(*v));
    return 9;
  }

  uint32_t index = Bits::FindLSBSetNonZero(val);

#define USE_MASK 0

#if USE_MASK
  static constexpr uint64_t mask[8] = {
    0xff,
    0xffff,
    0xffffff,
    0xffffffff,
    0xffffffffff,
    0xffffffffffff,
    0xffffffffffffff,
    0xffffffffffffffff,
  };

  val &= mask[index];

  ++index;
#else
  ++index;

  val &= ((1ULL << index * 8) - 1);
#endif

  *v = val >> index;

  return index;
}

#undef USE_MASK

// Returns 0 if the input is invalid, number of parsed bytes otherwise.
inline unsigned Parse64Safe(const uint8_t* begin, const uint8_t* end, uint64_t* v) {
  size_t sz = end - begin;

  if (*begin == 0) {
    if (sz < 8)
      return 0;
    ++begin;
    std::memcpy(v, begin, sizeof(*v));

    if (*v < (1ULL << 56))
      return 0;
    return 9;
  }

  unsigned index = Bits::FindLSBSetNonZero(*begin) + 1;
  if (sz < index)
    return 0;

  // Smart compiler knows how to optimize memcpy to small cnt values.
  memcpy(v, begin, index & 7);
  *v &= ((1ULL << index * 8) - 1);
  *v >>= index;

  return index;
}

// Can override more bytes than v requires upto 8 bytes.
// Requires 9 bytes in dest in the worst case.
template<typename T> unsigned EncodeT(T v, uint8_t* dest) {
  static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                "Only uint32/64 supported");

  uint32_t index = base::FindMsbNotZero(v | 1);
  if (v >= (T(1) << 7*sizeof(v))) {
    *dest++ = (1 << sizeof(T)) & 0xFF;
    memcpy(dest, &v, sizeof(v));
    return sizeof(v) + 1;
  }

  // Division by 7.
  uint32_t num_bytes = (index * 2454267027) >> 34;

  v = ((v * 2) + 1) << num_bytes;

  // Possible override.
  memcpy(dest, &v, sizeof(v));

  return num_bytes + 1;
}

template<typename T> unsigned ParseLengthT(const uint8_t* src) {
  uint8_t val = *src;

  if ((val & 0xFF) == ((1 << sizeof(T)) & 0xFF))  {
    return sizeof(T) + 1;
  }
  return 1 + base::FindLsbNotZero<uint32_t>(val);
}

// "Safe version". Result is undefined on invalid input.
template<typename T> unsigned ParseT(const uint8_t* src, T* dest) {
  static_assert(std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
                "Only uint32/64 supported");
  T val;

  // possibly reads beyond required limit.
  std::memcpy(&val, src, sizeof(val));

  if ((val & 0xFF) == ((1 << sizeof(T)) & 0xFF))  {
    ++src;
    memcpy(dest, src, sizeof(T));
    return sizeof(T) + 1;
  }
  uint32_t index = base::FindLsbNotZero<uint32_t>(val);

  ++index;
  val &= ((1ULL << index * 8) - 1);

  *dest = val >> index;

  return index;
}

template<typename T> uint32_t Length(T v) {
  return 1 + base::FindMsbNotZero(v | 1) / 7;
}

// Encodes v into buf and returns pointer to the next byte.
// dest must have at least 9 bytes.
inline unsigned Encode64(uint64_t v, uint8_t* dest) {
  return EncodeT<uint64_t>(v, dest);
}

// dest must have at least 8 bytes.
inline unsigned Encode32(uint32_t v, uint8_t* dest) {
  return EncodeT<uint32_t>(v, dest);
}

template<typename T> struct Traits {
  static_assert(std::numeric_limits<T>::is_integer, "T must be integer");

  static constexpr size_t max_size = sizeof(T) + 1 + (sizeof(T) - 1) / 8;
};

}  // namespace flit
}  // namespace base
