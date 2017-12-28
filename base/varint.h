// Copyright 2001 and onwards Google Inc.
//
// Raw support for varint encoding.  Higher level interfaces are
// provided by Encoder/Decoder/IOBuffer.  Clients should typically use
// those interfaces, unless speed is paramount.
//
// If decoding speed is very important, consider using PrefixVarint instead.
// It has the same compression ratio, but generally faster decoding.
//
// Provided routines:
//      vi_parse32_unchecked
//      vi_parse64_unchecked
//      vi_encode32_unchecked
//      vi_encode64_unchecked
//
// Modified by: Roman Gershman (romange@gmail.com)
#ifndef _CODING_VARINT_H
#define _CODING_VARINT_H

#include <cassert>
#include <string>
#include <type_traits>

#include "base/integral_types.h"

// Just a namespace, not a real class
class Varint {
 public:
  // Maximum lengths of varint encoding of uint32 and uint64
  static constexpr int kMax32 = 5;
  static constexpr int kMax64 = 10;

  template<typename T> static constexpr uint8 MaxSize() {
    static_assert(std::is_same<T, uint32>::value || std::is_same<T, uint64>::value ,
      "Must be uint64 or uint32");
    return std::is_same<T, uint32>::value ? kMax32 : kMax64;
  }

  static const uint8* Parse(const uint8* ptr, uint32* OUTPUT) { return Parse32(ptr, OUTPUT); }
  static const uint8* Parse(const uint8* ptr, uint64* OUTPUT) { return Parse64(ptr, OUTPUT); }
  template<typename T> static uint8* Encode(uint8* ptr, T v);

  // REQUIRES   "ptr" points to a buffer of length at least kMaxXX
  // EFFECTS    Scan next varint from "ptr" and store in OUTPUT.
  //            Returns pointer just past last read byte.  Returns
  //            NULL if a valid varint value was not found.
  static const uint8* Parse32(const uint8* ptr, uint32* OUTPUT);
  static const uint8* Parse64(const uint8* ptr, uint64* OUTPUT);

  // A fully inlined version of Parse32: useful in the most time critical
  // routines, but its code size is large
  static const uint8* Parse32Inline(const uint8* ptr, uint32* OUTPUT);

  // REQUIRES   "ptr" points just past the last byte of a varint-encoded value.
  // REQUIRES   A second varint must be encoded just before the one we parse,
  //            OR "base" must point to the first byte of the one we parse.
  // REQUIRES   Bytes [base, ptr-1] are readable
  //
  // EFFECTS    Scan backwards from "ptr" and store in OUTPUT. Stop at the last
  //            byte of the previous varint, OR at "base", whichever one comes
  //            first. Returns pointer to the first byte of the decoded varint
  //            NULL if a valid varint value was not found.
  static const uint8* Parse32Backward(const uint8* ptr, const uint8* base,
                                     uint32* OUTPUT);
  static const uint8* Parse64Backward(const uint8* ptr, const uint8* base,
                                     uint64* OUTPUT);

  // Attempts to parse a varint32 from a prefix of the bytes in [ptr,limit-1].
  // Never reads a character at or beyond limit.  If a valid/terminated varint32
  // was found in the range, stores it in *OUTPUT and returns a pointer just
  // past the last byte of the varint32. Else returns NULL.  On success,
  // "result <= limit".
  static const uint8* Parse32WithLimit(const uint8* ptr, const uint8* limit,
                                      uint32* OUTPUT);
  static const uint8* Parse64WithLimit(const uint8* ptr, const uint8* limit,
                                      uint64* OUTPUT);

  // REQUIRES   "ptr" points to the first byte of a varint-encoded value.
  // EFFECTS     Scans until the end of the varint and returns a pointer just
  //             past the last byte. Returns NULL if "ptr" does not point to
  //             a valid varint value.
  static const uint8* Skip32(const uint8* ptr);
  static const uint8* Skip64(const uint8* ptr);

  // REQUIRES   "ptr" points just past the last byte of a varint-encoded value.
  // REQUIRES   A second varint must be encoded just before the one we parse,
  //            OR "base" must point to the first byte of the one we parse.
  // REQUIRES   Bytes [base, ptr-1] are readable
  //
  // EFFECTS    Scan backwards from "ptr" and stop at the last byte of the
  //            previous varint, OR at "base", whichever one comes first.
  //            Returns pointer to the first byte of the skipped varint or
  //            NULL if a valid varint value was not found.
  static const uint8* Skip32Backward(const uint8* ptr, const uint8* base);
  static const uint8* Skip64Backward(const uint8* ptr, const uint8* base);

  // REQUIRES   "ptr" points to a buffer of length sufficient to hold "v".
  // EFFECTS    Encodes "v" into "ptr" and returns a pointer to the
  //            byte just past the last encoded byte.
  static uint8* Encode32(uint8* ptr, uint32 v);
  static uint8* Encode64(uint8* ptr, uint64 v);

  // A fully inlined version of Encode32: useful in the most time critical
  // routines, but its code size is large
  static uint8* Encode32Inline(uint8* ptr, uint32 v);

  // EFFECTS    Returns the encoding length of the specified value.
  static int Length32(uint32 v);
  static int Length64(uint64 v);

  static int Length32NonInline(uint32 v);

  // EFFECTS    Appends the varint representation of "value" to "*s".
  static void Append32(std::string* s, uint32 value);
  static void Append64(std::string* s, uint64 value);

  // EFFECTS    Encodes a pair of values to "*s".  The encoding
  //            is done by weaving together 4 bit groups of
  //            each number into a single 64 bit value, and then
  //            encoding this value as a Varint64 value.  This means
  //            that if both a and b are small, both values can be
  //            encoded in a single byte.
  static void EncodeTwo32Values(std::string* s, uint32 a, uint32 b);
  static const uint8* DecodeTwo32Values(const uint8* ptr, uint32* a, uint32* b);

  // Decode and sum up a sequence of deltas until the sum >= goal.
  // It is significantly faster than calling ParseXXInline in a loop.
  // NOTE(user): The code does NO error checking, it assumes all the
  // deltas are valid and the sum of deltas will never exceed kint64max. The
  // code works for both 32bits and 64bits varint, and on 64 bits machines,
  // the 64 bits version is almost always faster. Thus we only have a 64 bits
  // interface here. The interface is slightly different from the other
  // functions in that it requires *signed* integers.
  // REQUIRES   "ptr" points to the first byte of a varint-encoded delta.
  //            The sum of deltas >= goal (the code does NO boundary check).
  //            goal is positive and fit into a signed int64.
  // EFFECTS    Returns a pointer just past last read byte.
  //            "out" stores the actual sum.
  static const uint8* FastDecodeDeltas(const uint8* ptr, int64 goal, int64* out);

 private:
  static const uint8* Parse32FallbackInline(const uint8* p, uint32* val);
  static const uint8* Parse32Fallback(const uint8* p, uint32* val);
  static const uint8* Parse64Fallback(const uint8* p, uint64* val);

  static uint8* Encode32Fallback(uint8* ptr, uint32 v);

  static const uint8* DecodeTwo32ValuesSlow(const uint8* p, uint32* a, uint32* b);
  static const uint8* Parse32BackwardSlow(const uint8* ptr, const uint8* base,
                                         uint32* OUTPUT);
  static const uint8* Parse64BackwardSlow(const uint8* ptr, const uint8* base,
                                         uint64* OUTPUT);
  static const uint8* Skip32BackwardSlow(const uint8* ptr, const uint8* base);
  static const uint8* Skip64BackwardSlow(const uint8* ptr, const uint8* base);

  static void Append32Slow(std::string* s, uint32 value);
  static void Append64Slow(std::string* s, uint64 value);

  // Mapping from rightmost bit set to the number of bytes required
  static const uint8 length32_bytes_required[33];
};

/***** Implementation details; clients should ignore *****/

template<> inline uint8* Varint::Encode<uint64>(uint8* ptr, uint64 v) {
  return Varint::Encode64(ptr, v);
}

template<> inline uint8* Varint::Encode<uint32>(uint8* ptr, uint32 v) {
  return Varint::Encode32(ptr, v);
}

inline const uint8* Varint::Parse32FallbackInline(const uint8* ptr,
                                                  uint32* OUTPUT) {
  // Fast path
  uint32 byte, result;
  byte = *(ptr++); result = byte & 127;
  assert(byte >= 128);   // Already checked in inlined prelude
  byte = *(ptr++); result |= (byte & 127) <<  7; if (byte < 128) goto done;
  byte = *(ptr++); result |= (byte & 127) << 14; if (byte < 128) goto done;
  byte = *(ptr++); result |= (byte & 127) << 21; if (byte < 128) goto done;
  byte = *(ptr++); result |= (byte & 127) << 28; if (byte < 128) goto done;
  return NULL;       // Value is too long to be a varint32
 done:
  *OUTPUT = result;
  return (ptr);
}

inline const uint8* Varint::Parse32(const uint8* ptr, uint32* OUTPUT) {
  // Fast path for inlining
  uint32 byte = *ptr;
  if (byte < 128) {
    *OUTPUT = byte;
    return (ptr) + 1;
  } else {
    return Parse32Fallback(ptr, OUTPUT);
  }
}

inline const uint8* Varint::Parse32Inline(const uint8* ptr, uint32* OUTPUT) {
  // Fast path for inlining
  uint32 byte = *ptr;
  if (byte < 128) {
    *OUTPUT = byte;
    return (ptr) + 1;
  } else {
    return Parse32FallbackInline(ptr, OUTPUT);
  }
}

inline const uint8* Varint::Skip32(const uint8* ptr) {
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  return NULL; // value is too long to be a varint32
}

inline const uint8* Varint::Parse32Backward(const uint8* ptr, const uint8* base,
                                           uint32* OUTPUT) {
  if (ptr > base + kMax32) {
    // Fast path
    uint32 byte, result;
    byte = *(--ptr); if (byte > 127) return NULL;
    result = byte;
    byte = *(--ptr); if (byte < 128) goto done;
    result <<= 7; result |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    result <<= 7; result |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    result <<= 7; result |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    result <<= 7; result |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    return NULL; // Value is too long to be a varint32
 done:
    *OUTPUT = result;
    return (ptr+1);
  } else {
    return Parse32BackwardSlow(ptr, base, OUTPUT);
  }
}

inline const uint8* Varint::Skip32Backward(const uint8* ptr, const uint8* base) {
  if (ptr > base + kMax32) {
    if (*(--ptr) > 127) return NULL;
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    return NULL; // value is too long to be a varint32
  } else {
    return Skip32BackwardSlow(ptr, base);
  }
}

inline const uint8* Varint::Parse32WithLimit(const uint8* ptr,
                                             const uint8* limit,
                                            uint32* OUTPUT) {
  if (ptr + kMax32 <= limit) {
    return Parse32(ptr, OUTPUT);
  } else {
    // Slow version with bounds checks
    uint32 b, result;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result = b & 127;          if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) <<  7; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 14; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 21; if (b < 128) goto done;
    if (ptr >= limit) return NULL;
    b = *(ptr++); result |= (b & 127) << 28; if (b < 16) goto done;
    return NULL;       // Value is too long to be a varint32
   done:
    *OUTPUT = result;
    return (ptr);
  }

}

inline const uint8* Varint::Parse64(const uint8* ptr, uint64* OUTPUT) {
  uint32 byte = *ptr;
  if (byte < 128) {
    *OUTPUT = byte;
    return (ptr) + 1;
  } else {
    return Parse64Fallback(ptr, OUTPUT);
  }
}

inline const uint8* Varint::Skip64(const uint8* ptr) {
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  if (*ptr++ < 128) return (ptr);
  return NULL; // value is too long to be a varint64
}

inline const uint8* Varint::Parse64Backward(const uint8* ptr, const uint8* b,
                                            uint64* OUTPUT) {
  if (ptr > b + kMax64) {
    // Fast path
    uint32 byte;
    uint64 res;

    byte = *(--ptr); if (byte > 127) return NULL;

    res = byte;
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;
    res <<= 7; res |= (byte & 127);
    byte = *(--ptr); if (byte < 128) goto done;

    return NULL;       // Value is too long to be a varint64

 done:
    *OUTPUT = res;
    return (ptr + 1);
  } else {
    return Parse64BackwardSlow(ptr, b, OUTPUT);
  }
}

inline const uint8* Varint::Skip64Backward(const uint8* ptr, const uint8* b) {
  if (ptr > b + kMax64) {
    // Fast path
    if (*(--ptr) > 127) return NULL;
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    if (*(--ptr) < 128) return (ptr+1);
    return NULL; // value is too long to be a varint64
  } else {
    return Skip64BackwardSlow(ptr, b);
  }
}

inline const uint8* Varint::DecodeTwo32Values(const uint8* ptr,
                                             uint32* a, uint32* b) {
  if (*ptr < 128) {
    // Special case for small values
    *a = (*ptr & 0xf);
    *b = *ptr >> 4;
    return (ptr) + 1;
  } else {
    return DecodeTwo32ValuesSlow(ptr, a, b);
  }
}

#if (defined __i386__ || defined __x86_64__) && defined __GNUC__
inline int Varint::Length32(uint32 v) {
  // Find the rightmost bit set, and index into a small table

  // "ro" for the input spec means the input can come from either a
  // register ("r") or offsetable memory ("o").
  //
  // If "n == 0", the "bsr" instruction sets the "Z" flag, so we
  // conditionally move "-1" into the result.
  //
  // Note: the cmovz was introduced on PIII's, and may not work on
  // older machines.
  int bits;
  const int neg1 = -1;
  __asm__("bsr   %1, %0\n\t"
          "cmovz %2, %0"
          : "=&r" (bits)               // Output spec, early clobber
          : "ro" (v), "r" (neg1)        // Input spec
          : "cc"                        // Clobbers condition-codes
          );
  return Varint::length32_bytes_required[bits+1];
}

#else
inline int Varint::Length32(uint32 v) {
  /*
    The following version is about 1.5X the code size, but is faster than
    the loop below.

  if (v < (1u << 14)) {
    if (v < (1u << 7)) {
      return 1;
    } else {
      return 2;
    }
  } else {
    if (v < (1u << 28)) {
      if (v < (1u << 21)) {
        return 3;
      } else {
        return 4;
      }
    } else {
      return 5;
    }
  }
  */

  // Each byte of output stores 7 bits of "v" until "v" becomes zero
  int nbytes = 0;
  do {
    nbytes++;
    v >>= 7;
  } while (v != 0);
  return nbytes;
}
#endif

inline void Varint::Append32(std::string* s, uint32 value) {
  // Inline the fast-path for single-character output, but fall back to the .cc
  // file for the full version. The size<capacity check is so the compiler can
  // optimize out the string resize code.
  if (value < 128 && s->size() < s->capacity()) {
    s->push_back((unsigned char)value);
  } else {
    Append32Slow(s, value);
  }
}

inline void Varint::Append64(std::string* s, uint64 value) {
  // Inline the fast-path for single-character output, but fall back to the .cc
  // file for the full version. The size<capacity check is so the compiler can
  // optimize out the string resize code.
  if (value < 128 && s->size() < s->capacity()) {
    s->push_back((unsigned char)value);
  } else {
    Append64Slow(s, value);
  }
}

inline uint8* Varint::Encode32Inline(uint8* ptr, uint32 v) {
  // Operate on characters as unsigneds
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return (ptr);
}

#if (-1 >> 1) != -1
#error FastDecodeDeltas() needs right-shift to sign-extend.
#endif
inline const uint8* Varint::FastDecodeDeltas(const uint8* ptr,
                                            int64 goal,
                                            int64* out) {
  int64 value;
  int64 sum = - goal;
  int64 shift = 0;
  // Make decoding faster by eliminating unpredictable branching.
  do {
    value = static_cast<int8>(*ptr++);  // sign extend one byte of data
    sum += (value & 0x7F) << shift;
    shift += 7;
    // (value >> 7) is either -1(continuation byte) or 0 (stop byte)
    shift &= value >> 7;
    // Loop if we haven't reached goal (sum < 0) or we haven't finished
    // parsing current delta (value < 0). We write it in the form of
    // (a | b) < 0 as opposed to (a < 0 || b < 0) as the former one is
    // usually as fast as a test for (a < 0).
  } while ((sum | value) < 0);

  *out = goal + sum;
  return ptr;
}

#endif /* _VARINT_H */
