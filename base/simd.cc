// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/simd.h"
#include "base/bits.h"
#include "base/logging.h"

#include <x86intrin.h>

namespace base {

// Returns 16bit mask saying which byte in p equals to the appropriate byte in cx16.
// p points to 16 byte region that is compared to cx16.
inline uint16_t EqualChar16(const void* p, __m128i cx16) {
  return _mm_movemask_epi8(_mm_cmpeq_epi8(cx16, *(const __m128i*)p));
}


// Based on https://mischasan.wordpress.com/2011/11/09/the-generic-sse2-loop/
size_t CountVal8(const uint8_t* ptr, size_t len, char c) {
  size_t res = 0;
  size_t i;

#define CNT_LOOP(x) \
  for (i = 0; i < (x); ++i) \
    res += (ptr[i] == c)

  if (UNLIKELY(len < 16)) {
    CNT_LOOP(len);
    return res;
  }

  __m128i cx16 = _mm_set1_epi8(c);  // replicated c into 16 bytes.
  size_t align = 15 & (-intptr_t(ptr));

  if (align) {
    CNT_LOOP(align);
    ptr += align;
    len -= align;
  }
  const uint8_t* end = ptr + (len & size_t(~15ULL));

  for (; ptr < end; ptr += 16) {
    uint16_t m = EqualChar16(ptr, cx16);
    res += Bits::CountOnes(m);
  }
  len &= 15;

  CNT_LOOP(len);
  return res;
}

#ifdef __SSE4_1__

// taken from: https://github.com/lemire/FastDifferentialCoding/blob/master/src/fastdelta.c
void ComputeDeltasInplace(uint32_t* buffer, size_t length, uint32_t starting_point) {
  __m128i prev = _mm_set1_epi32(starting_point); // starting_point replicated 4 times.
  size_t i = 0;
  __m128i* buf16 = (__m128i*)buffer;

  for(; i  < length/4; i++) {
    // Load 16 bytes from buf16 + i. Note that buf16 + i advances with 16 byte steps.
    __m128i curr =  _mm_lddqu_si128 (buf16 + i);

    // val is (curr << 4bytes) | (prev >> 12 bytes): cur[2], cur[1], cur[0], prev[3].
    __m128i val = _mm_or_si128(_mm_slli_si128(curr, 4), _mm_srli_si128(prev, 12));

    // cur[3] - cur[2], cur[2] - cur[1], cur[1] - cur[0], cur[0] - prev[3].
    __m128i delta = _mm_sub_epi32(curr, val);

    _mm_storeu_si128(buf16 + i, delta);
    prev = curr;
  }

  // sse3.
  uint32_t lastprev = _mm_extract_epi32(prev, 3);
  for(i = 4 * i; i < length; ++i) {
    uint32_t curr = buffer[i];
    buffer[i] = curr - lastprev;
    lastprev = curr;
  }
}

void ComputeDeltasInplace(uint16_t* buffer, size_t length, uint16_t starting_point) {
  __m128i prev = _mm_set1_epi32(starting_point); // starting_point replicated 4 times.
  size_t i = 0;
  __m128i* buf16 = (__m128i*)buffer;

  for(; i  < length/8; i++) {
    // Load 16 bytes (8 uint16).
    __m128i curr =  _mm_lddqu_si128 (buf16 + i);

    // val is (curr << 2bytes) | (prev >> 14 bytes), i.e. last 2 bytes of prev are stored at bytes
    // 0, 1 and curr is shifted 1 place left (in uint16 step).
    __m128i val = _mm_or_si128(_mm_slli_si128(curr, 2), _mm_srli_si128(prev, 14));

    __m128i delta = _mm_sub_epi16(curr, val);

    _mm_storeu_si128(buf16 + i, delta);
    prev = curr;
  }

  // sse2.
  uint16_t lastprev = _mm_extract_epi16(prev, 7);
  for(i = 8 * i; i < length; ++i) {
    uint16_t curr = buffer[i];
    buffer[i] = curr - lastprev;
    lastprev = curr;
  }
}

#endif

}  // namespace base
