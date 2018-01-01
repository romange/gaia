// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <random>

#include "base/flit.h"
#include "base/logging.h"
#include "base/gtest.h"

#include "strings/numbers.h"
#include "strings/strip.h"
#include "base/varint.h"

namespace base {
using namespace std;

static_assert(flit::Traits<uint32_t>::max_size == 5, "");
static_assert(flit::Traits<uint16_t>::max_size == 3, "");
static_assert(flit::Traits<uint64_t>::max_size == 9, "");

#if 0
static std::vector<uint64> ReadIds() {
  file::LineReader line_reader(ProgramRunfile("testdata/flit.txt.zst"));
  decltype(ReadIds()) res;

  StringPiece line;
  uint64_t val;
  while (line_reader.Next(&line)) {
    StripWhiteSpace(&line);
    CHECK(!line.empty());
    CHECK(safe_strtou64(line, &val)) << line;
    res.push_back(val);
  }
  return res;
}
#endif

inline int flit64enc(void* buf, uint64_t v) {
  int lzc = 64;
  if (v) lzc = __builtin_clzll(v);
  if (lzc > 56) {
    *(uint8_t*)buf = (uint8_t)v << 1 | 1;
    return 1;
  }
  if (lzc < 8) {
    uint8_t* p = (uint8_t*)buf;
    *p++ = 0;
    *(uint64_t*)p = v;
    return 9;
  }

  // count extra bytes
  unsigned e = ((63 - lzc) * 2454267027) >> 34;  // (63 - lzc) / 7

  v <<= 1;
  v |= 1;
  v <<= e;
  *(uint64_t*)buf = v;

  return e + 1;
}

inline int flit64dec(uint64_t* v, const void* buf) {
  uint64_t x = *(uint64_t*)buf;

  int tzc = 8;
  if (x) tzc = __builtin_ctzll(x);
  if (tzc > 7) {
   const uint8_t* cp = (const uint8_t*)buf + 1;
   *v = *(const uint64_t*)cp;
   return 9;
  }

  static const uint64_t mask[8] = {
    0xff,
    0xffff,
    0xffffff,
    0xffffffff,
    0xffffffffff,
    0xffffffffffff,
    0xffffffffffffff,
    0xffffffffffffffff,
  };
  x &= mask[tzc];

  // const here seems to ensure that 'size' is not aliased by '*v'
  const int size = tzc + 1;

  *v = x >> size;

  return size;
}

class FlitTest : public testing::Test {

protected:
  unsigned Flit64(uint64 val) {
    return flit::Encode64(val, buf_);
  }

  unsigned UnFlit64(uint64* val) {
    return flit::ParseT(buf_, val);
  }

  uint8_t buf_[9];
};

TEST_F(FlitTest, Basic) {
  EXPECT_EQ(1, flit::Length<uint32_t>(127));
  EXPECT_EQ(2, flit::Length<uint32_t>(128));
  EXPECT_EQ(2, flit::Length<uint32_t>((1 << 14) - 1));
  EXPECT_EQ(3, flit::Length<uint32_t>(1 << 14));
}

#define TEST_CONST(x, y) \
  ASSERT_EQ(y, Flit64(x)); \
  ASSERT_EQ(y, flit::ParseLengthT<uint64_t>(buf_)); \
  ASSERT_EQ(y, UnFlit64(&val)); \
  EXPECT_EQ((x), val)

TEST_F(FlitTest, Flit) {
  uint64 val = 0;
  TEST_CONST(0, 1);
  TEST_CONST(127, 1);
  TEST_CONST(128, 2);
  TEST_CONST(255, 2);
  TEST_CONST((1<<14) - 1, 2);
  TEST_CONST((1<<21) - 1, 3);
  TEST_CONST((1ULL << 32), 5);
  TEST_CONST((1ULL << 31), 5);
  TEST_CONST((1ULL<<56), 9);
  TEST_CONST((1ULL<<63), 9);
  TEST_CONST(85039090594426347ULL, 9);
}

#if 0
TEST_F(FlitTest, Bug1) {
  std::vector<uint64_t> vec = ReadIds();
  size_t buf_len = vec.size() * flit::Traits<uint64_t>::max_size;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[buf_len]);
  uint8_t* next = buf.get();
  for (uint64_t val : vec) {
    next += flit::EncodeT<uint64_t>(val, next);
  }
  ASSERT_LE(next - buf.get(), buf_len);
  const uint8_t* src = buf.get();
  uint64_t val;
  for (unsigned i = 0; i < vec.size(); ++i) {
    unsigned res = flit::ParseT(src, &val);
    ASSERT_EQ(res, flit::Length(val)) << i << " " << vec[i];
    ASSERT_EQ(vec[i], val) << i;
    src += res;
  }
}
#endif

static std::mt19937_64 rnd_engine;

uint64_t RandUint64() {
  unsigned bit_len = (rnd_engine() % 64) + 1;
  return rnd_engine() & ((1ULL << bit_len) - 1);
}

static void FillEncoded(uint8_t* buf, unsigned num) {
  for (unsigned i = 0; i < num; ++i) {
    volatile uint64_t val = RandUint64();
    buf += flit::Encode64(val, buf);
  }
}

constexpr unsigned kBatchLen = 1000;

template <typename T> void BM_FlitEncode(benchmark::State& state) {
  T input[kBatchLen];
  std::generate(input, input + arraysize(input), RandUint64);
  std::unique_ptr<uint8[]> buf(new uint8_t[kBatchLen * 10]);

  while (state.KeepRunning()) {
    uint8_t* next = buf.get();
    for (unsigned i = 0; i < arraysize(input); i +=4) {
      next += flit::EncodeT<T>(input[i], next);
      next += flit::EncodeT<T>(input[i] + 1, next);
      next += flit::EncodeT<T>(input[i] + 2, next);
      next += flit::EncodeT<T>(input[i] + 3, next);
    }
  }
}
BENCHMARK_TEMPLATE(BM_FlitEncode, uint32_t);
BENCHMARK_TEMPLATE(BM_FlitEncode, uint64_t);


static void BM_FlitEncodeGold(benchmark::State& state) {
  uint64 input[kBatchLen];
  std::generate(input, input + arraysize(input), RandUint64);
  uint8 buf[arraysize(input) * 10];

  while (state.KeepRunning()) {
    uint8_t* next = buf;
    for (unsigned i = 0; i < arraysize(input); i +=4) {
      next += flit64enc(next, input[i]);
      next += flit64enc(next, input[i] + 1);
      next += flit64enc(next, input[i] + 2);
      next += flit64enc(next, input[i] + 3);
    }
    sink_result(next);
  }
}
BENCHMARK(BM_FlitEncodeGold);


static void BM_FlitDecode(benchmark::State& state) {
  uint8_t buf[kBatchLen * 9];
  FillEncoded(buf, kBatchLen);

  while (state.KeepRunning()) {
    uint64_t val = 0;
    const uint8_t* rn = buf;
    for (unsigned i = 0; i < kBatchLen /4; ++i) {
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      rn += flit::Parse64Fast(rn, &val);
      sink_result(val);
    }
  }
}
BENCHMARK(BM_FlitDecode);

static void BM_FlitDecodeGold(benchmark::State& state) {
  uint8_t buf[kBatchLen * 9];
  FillEncoded(buf, kBatchLen);

  while (state.KeepRunning()) {
    uint64_t val = 0;
    const uint8_t* rn = buf;
    for (unsigned i = 0; i < kBatchLen /4; ++i) {
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      rn += flit64dec(&val, rn);
      sink_result(val);
    }
  }
}
BENCHMARK(BM_FlitDecodeGold);

static void BM_VarintEncode(benchmark::State& state) {
  uint64 input[kBatchLen];
  for (unsigned i = 0; i < arraysize(input); ++i)
    input[i] = RandUint64();
  uint8 buf[kBatchLen * 10];

  while (state.KeepRunning()) {
    uint8_t* rn = buf;
    for (unsigned i = 0; i < arraysize(input); i +=4) {
      rn = Varint::Encode64(rn, input[i]);
      rn = Varint::Encode64(rn, input[i] + 1);
      rn = Varint::Encode64(rn, input[i] + 2);
      rn = Varint::Encode64(rn, input[i] + 3);
    }
  }
}
BENCHMARK(BM_VarintEncode);

}  // namespace util
