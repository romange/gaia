// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/simd.h"

#include <memory>
#include <gmock/gmock.h>

#include "base/integral_types.h"
#include "base/gtest.h"
#include "base/logging.h"

using testing::ElementsAreArray;
using testing::ElementsAre;

namespace base {

class SimdTest {

};

TEST(SimdTest, Basic) {
  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8[]> buf(new uint8[kBufSize]);
  std::fill(buf.get(), buf.get() + kBufSize, 1);
  EXPECT_EQ(0, CountVal8(buf.get(), kBufSize, 0));
  ASSERT_EQ(16, CountVal8(buf.get(), 16, 1));
  ASSERT_EQ(16, CountVal8(buf.get() + 1, 16, 1));

  for (unsigned i = 0; i < 512; ++i) {
    for (unsigned len = 0; len < 512 - i; ++len) {
      ASSERT_EQ(len, CountVal8(buf.get() + i, len, 1)) << i;
    }
  }

  for (unsigned i = 0; i < 1024; ++i) {
    buf[i] = (i % 2);
  }
  for (unsigned i = 0; i < 900; ++i) {
    ASSERT_EQ(50, CountVal8(buf.get() + i, 100, 1));
  }
}

TEST(SimdTest, Deltas) {
  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint32[]> buf(new uint32[kBufSize]);
  for (unsigned i = 0; i < kBufSize; ++i)
    buf[i] = i + 1000;

  ComputeDeltasInplace(buf.get(), kBufSize, 0);
  EXPECT_EQ(1000, buf[0]);

  auto pred = [](uint32 val) { return val == 1;};
  auto end = buf.get() + kBufSize;
  EXPECT_TRUE(std::find_if_not(buf.get() + 1, end, pred) == end);

  std::unique_ptr<uint16[]> buf16(new uint16[9]);
  uint16 start = 1000;
  for (unsigned i = 0; i < 9; ++i){
    buf16[i] = i + start;
    start += i;
  }
  ComputeDeltasInplace(buf16.get(), 9, 0);
  EXPECT_THAT(buf16[0], 1000);
  for (unsigned i = 1; i < 9; ++i)
    EXPECT_EQ(buf16[i], i);
}

TEST(SimdTest, OverFlow) {
  std::unique_ptr<uint8_t[]> buf(new uint8_t[32]);
  memset(buf.get(), 1, 32);

  EXPECT_EQ(30, CountVal8(buf.get() + 2, 30, 1));
}

using benchmark::DoNotOptimize;

static void BM_Simd(benchmark::State& state) {
  std::unique_ptr<uint8[]> buf(new uint8[state.range(0) + 20]);
  while (state.KeepRunning()) {
    CountVal8(buf.get() + 13, state.range(0), 1);
  }
}
BENCHMARK(BM_Simd)->Range(8, 1 << 16);

static void BM_Plain(benchmark::State& state) {
  std::unique_ptr<uint8[]> buf(new uint8[state.range(0) + 20]);
  while (state.KeepRunning()) {
    DoNotOptimize(std::count(buf.get() + 13, buf.get() + 13 + state.range(0), 1));
  }
}
BENCHMARK(BM_Plain)->Range(8, 1 << 16);

}  // namespace base
