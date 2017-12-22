// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/coding/double_compressor.h"
#include <gmock/gmock.h>

#include "base/logging.h"
#include "util/math/float2decimal.h"

using testing::ElementsAreArray;

namespace util {

class DoubleCompressorTest : public testing::Test {
 public:
  DoubleCompressorTest() {
  }

 protected:
  DoubleCompressor dc_;
  DoubleDecompressor dd_;

  static_assert(dtoa::FpTraits<double>::kMaxExp == 972, "");
  static_assert(dtoa::FpTraits<double>::kDenormalExp == -1074, "");

  static double MakeDouble(uint64_t f, int e) {
    return dtoa::FpTraits<double>::FromFP(f, e);
  }
  uint8_t buf_[DoubleCompressor::COMMIT_MAX_SIZE];
  double actual_[DoubleDecompressor::BLOCK_MAX_LEN];
};


TEST_F(DoubleCompressorTest, Basic) {
  std::vector<double> arr({1, 2, 3, 4, 5, 6});

  uint32_t sz;
  int res;

  sz = dc_.Commit(arr.data(), arr.size(), buf_);
  EXPECT_EQ(sz, DoubleDecompressor::BlockSize(buf_));

  res = dd_.Decompress(buf_, sz, actual_);
  ASSERT_EQ(6, res);

  for (int i = 0; i < res; ++i) {
    EXPECT_EQ(arr[i], actual_[i]);
  }

  for (unsigned i = 7; i <= 2048; ++i) {
    arr.push_back(double(i) * 100000);
  }
  sz = dc_.Commit(arr.data(), arr.size(), buf_);
  res = dd_.Decompress(buf_, sz, actual_);
  ASSERT_EQ(2048, res);

  for (unsigned i = 1; i <= 6; ++i) {
    ASSERT_EQ(i, actual_[i - 1]);
  }
  for (unsigned i = 7; i <= 2048; ++i) {
    ASSERT_FLOAT_EQ(double(i) * 100000, actual_[i - 1]);
  }
}

TEST_F(DoubleCompressorTest, BigExp) {
  std::vector<double> inp;
  unsigned sz;
  int res;
  for (unsigned i = 0; i < 128; ++i) {
    const uint64_t val = 6567258882077402UL;
    double d = MakeDouble(val + i, 954);
    inp.push_back(d);
    // fprintf(stdout, "%.30e\n", d);
  }

  sz = dc_.Commit(inp.data(), inp.size(), buf_);
  res = dd_.Decompress(buf_, sz, actual_);
  ASSERT_EQ(128, res);
}

}  // namespace util
