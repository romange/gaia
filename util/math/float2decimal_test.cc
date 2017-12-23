// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/math/float2decimal.h"

#include <random>
#include <double-conversion/double-conversion.h>

#include "base/gtest.h"
#include "strings/stringpiece.h"

namespace util {

static float StringToSingle(StringPiece str) {
  double_conversion::StringToDoubleConverter conv(0, 0.0, 0.0, "inf", "nan");

  int processed_characters_count = 0;
  return conv.StringToFloat(str.data(), str.size(), &processed_characters_count);
}

static double StringToDouble(char const* str, char const* end) {
  double_conversion::StringToDoubleConverter conv(0, 0.0, 0.0, "inf", "nan");

  int processed_characters_count = 0;
  return conv.StringToDouble(str, static_cast<int>(end - str), &processed_characters_count);
}

static float MakeSingle(uint32_t sign_bit, uint32_t biased_exponent, uint32_t significand) {
  assert(sign_bit == 0 || sign_bit == 1);
  assert(biased_exponent <= 0xFF);
  assert(significand <= 0x007FFFFF);

  uint32_t bits = 0;

  bits |= sign_bit << 31;
  bits |= biased_exponent << 23;
  bits |= significand;

  return IEEEFloat<float>(bits).value;
}

static float MakeSingle(uint64_t f, int e) {
  return dtoa::FpTraits<float>::FromFP(f, e);
}

static double MakeDouble(uint64_t sign_bit, uint64_t biased_exponent, uint64_t significand) {
  assert(sign_bit == 0 || sign_bit == 1);
  assert(biased_exponent <= 0x7FF);
  assert(significand <= 0x000FFFFFFFFFFFFF);

  uint64_t bits = 0;

  bits |= sign_bit << 63;
  bits |= biased_exponent << 52;
  bits |= significand;

  return IEEEFloat<double>(bits).value;
}

// ldexp -- convert f * 2^e to IEEE double precision
static double MakeDouble(uint64_t f, int e) {
  return dtoa::FpTraits<double>::FromFP(f, e);
}

class Float2DecimalTest : public testing::Test {
 protected:
  void CheckFloat(float d0) const;
  void CheckDouble(double d0) const;
};

void Float2DecimalTest::CheckFloat(float d0) const {
  char str[32];
  auto const end = dtoa::ToString(d0, str);

  assert(end - str <= 26);
  *end = '\0';

  {
    float const d1 = StringToSingle(StringPiece(str, end - str));
    uint32_t const b0 = IEEEFloat<float>(d0).bits;
    uint32_t const b1 = IEEEFloat<float>(d1).bits;
    ASSERT_EQ(b0, b1) << d0;
  }

  {
    float const d1 = StringToDouble(str, end);
    uint32_t const b0 = IEEEFloat<float>(d0).bits;
    uint32_t const b1 = IEEEFloat<float>(d1).bits;
    ASSERT_EQ(b0, b1);
  }

  const auto& conv = double_conversion::DoubleToStringConverter::EcmaScriptConverter();

  char dc[32];
  double_conversion::StringBuilder builder(dc, 32);
  conv.ToShortestSingle(d0, &builder);
  builder.Finalize();
  EXPECT_EQ(strlen(str), strlen(dc));
  if (d0 != 0.000244140625) {
    EXPECT_STREQ(dc, str) << d0;
  }
}

void Float2DecimalTest::CheckDouble(double d0) const {
  char str[32];
  auto const end = dtoa::ToString(d0, str);

  assert(end - str <= 26);
  *end = '\0';

  {
    double const d1 = StringToDouble(str, end);
    uint64_t const b0 = IEEEFloat<double>(d0).bits;
    uint64_t const b1 = IEEEFloat<double>(d1).bits;
    ASSERT_EQ(b0, b1);
  }
}

TEST_F(Float2DecimalTest, Basic) {
  CheckFloat(1.2621774483536189e-29);
  CheckFloat(2048.00024);
  CheckFloat(0.000244140625);
  CheckFloat(12345.60001);
  CheckFloat(123.456);
  CheckFloat(123.456e35);
  CheckFloat(123456.7890123);
}

TEST_F(Float2DecimalTest, Single) {
  CheckFloat(MakeSingle(0, 0, 0x00000000));  // +0
  CheckFloat(MakeSingle(0, 0, 0x00000001));  // min denormal
  CheckFloat(MakeSingle(0, 0, 0x007FFFFF));  // max denormal
  CheckFloat(MakeSingle(0, 1, 0x00000000));  // min normal
  CheckFloat(MakeSingle(0, 1, 0x00000001));
  CheckFloat(MakeSingle(0, 1, 0x007FFFFF));
  CheckFloat(MakeSingle(0, 2, 0x00000000));
  CheckFloat(MakeSingle(0, 2, 0x00000001));
  CheckFloat(MakeSingle(0, 24, 0x00000000));  // fail if no special case in normalized boundaries
  CheckFloat(MakeSingle(0, 30, 0x00000000));  // fail if no special case in normalized boundaries
  CheckFloat(MakeSingle(0, 31, 0x00000000));  // fail if no special case in normalized boundaries
  CheckFloat(MakeSingle(0, 57, 0x00000000));  // fail if no special case in normalized boundaries
  CheckFloat(MakeSingle(0, 254, 0x007FFFFE));
  CheckFloat(MakeSingle(0, 254, 0x007FFFFF));  // max normal
  for (int e = 2; e < 254; ++e) {
    CheckFloat(MakeSingle(0, e - 1, 0x007FFFFF));
    CheckFloat(MakeSingle(0, e, 0x00000000));
    CheckFloat(MakeSingle(0, e, 0x00000001));
  }

  // V. Paxson and W. Kahan, "A Program for Testing IEEE Binary-Decimal Conversion", manuscript, May
  // 1991,
  // ftp://ftp.ee.lbl.gov/testbase-report.ps.Z    (report)
  // ftp://ftp.ee.lbl.gov/testbase.tar.Z          (program)

  // Table 16: Stress Inputs for Converting 24-bit Binary to Decimal, < 1/2 ULP
  CheckFloat(MakeSingle(12676506, -102));  // digits  1, bits 32
  CheckFloat(MakeSingle(12676506, -103));  // digits  2, bits 29
  CheckFloat(MakeSingle(15445013, +86));   // digits  3, bits 34
  CheckFloat(MakeSingle(13734123, -138));  // digits  4, bits 32
  CheckFloat(MakeSingle(12428269, -130));  // digits  5, bits 30
  CheckFloat(MakeSingle(15334037, -146));  // digits  6, bits 31
  CheckFloat(MakeSingle(11518287, -41));   // digits  7, bits 30
  CheckFloat(MakeSingle(12584953, -145));  // digits  8, bits 31
  CheckFloat(MakeSingle(15961084, -125));  // digits  9, bits 32
  CheckFloat(MakeSingle(14915817, -146));  // digits 10, bits 31
  CheckFloat(MakeSingle(10845484, -102));  // digits 11, bits 30
  CheckFloat(MakeSingle(16431059, -61));   // digits 12, bits 29

  // Table 17: Stress Inputs for Converting 24-bit Binary to Decimal, > 1/2 ULP
  CheckFloat(MakeSingle(16093626, +69));   // digits  1, bits 30
  CheckFloat(MakeSingle(9983778, +25));    // digits  2, bits 31
  CheckFloat(MakeSingle(12745034, +104));  // digits  3, bits 31
  CheckFloat(MakeSingle(12706553, +72));   // digits  4, bits 31
  CheckFloat(MakeSingle(11005028, +45));   // digits  5, bits 30
  CheckFloat(MakeSingle(15059547, +71));   // digits  6, bits 31
  CheckFloat(MakeSingle(16015691, -99));   // digits  7, bits 29
  CheckFloat(MakeSingle(8667859, +56));    // digits  8, bits 33
  CheckFloat(MakeSingle(14855922, -82));   // digits  9, bits 35
  CheckFloat(MakeSingle(14855922, -83));   // digits 10, bits 33
  CheckFloat(MakeSingle(10144164, -110));  // digits 11, bits 32
  CheckFloat(MakeSingle(13248074, +95));   // digits 12, bits 33
}

TEST_F(Float2DecimalTest, Double) {
  CheckDouble(MakeDouble(0, 0, 0x0000000000000000));  // +0
  CheckDouble(MakeDouble(0, 0, 0x0000000000000001));  // min denormal
  CheckDouble(MakeDouble(0, 0, 0x000FFFFFFFFFFFFF));  // max denormal
  CheckDouble(MakeDouble(0, 1, 0x0000000000000000));  // min normal
  CheckDouble(MakeDouble(0, 1, 0x0000000000000001));
  CheckDouble(MakeDouble(0, 1, 0x000FFFFFFFFFFFFF));
  CheckDouble(MakeDouble(0, 2, 0x0000000000000000));
  CheckDouble(MakeDouble(0, 2, 0x0000000000000001));
  CheckDouble(
      MakeDouble(0, 4, 0x0000000000000000));  // fail if no special case in normalized boundaries
  CheckDouble(
      MakeDouble(0, 5, 0x0000000000000000));  // fail if no special case in normalized boundaries
  CheckDouble(
      MakeDouble(0, 6, 0x0000000000000000));  // fail if no special case in normalized boundaries
  CheckDouble(
      MakeDouble(0, 10, 0x0000000000000000));  // fail if no special case in normalized boundaries
  CheckDouble(MakeDouble(0, 2046, 0x000FFFFFFFFFFFFE));
  CheckDouble(MakeDouble(0, 2046, 0x000FFFFFFFFFFFFF));  // max normal

  for (int e = 2; e < 2046; ++e) {
    CheckDouble(MakeDouble(0, e - 1, 0x000FFFFFFFFFFFFF));
    CheckDouble(MakeDouble(0, e, 0x0000000000000000));
    CheckDouble(MakeDouble(0, e, 0x0000000000000001));
  }

  // Some numbers to check different code paths in fast_dtoa
  CheckDouble(-1.0);
  CheckDouble(1e+4);
  CheckDouble(1.2e+6);
  CheckDouble(4.9406564584124654e-324);  // DigitGen: exit integral loop
  CheckDouble(2.2250738585072009e-308);  // DigitGen: exit fractional loop
  CheckDouble(1.82877982605164e-99);
  CheckDouble(1.1505466208671903e-09);
  CheckDouble(5.5645893133766722e+20);
  CheckDouble(53.034830388866226);
  CheckDouble(0.0021066531670178605);

  // V. Paxson and W. Kahan, "A Program for Testing IEEE Binary-Decimal Conversion", manuscript, May
  // 1991,
  // ftp://ftp.ee.lbl.gov/testbase-report.ps.Z    (report)
  // ftp://ftp.ee.lbl.gov/testbase.tar.Z          (program)

  // Table 3: Stress Inputs for Converting 53-bit Binary to Decimal, < 1/2 ULP
  CheckDouble(MakeDouble(8511030020275656, -342));   // digits  1, bits 63
  CheckDouble(MakeDouble(5201988407066741, -824));   // digits  2, bits 63
  CheckDouble(MakeDouble(6406892948269899, +237));   // digits  3, bits 62
  CheckDouble(MakeDouble(8431154198732492, +72));    // digits  4, bits 61
  CheckDouble(MakeDouble(6475049196144587, +99));    // digits  5, bits 64
  CheckDouble(MakeDouble(8274307542972842, +726));   // digits  6, bits 64
  CheckDouble(MakeDouble(5381065484265332, -456));   // digits  7, bits 64
  CheckDouble(MakeDouble(6761728585499734, -1057));  // digits  8, bits 64
  CheckDouble(MakeDouble(7976538478610756, +376));   // digits  9, bits 67
  CheckDouble(MakeDouble(5982403858958067, +377));   // digits 10, bits 63
  CheckDouble(MakeDouble(5536995190630837, +93));    // digits 11, bits 63
  CheckDouble(MakeDouble(7225450889282194, +710));   // digits 12, bits 66
  CheckDouble(MakeDouble(7225450889282194, +709));   // digits 13, bits 64
  CheckDouble(MakeDouble(8703372741147379, +117));   // digits 14, bits 66
  CheckDouble(MakeDouble(8944262675275217, -1001));  // digits 15, bits 63
  CheckDouble(MakeDouble(7459803696087692, -707));   // digits 16, bits 63
  CheckDouble(MakeDouble(6080469016670379, -381));   // digits 17, bits 62
  CheckDouble(MakeDouble(8385515147034757, +721));   // digits 18, bits 64
  CheckDouble(MakeDouble(7514216811389786, -828));   // digits 19, bits 64
  CheckDouble(MakeDouble(8397297803260511, -345));   // digits 20, bits 64
  CheckDouble(MakeDouble(6733459239310543, +202));   // digits 21, bits 63
  CheckDouble(MakeDouble(8091450587292794, -473));   // digits 22, bits 63

  // Table 4: Stress Inputs for Converting 53-bit Binary to Decimal, > 1/2 ULP
  CheckDouble(MakeDouble(6567258882077402, +952));  // digits  1, bits 62
  CheckDouble(MakeDouble(6712731423444934, +535));  // digits  2, bits 65
  CheckDouble(MakeDouble(6712731423444934, +534));  // digits  3, bits 63
  CheckDouble(MakeDouble(5298405411573037, -957));  // digits  4, bits 62
  CheckDouble(MakeDouble(5137311167659507, -144));  // digits  5, bits 61
  CheckDouble(MakeDouble(6722280709661868, +363));  // digits  6, bits 64
  CheckDouble(MakeDouble(5344436398034927, -169));  // digits  7, bits 61
  CheckDouble(MakeDouble(8369123604277281, -853));  // digits  8, bits 65
  CheckDouble(MakeDouble(8995822108487663, -780));  // digits  9, bits 63
  CheckDouble(MakeDouble(8942832835564782, -383));  // digits 10, bits 66
  CheckDouble(MakeDouble(8942832835564782, -384));  // digits 11, bits 64
  CheckDouble(MakeDouble(8942832835564782, -385));  // digits 12, bits 61
  CheckDouble(MakeDouble(6965949469487146, -249));  // digits 13, bits 67
  CheckDouble(MakeDouble(6965949469487146, -250));  // digits 14, bits 65
  CheckDouble(MakeDouble(6965949469487146, -251));  // digits 15, bits 63
  CheckDouble(MakeDouble(7487252720986826, +548));  // digits 16, bits 63
  CheckDouble(MakeDouble(5592117679628511, +164));  // digits 17, bits 65
  CheckDouble(MakeDouble(8887055249355788, +665));  // digits 18, bits 67
  CheckDouble(MakeDouble(6994187472632449, +690));  // digits 19, bits 64
  CheckDouble(MakeDouble(8797576579012143, +588));  // digits 20, bits 62
  CheckDouble(MakeDouble(7363326733505337, +272));  // digits 21, bits 61
  CheckDouble(MakeDouble(8549497411294502, -448));  // digits 22, bits 66

  // Table 20: Stress Inputs for Converting 56-bit Binary to Decimal, < 1/2 ULP
  CheckDouble(MakeDouble(50883641005312716, -172));  // digits  1, bits 65
  CheckDouble(MakeDouble(38162730753984537, -170));  // digits  2, bits 64
  CheckDouble(MakeDouble(50832789069151999, -101));  // digits  3, bits 64
  CheckDouble(MakeDouble(51822367833714164, -109));  // digits  4, bits 62
  CheckDouble(MakeDouble(66840152193508133, -172));  // digits  5, bits 64
  CheckDouble(MakeDouble(55111239245584393, -138));  // digits  6, bits 64
  CheckDouble(MakeDouble(71704866733321482, -112));  // digits  7, bits 62
  CheckDouble(MakeDouble(67160949328233173, -142));  // digits  8, bits 61
  CheckDouble(MakeDouble(53237141308040189, -152));  // digits  9, bits 63
  CheckDouble(MakeDouble(62785329394975786, -112));  // digits 10, bits 62
  CheckDouble(MakeDouble(48367680154689523, -77));   // digits 11, bits 61
  CheckDouble(MakeDouble(42552223180606797, -102));  // digits 12, bits 62
  CheckDouble(MakeDouble(63626356173011241, -112));  // digits 13, bits 62
  CheckDouble(MakeDouble(43566388595783643, -99));   // digits 14, bits 64
  CheckDouble(MakeDouble(54512669636675272, -159));  // digits 15, bits 61
  CheckDouble(MakeDouble(52306490527514614, -167));  // digits 16, bits 67
  CheckDouble(MakeDouble(52306490527514614, -168));  // digits 17, bits 65
  CheckDouble(MakeDouble(41024721590449423, -89));   // digits 18, bits 62
  CheckDouble(MakeDouble(37664020415894738, -132));  // digits 19, bits 60
  CheckDouble(MakeDouble(37549883692866294, -93));   // digits 20, bits 62
  CheckDouble(MakeDouble(69124110374399839, -104));  // digits 21, bits 65
  CheckDouble(MakeDouble(69124110374399839, -105));  // digits 22, bits 62

  // Table 21: Stress Inputs for Converting 56-bit Binary to Decimal, > 1/2 ULP
  CheckDouble(MakeDouble(49517601571415211, -94));   // digits  1, bits 63
  CheckDouble(MakeDouble(49517601571415211, -95));   // digits  2, bits 60
  CheckDouble(MakeDouble(54390733528642804, -133));  // digits  3, bits 63
  CheckDouble(MakeDouble(71805402319113924, -157));  // digits  4, bits 62
  CheckDouble(MakeDouble(40435277969631694, -179));  // digits  5, bits 61
  CheckDouble(MakeDouble(57241991568619049, -165));  // digits  6, bits 61
  CheckDouble(MakeDouble(65224162876242886, +58));   // digits  7, bits 65
  CheckDouble(MakeDouble(70173376848895368, -138));  // digits  8, bits 61
  CheckDouble(MakeDouble(37072848117383207, -99));   // digits  9, bits 61
  CheckDouble(MakeDouble(56845051585389697, -176));  // digits 10, bits 64
  CheckDouble(MakeDouble(54791673366936431, -145));  // digits 11, bits 64
  CheckDouble(MakeDouble(66800318669106231, -169));  // digits 12, bits 64
  CheckDouble(MakeDouble(66800318669106231, -170));  // digits 13, bits 61
  CheckDouble(MakeDouble(66574323440112438, -119));  // digits 14, bits 65
  CheckDouble(MakeDouble(65645179969330963, -173));  // digits 15, bits 62
  CheckDouble(MakeDouble(61847254334681076, -109));  // digits 16, bits 63
  CheckDouble(MakeDouble(39990712921393606, -145));  // digits 17, bits 62
  CheckDouble(MakeDouble(59292318184400283, -149));  // digits 18, bits 62
  CheckDouble(MakeDouble(69116558615326153, -143));  // digits 19, bits 65
  CheckDouble(MakeDouble(69116558615326153, -144));  // digits 20, bits 62
  CheckDouble(MakeDouble(39462549494468513, -152));  // digits 21, bits 63
  CheckDouble(MakeDouble(39462549494468513, -153));  // digits 22, bits 61
}


TEST_F(Float2DecimalTest, Decimal) {
  int64_t val;
  int16_t exponent;
  uint16_t len;

  ASSERT_TRUE(dtoa::ToDecimal(39462549494468513e-153, &val, &exponent, &len));
  EXPECT_EQ(39462549494468514, val);
  EXPECT_EQ(17, len);
  EXPECT_EQ(exponent, -153);

  ASSERT_TRUE(dtoa::ToDecimal(-0.0, &val, &exponent, &len));
  EXPECT_EQ(0, val);

  ASSERT_TRUE(dtoa::ToDecimal(-73.925299999999993, &val, &exponent, &len));
  EXPECT_EQ(-739253, val);

  constexpr double kNum1 = -73.929589;
  ASSERT_TRUE(dtoa::ToDecimal(kNum1, &val, &exponent, &len));
  // EXPECT_EQ(-73929589, val); :(

  const auto& conv = double_conversion::DoubleToStringConverter::EcmaScriptConverter();

  char buf[32];
  double_conversion::StringBuilder builder(buf, 32);
  conv.ToShortest(kNum1, &builder);
  builder.Finalize();

  EXPECT_STREQ("-73.929589", buf);

  char* end = dtoa::ToString(kNum1, buf);
  *end = '\0';
  // EXPECT_STREQ("-73.929589", buf); ? :(
  ASSERT_TRUE(dtoa::ToDecimal(-73.9761, &val, &exponent, &len));
  EXPECT_EQ(-739761, val);
}

static void BM_LoopSingle(benchmark::State& state) {
  int const min_exp = 0;
  int const max_exp = (1 << 8) - 1; // exclusive!

  uint32_t bits = min_exp << 23;

  char str[32];

  while (state.KeepRunning()) {
    float const f = IEEEFloat<float>(bits).value;
    dtoa::ToString(f, str);

    ++bits;

    int next_exp = bits >> 23;
    if (next_exp == max_exp)
      break;
  }
}
BENCHMARK(BM_LoopSingle);

struct RandomDoubles {
    // Test uniformly distributed bit patterns instead of uniformly distributed
    // floating-points...

    std::random_device rd_;
    std::mt19937_64 random_;
    std::uniform_int_distribution<uint64_t> gen_;

    RandomDoubles()
        : rd_()
        , random_(rd_())
        , gen_(0, (uint64_t{0x7FF} << 52) - 1) {
    }

    double operator()() {
      IEEEFloat<double>  v(gen_(random_));
      return v.value;
    }
};

static void BM_LoopDouble(benchmark::State& state) {
  RandomDoubles rng;

  uint64_t iters = state.range_x();
  char str[32];
  std::unique_ptr<double[]> vals(new double[iters]);
  for (uint64_t i = 0; i < iters; ++i) {
    vals[i] = rng();
  }

  while (state.KeepRunning()) {
    for (uint64_t i = 0; i < iters; ++i) {
      dtoa::ToString(vals[i], str);
    }
  }
}
BENCHMARK(BM_LoopDouble)->Arg(1 << 10);

static void BM_DoubleDecimal(benchmark::State& state) {
  uint64_t bits = 0;

  int64_t val;
  int16_t exponent;
  uint16_t len;
  while (state.KeepRunning()) {
    double d = IEEEFloat<double>(bits).value;
    benchmark::DoNotOptimize(dtoa::ToDecimal(d, &val, &exponent, &len));

    ++bits;
  }
}
BENCHMARK(BM_DoubleDecimal);

}  // namespace util


