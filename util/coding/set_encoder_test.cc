// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/coding/set_encoder.h"

#include <random>
#include <gmock/gmock.h>

#include "base/endian.h"
#include "base/gtest.h"
#include "base/logging.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::UnorderedElementsAreArray;
using testing::WhenSorted;

using namespace std;

namespace util {

__attribute__ ((noinline))
void SerializeTo(const vector<uint64_t>& v, uint8_t* dest) {
  for (const auto & val : v) {
    LittleEndian::Store64(dest, val);
    dest += sizeof(uint64_t);
  }
}

template<typename T, bool B> struct  SetEncoderTraits {
  typedef T type;
  static constexpr bool use_sequence = B;
};

template<typename T> class SetEncoderTest : public ::testing::Test {
 public:
  SetEncoderTest() {
  }
 protected:
  typedef typename T::type type_t;

  SeqEncoder<sizeof(type_t)> encoder_;
  SeqDecoder<sizeof(type_t)> decoder_;

  std::vector<type_t> arr_;

  union {
    char buf[2000];
    uint32 ibuf[1];
  } u_;


  static constexpr bool use_sequence = T::use_sequence;
};

TEST(DeltaEncode, Basic) {
  std::vector<uint16_t> v({1, 2, 3, 4, 5, 6, 7, 8, 9});
  unsigned sz = DeltaEncode16(v.data(), v.size(), &v.front());
  EXPECT_EQ(3, sz);
  v.resize(sz);

  EXPECT_THAT(v, ElementsAre(1, 0, 6 << 1 | 1));

  v = {0, 1, 2, 3, 4, 5};
  sz = DeltaEncode16(v.data(), v.size(), &v.front());
  EXPECT_EQ(2, sz);
  v.resize(sz);
  EXPECT_THAT(v, ElementsAre(0, 4 << 1 | 1));
}

TEST(DeltaEncode, Parse) {
  std::vector<uint16_t> source({56,58,59,60,62,64});

  std::vector<uint16_t> dest;
  dest.resize(source.size());

  unsigned sz = DeltaEncode16(source.data(), source.size(), &dest.front());

  EXPECT_EQ(6, sz);
  dest.resize(sz);

  EXPECT_THAT(dest, ElementsAre(56, 1, 0 << 1, 0 << 1, 1 << 1, 1 << 1));
  uint8_t buf[128];
  uint8_t* next = buf;

  for (uint16_t val : dest)
    next += base::flit::Encode32(val, next);

  std::vector<uint16_t> parsed(source.size());
  size_t num_symbols = internal::DeflateFlitAndMap(
      buf, next - buf, [](auto id) { return id; },
      &parsed.front(), parsed.size());

  ASSERT_EQ(num_symbols, parsed.size());
  EXPECT_THAT(parsed, ElementsAreArray(source));
}

TEST(CPP, StrictAliasBug) {
  std::vector<uint64> d(32);

  unique_ptr<uint8_t[]> tmp(new uint8_t[1024]);

  SerializeTo(d, tmp.get() + 6);
}

typedef ::testing::Types<SetEncoderTraits<uint64_t, false>,
                         SetEncoderTraits<uint64_t, true>,
                         SetEncoderTraits<uint32_t, false>,
                         SetEncoderTraits<uint32_t, true>> MyTypes;

TYPED_TEST_CASE(SetEncoderTest, MyTypes);

TYPED_TEST(SetEncoderTest, Basic) {
  this->arr_ = { 1, 2, 3, 6, 7, 8};

  if (!TestFixture::use_sequence) {
    this->encoder_.DisableSeqDictionary();
  }

  for (unsigned i = 0; i < 1; ++i) {
    this->encoder_.Add(this->arr_.data(), this->arr_.size());
  }
  this->encoder_.Flush();

  string dic_enc;
  ASSERT_FALSE(this->encoder_.GetDictSerialized(&dic_enc));

  const auto& cb_vec = this->encoder_.compressed_blocks();
  ASSERT_EQ(1, cb_vec.size());

  uint32_t consumed = 0;
  int res = this->decoder_.Decompress(cb_vec[0], &consumed);
  ASSERT_EQ(0, res);  // finished
  ASSERT_EQ(cb_vec[0].size(), consumed);

  auto page = this->decoder_.GetNextIntPage();

  ASSERT_THAT(page, ElementsAreArray(this->arr_));
  page = this->decoder_.GetNextIntPage();
  EXPECT_TRUE(page.empty());
}

TYPED_TEST(SetEncoderTest, Dict) {
  if (!TestFixture::use_sequence) {
    this->encoder_.DisableSeqDictionary();
  }

  this->arr_ = { 1, 2, 3, 6, 7, 8};
  for (unsigned i = 0; i < 10000; ++i) {
    this->encoder_.Add(this->arr_.data(), this->arr_.size());
  }
  this->encoder_.Flush();

  string dic_enc;
  ASSERT_TRUE(this->encoder_.GetDictSerialized(&dic_enc));

  const auto& cb_vec = this->encoder_.compressed_blocks();
  ASSERT_EQ(1, cb_vec.size());

  this->decoder_.SetDict(reinterpret_cast<const uint8_t*>(dic_enc.data()), dic_enc.size());

  uint32_t consumed = 0;

  int res = this->decoder_.Decompress(cb_vec[0], &consumed);
  ASSERT_EQ(0, res);  // finished
  ASSERT_EQ(cb_vec[0].size(), consumed);

  for (auto page = this->decoder_.GetNextIntPage(); !page.empty();
       page = this->decoder_.GetNextIntPage()) {
    ASSERT_GT(page.size(), 5000);
    unsigned index = 0;
    unsigned seq_size = this->arr_.size();
    ASSERT_EQ(0, page.size() % seq_size);
    for (; index < page.size() / seq_size; ++index) {
      auto seq = typename decltype(this->decoder_)::IntRange(page, index * seq_size, seq_size);

      ASSERT_THAT(seq, WhenSorted(ElementsAreArray(this->arr_)));
    }
  }
}

TYPED_TEST(SetEncoderTest, Fallback) {
  if (!TestFixture::use_sequence) {
    this->encoder_.DisableSeqDictionary();
  }
  std::default_random_engine re;

  for (unsigned i = 0; i < 17000; ++i) {
    std::uniform_int_distribution<int64> dis(0, (1 << 13) - 1);
    this->arr_.clear();
    unsigned start = dis(re);
    for  (unsigned j = start; j < start + 10; ++j) {
      this->arr_.push_back(j);
    }
    this->encoder_.Add(this->arr_.data(), this->arr_.size());
  }

  LOG(ERROR) << "TBD: FAILS HERE";
  return;

  std::uniform_int_distribution<int64> dis(1 << 17, (1 << 20));
  for (unsigned i = 0; i < 1000; ++i) {
    this->arr_.clear();
    for  (unsigned j = 0; j < 10; ++j) {
      this->arr_.push_back(dis(re));
    }
    this->encoder_.Add(this->arr_.data(), this->arr_.size());
  }
  this->encoder_.Flush();

}


TYPED_TEST(SetEncoderTest, FewBlocks) {
  if (!TestFixture::use_sequence) {
    return;
  }

  constexpr uint32_t kOffset = 1 << 20;
  constexpr uint32_t kRange = 12101;
  uint32_t next = 0;
  for (unsigned i = 0; i < 5000; ++i) {
    this->arr_.clear();

    for  (unsigned j = 0; j < 50; ++j) {
      this->arr_.push_back(kOffset + next);
      next = (next + (j + 1)) % kRange;
    }
    this->encoder_.Add(this->arr_.data(), this->arr_.size());
  }

  this->encoder_.Flush();

  string dic_enc;
  ASSERT_TRUE(this->encoder_.GetDictSerialized(&dic_enc));

  const auto& cb_vec = this->encoder_.compressed_blocks();
  ASSERT_GT(cb_vec.size(), 1);

  uint32_t consumed = 0;
  unsigned i = 0;
  for (; i < cb_vec.size() - 1; ++i) {
    int res = this->decoder_.Decompress(cb_vec[i], &consumed);
    ASSERT_EQ(res, 1);
    ASSERT_EQ(cb_vec[i].size(), consumed);
  }

  int res = this->decoder_.Decompress(cb_vec[i], &consumed);
  ASSERT_EQ(res, 0);
  ASSERT_EQ(cb_vec[i].size(), consumed);
}

}  // namespace util
