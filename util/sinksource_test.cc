#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "base/fixed.h"
#include "base/logging.h"

#include "util/zlib_source.h"
#include "util/zstd_sinksource.h"

using std::string;
using std::vector;

namespace util {

using google::protobuf::io::StringOutputStream;

bool is_rep_char(const string& s, char c) {
  for (auto x : s) {
    if (x != c)
      return false;
  }
  return true;
}

using namespace strings;

class SourceTest : public testing::Test {
  void SetUp() {
    for (int i = 0; i < 100000; ++i) {
      coding::AppendFixed32(i, &original_);
    }

    CHECK_EQ(100000 * coding::kFixed32Bytes, original_.size());
    const uint8* read = reinterpret_cast<const uint8*>(original_.data());
    for (int i = 0; i < 100000; ++i) {
      uint32 val = coding::DecodeFixed32(read);
      read += coding::kFixed32Bytes;
      CHECK_EQ(i, val);
    }

    StringSink* ssink = new StringSink;
    ZlibSink zsink(ssink);
    strings::ByteRange br = ToByteRange(original_);
    while (!br.empty()) {
      strings::ByteRange prefix = br.substr(0, 1000);
      br.remove_prefix(1000);
      CHECK_STATUS(zsink.Append(prefix));
    }
    CHECK_STATUS(zsink.Flush());
    compressed_.swap(ssink->contents());
  }

 protected:
  string original_, compressed_;
};

TEST_F(SourceTest, Basic) {
  StringSource* ssource = new StringSource(compressed_);

  EXPECT_TRUE(ZlibSource::IsZlibSource(ssource));
  ZlibSource gsource(ssource);

  const uint8* read = reinterpret_cast<const uint8*>(original_.data());
  size_t compared = 0;

  std::array<uint8, 1215> buf;

  while (compared < original_.size()) {
    auto result = gsource.Read(strings::MutableByteRange(buf));
    ASSERT_TRUE(result.ok()) << result.status << ", " << compared << "/" << original_.size();

    EXPECT_GT(result.obj, 0) << "compared: " << compared;
    int cmp = memcmp(read, buf.data(), result.obj);
    ASSERT_EQ(0, cmp);

    compared += result.obj;
    read += result.obj;
  }
  EXPECT_EQ(original_.size(), compared);
}

TEST_F(SourceTest, MinSize) {
  StringSource* ssource = new StringSource(compressed_);

  ZlibSource gsource(ssource, ZlibSource::AUTO);
  size_t compared = 0;
  const uint8* read = reinterpret_cast<const uint8*>(original_.data());
  std::default_random_engine rd(10);
  std::array<uint8, 1215> buf;

  while (compared < original_.size()) {
    auto result = gsource.Read(strings::MutableByteRange(buf));

    ASSERT_TRUE(result.ok());
    ASSERT_GT(result.obj, 0) << "compared: " << compared;

    size_t new_sz = (rd() % result.obj) + 1;

    int cmp = memcmp(read, buf.begin(), new_sz);
    EXPECT_EQ(0, cmp);

    gsource.Prepend(strings::ByteRange(buf.begin() + new_sz, result.obj - new_sz));

    compared += new_sz;
    read += new_sz;
  }
  EXPECT_EQ(original_.size(), compared);
}

TEST_F(SourceTest, Zlib) {
  string buf;

  StringSink* compressed = new StringSink;
  ZlibSink z_compr(compressed);

  for (unsigned i = 0; i < 1000; ++i) {
    buf.assign(1000, 'a' + (i % 32));
    auto status = z_compr.Append(ToByteRange(buf));
    ASSERT_TRUE(status.ok()) << status;
  }
  ASSERT_TRUE(z_compr.Flush().ok());
  EXPECT_GT(compressed->contents().size(), 8);

  StringSource* src = new StringSource(compressed->contents(), 256);
  ZlibSource z_src(src);

  buf.resize(1000);

  for (unsigned i = 0; i < 1000; ++i) {
    auto result = z_src.Read(AsMutableByteRange(buf));
    ASSERT_TRUE(result.ok()) << result.status;
    ASSERT_EQ(buf.size(), result.obj);
    ASSERT_TRUE(is_rep_char(buf, 'a' + (i % 32))) << i;
  }
}

TEST_F(SourceTest, MultipleStreams) {
  string mult_compr = compressed_ + compressed_;
  StringSource* ssource = new StringSource(mult_compr, 1023);

  ZlibSource gsource(ssource, ZlibSource::AUTO);
  std::array<uint8, 1215> buf;
  size_t read = 0;
  while (true) {
    auto result = gsource.Read(strings::MutableByteRange(buf));
    ASSERT_TRUE(result.ok()) << result.status;
    read += result.obj;
    if (result.obj == 0)
      break;
  }
  EXPECT_EQ(original_.size() * 2, read);
}

class ZstdSourceTest : public testing::Test {};

TEST_F(ZstdSourceTest, Basic) {
  string buf;

  StringSink* compressed = new StringSink;
  ZStdSink zstd_compress(compressed);

  zstd_compress.Init(10);
  for (unsigned i = 0; i < 1000; ++i) {
    buf.assign(1000, 'a' + (i % 32));
    auto status = zstd_compress.Append(ToByteRange(buf));
    ASSERT_TRUE(status.ok()) << status;
  }
  ASSERT_TRUE(zstd_compress.Flush().ok());
  EXPECT_GT(compressed->contents().size(), 8);

  StringSource* src = new StringSource(compressed->contents(), 256);
  ZStdSource zstd_src(src);

  buf.resize(1000);

  for (unsigned i = 0; i < 1000; ++i) {
    auto result = zstd_src.Read(AsMutableByteRange(buf));
    ASSERT_TRUE(result.ok()) << result.status;
    ASSERT_EQ(buf.size(), result.obj);
    ASSERT_TRUE(is_rep_char(buf, 'a' + (i % 32))) << i;
  }
}

}  // namespace util
