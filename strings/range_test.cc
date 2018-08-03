#include <array>
#include <gtest/gtest.h>

#include "base/integral_types.h"
#include "base/logging.h"

#include "strings/stringpiece.h"
#include "strings/split.h"

using namespace std;

namespace strings {

class StringPieceTest : public testing::Test {
};

TEST_F(StringPieceTest, Length) {
  StringPiece pc("Fooo");
  EXPECT_EQ(4, pc.size());

  std::array<uint8, 4> array{{1, 3, 4, 8}};
  strings::ByteRange slice(array);
  EXPECT_EQ(4, slice.size());
  EXPECT_EQ(3, slice[1]);
  EXPECT_EQ(3, pc.rfind('o'));
}

TEST_F(StringPieceTest, Range) {
  std::array<int32, 4> arr{{1, 2, 3, 4}};
  std::vector<int32> v(5);
  Range<int32*> r1(arr);
  EXPECT_EQ(arr.size(), r1.size());

  Range<const int32*> r2(v);
  EXPECT_EQ(Range<const int32*>(v), r2);
}

}  // namespace strings
