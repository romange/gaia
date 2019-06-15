// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/hash.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "file/filesource.h"
#include "strings/strip.h"

using namespace std;

namespace base {

static std::vector<std::string> ReadIds() {
  string filename("testdata/ids.txt.gz");
  if (std::getenv("BAZEL")) {
    filename.assign("gaia/base/" + filename);
  }

  file::LineReader line_reader(base::ProgramRunfile(filename));
  decltype(ReadIds()) res;

  StringPiece line;
  while (line_reader.Next(&line)) {
    line = absl::StripAsciiWhitespace(line);
    res.push_back(strings::AsString(line));
    CHECK(!line.empty());
  }
  return res;
}

class HashTest : public testing::Test {
 protected:
};

TEST_F(HashTest, Basic) {
  auto ids = ReadIds();
  ASSERT_GT(ids.size(), 10);
}

static void BM_MurMur(benchmark::State& state) {
  auto ids = ReadIds();
  uint32 i = 0;
  while (state.KeepRunning()) {
    int j = i++ % ids.size();
    const auto* val = reinterpret_cast<const uint8*>(ids[j].data());
    sink_result(base::MurmurHash3_x86_32(val, ids[j].size(), i));
  }
}
BENCHMARK(BM_MurMur);

}  // namespace base
