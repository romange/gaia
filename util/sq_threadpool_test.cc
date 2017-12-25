// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/sq_threadpool.h"

#include "base/gtest.h"
#include "base/logging.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace util {

class SQThreadPoolTest : public testing::Test {
protected:
  static void SetUpTestCase() {
  }
};

using namespace boost;

namespace {

}  // namespace

TEST_F(SQThreadPoolTest, Read) {
  int fd = open(base::kProgramName, O_RDONLY, 0400);
  CHECK_GT(fd, 0);
  FileIOManager mngr(1);
  std::array<uint8_t[16], 16> a;

  std::vector<FileIOManager::ReadResult> reads;
  for (unsigned i = 0; i < a.size(); ++i) {
    reads.push_back(mngr.Read(fd, i * 16, strings::MutableByteRange(a[i], sizeof(a[0]))));
  }

  for (unsigned i = 0; i < reads.size(); ++i) {
    reads[i].get();
  }
}

}  // namespace util
