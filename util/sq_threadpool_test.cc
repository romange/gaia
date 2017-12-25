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
  // int fd = open("/dev/random", O_RDONLY, 0400);
  // CHECK_GT(fd, 0);
}

TEST_F(SQThreadPoolTest, Promise) {
  fibers::future<void> f;
  {
    fibers::promise<void> p;
    f = p.get_future();
  }
  fibers::promise<StatusObject<int>> p2;
  p2.set_value(5);
  fibers::promise<StatusObject<int>> p3(std::move(p2));
  StatusObject<int> res = p3.get_future().get();
  EXPECT_EQ(5, res.obj);
}

}  // namespace util