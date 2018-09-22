// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"

using namespace std::chrono;
using namespace boost;
using namespace asio;

namespace util {

class IoContextTest : public testing::Test {
 protected:
  void SetUp() override {
  }

  void TearDown() {
  }
};

TEST_F(IoContextTest, Basic) {
  io_context cntx(1);   // no locking
  int i = 0;
  cntx.post([&i] {++i;});
  EXPECT_EQ(0, i);
  EXPECT_EQ(1, cntx.run_one());
  EXPECT_EQ(1, i);
  EXPECT_EQ(0, cntx.run_one());
}

static void BM_RunOne(benchmark::State& state) {
  io_context cntx(state.range_x());   // no locking
  ip::tcp::endpoint endpoint(ip::tcp::v4(), 0);
  ip::tcp::acceptor acceptor(cntx);
  acceptor.async_accept([](auto ec, boost::asio::ip::tcp::socket s) {});
  int i = 0;
  while (state.KeepRunning()) {
    cntx.post([&i] { ++i; });
    cntx.run_one();
  }
}
BENCHMARK(BM_RunOne)->Arg(0)->Arg(1);

}  // namespace util
