// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <chrono>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "util/asio/io_context_pool.h"

using namespace std::chrono;
using namespace boost;
using namespace asio;
using namespace std::chrono_literals;

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

TEST_F(IoContextTest, Stop) {
  io_context cntx;
  int i = 0;
  auto inc = [&i] {++i;};
  cntx.post(inc);
  EXPECT_EQ(1, cntx.poll_one());
  EXPECT_EQ(1, i);
  cntx.post(inc);
  cntx.stop();
  EXPECT_EQ(0, cntx.poll_one());
  EXPECT_EQ(1, i);
  cntx.restart();
  EXPECT_EQ(1, cntx.poll_one());
  EXPECT_EQ(2, i);
}

TEST_F(IoContextTest, FiberJoin) {
  IoContextPool pool(1);
  pool.Run();
  IoContext& cntx = pool.GetNextContext();

  int i = 0;
  auto cb = [&] {
    ++i;
    EXPECT_TRUE(cntx.InContextThread());
  };
  cntx.PostSynchronous(cb);
  EXPECT_EQ(1, i);

  fibers::fiber fb;
  EXPECT_FALSE(fb.joinable());
  cntx.PostSynchronous([cb, &fb] { fb = fibers::fiber(cb); });
  EXPECT_TRUE(fb.joinable());
  fb.join();
  EXPECT_EQ(2, i);
}

TEST_F(IoContextTest, RunAndStop) {
  IoContextPool pool(1);
  pool.Run();
  pool.Stop();
}

class CancelImpl final : public IoContext::Cancellable {
 bool cancel_ = false;
 bool& finished_;
 public:
  CancelImpl(bool* finished) : finished_(*finished) {}

  void Run() override {
    while (!cancel_) {
      this_fiber::sleep_for(10ms);
    }
    finished_ = true;
  }

  void Cancel() override {
    cancel_ = true;
  }
};

TEST_F(IoContextTest, AttachCancellable) {
  IoContextPool pool(1);
  pool.Run();

  bool cancellable_finished = false;

  pool.GetNextContext().AttachCancellable(new CancelImpl(&cancellable_finished));

  pool.Stop();
  EXPECT_TRUE(cancellable_finished);
}

static void BM_RunOneNoLock(benchmark::State& state) {
  io_context cntx(1);   // no locking

  ip::tcp::endpoint endpoint(ip::tcp::v4(), 0);
  std::vector<std::unique_ptr<ip::tcp::acceptor>> acc_arr(state.range(0));
  std::vector<std::unique_ptr<steady_timer>> timer_arr(state.range(0));
  for (auto& a : acc_arr) {
    a.reset(new ip::tcp::acceptor(cntx, endpoint));
    a->async_accept([](auto ec, boost::asio::ip::tcp::socket s) {});
  }

  for (auto& a : timer_arr) {
    a.reset(new steady_timer(cntx));
    a->expires_after(2s);
    a->async_wait([](auto ec) {});
  }

  int i = 0;
  while (state.KeepRunning()) {
    cntx.post([&i] { ++i; });
    cntx.run_one();
  }
}
BENCHMARK(BM_RunOneNoLock)->Range(1, 64);

}  // namespace util
