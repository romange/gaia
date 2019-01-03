// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <chrono>

#include "base/gtest.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "util/asio/glog_asio_sink.h"
#include "util/asio/io_context_pool.h"

using namespace std::chrono;
using namespace boost;
using namespace asio;
using namespace std::chrono_literals;

namespace util {

namespace {

class TestSink final : public ::google::LogSink {
 public:
  TestSink() {
  }

  void send(google::LogSeverity severity, const char *full_filename, const char *base_filename,
            int line, const struct ::tm *tm_time, const char *message, size_t message_len) override;

  void WaitTillSent() override;

  unsigned sends = 0;
  unsigned waits = 0;

 private:
};

void TestSink::send(google::LogSeverity severity, const char *full_filename,
                    const char *base_filename, int line, const struct ::tm *tm_time,
                    const char *message, size_t message_len) {
  // cout << full_filename << "/" << StringPiece(message, message_len) << ": " << severity << endl;
  ++sends;
}

void TestSink::WaitTillSent() {
  ++waits;
}

class TestGlogClient : public GlogAsioSink {
  unsigned &num_calls_;

 public:
  TestGlogClient(unsigned *num_calls) : num_calls_(*num_calls) {
  }

 protected:
  void HandleItem(const Item &item) override {
    if (0 == strcmp(item.base_filename, _TEST_BASE_FILE_)) {
      ++num_calls_;
    }
  }
};

}  // namespace

class IoContextTest : public testing::Test {
 protected:
  void SetUp() override {
  }

  void TearDown() {
  }
};

TEST_F(IoContextTest, Basic) {
  io_context cntx(1);  // no locking
  int i = 0;
  cntx.post([&i] { ++i; });
  EXPECT_EQ(0, i);
  EXPECT_EQ(1, cntx.run_one());
  EXPECT_EQ(1, i);
  EXPECT_EQ(0, cntx.run_one());
}

TEST_F(IoContextTest, Stop) {
  io_context cntx;
  int i = 0;
  auto inc = [&i] { ++i; };
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
  IoContext &cntx = pool.GetNextContext();

  int i = 0;
  auto cb = [&] {
    ++i;
    EXPECT_TRUE(cntx.InContextThread());
  };
  cntx.Await(cb);
  EXPECT_EQ(1, i);

  fibers::fiber fb;
  EXPECT_FALSE(fb.joinable());
  fb = cntx.LaunchFiber(cb);
  EXPECT_TRUE(fb.joinable());
  fb.join();
  EXPECT_EQ(2, i);
}

TEST_F(IoContextTest, Results) {
  IoContextPool pool(1);
  pool.Run();
  IoContext& cntx = pool.GetNextContext();

  int i = cntx.Await([] { return 5;});
  EXPECT_EQ(5, i);

  i = cntx.AwaitSafe([&] { return i + 5;});
  EXPECT_EQ(10, i);
}

TEST_F(IoContextTest, RunAndStop) {
  IoContextPool pool(1);
  pool.Run();
  pool.Stop();
}

class CancelImpl final : public IoContext::Cancellable {
  bool cancel_ = false;
  bool &finished_;

 public:
  CancelImpl(bool *finished) : finished_(*finished) {
  }

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

TEST_F(IoContextTest, Sink) {
  TestSink sink;
  google::AddLogSink(&sink);

  for (unsigned i = 0; i < 100; ++i) {
    LOG(INFO) << "Foo";
  }

  EXPECT_EQ(100, sink.sends);
  EXPECT_EQ(100, sink.waits);

  google::RemoveLogSink(&sink);

  for (unsigned i = 0; i < 100; ++i) {
    LOG(INFO) << "Foo";
  }
  EXPECT_EQ(100, sink.sends);
  EXPECT_EQ(100, sink.waits);
}

TEST_F(IoContextTest, Glog) {
  IoContextPool pool(1);
  pool.Run();

  unsigned num_calls = 0;
  pool.GetNextContext().AttachCancellable(new TestGlogClient(&num_calls));
  for (unsigned i = 0; i < 32; ++i) {
    LOG(INFO) << "TEST";
  }
  for (int i = 0; num_calls < 32 && i < 5; ++i) {
    SleepForMilliseconds(10);
  }
  pool.Stop();
  EXPECT_GE(32, num_calls);
}

static void BM_RunOneNoLock(benchmark::State &state) {
  io_context cntx(1);  // no locking

  ip::tcp::endpoint endpoint(ip::tcp::v4(), 0);
  std::vector<std::unique_ptr<ip::tcp::acceptor>> acc_arr(state.range(0));
  std::vector<std::unique_ptr<steady_timer>> timer_arr(state.range(0));
  for (auto &a : acc_arr) {
    a.reset(new ip::tcp::acceptor(cntx, endpoint));
    a->async_accept([](auto ec, boost::asio::ip::tcp::socket s) {});
  }

  for (auto &a : timer_arr) {
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
