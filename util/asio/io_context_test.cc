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

class CancelImpl final : public IoContext::Cancellable {
  bool cancel_ = false;
  bool &finished_;
  fibers::fiber fb_;

 public:
  CancelImpl(bool *finished) : finished_(*finished) {
  }

  ~CancelImpl() {
    CHECK(finished_);
  }

  void Run() override {
    fb_ = fibers::fiber([this] {
      while (!cancel_) {
        this_fiber::sleep_for(10ms);
      }
    });

    while (!cancel_) {
      this_fiber::sleep_for(1ms);
    }
    finished_ = true;
  }

  void Cancel() override {
    cancel_ = true;

    fb_.join();
  }
};

}  // namespace

class IoContextTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::make_unique<IoContextPool>(1);
    pool_->Run();
  }

  void TearDown() {
    pool_->Stop();
  }
  std::unique_ptr<IoContextPool> pool_;
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
  IoContext &cntx = pool_->GetNextContext();

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
  IoContext& cntx = pool_->GetNextContext();

  int i = cntx.Await([] { return 5;});
  EXPECT_EQ(5, i);

  i = cntx.AwaitSafe([&] { return i + 5;});
  EXPECT_EQ(10, i);
}

TEST_F(IoContextTest, RunAndStop) {
}

TEST_F(IoContextTest, RunAndStopFromContext) {
  IoContext& cntx = pool_->GetNextContext();
  cntx.AwaitSafe([this] { pool_->Stop(); });
}

TEST_F(IoContextTest, AttachCancellableStopFromMain) {
  bool cancellable_finished = false;
  IoContext& cntx = pool_->GetNextContext();
  cntx.AttachCancellable(new CancelImpl(&cancellable_finished));

  pool_->Stop();
  EXPECT_TRUE(cancellable_finished);
}

TEST_F(IoContextTest, AttachCancellableStopFromContext) {
  bool cancellable_finished = false;
  IoContext& cntx = pool_->GetNextContext();
  cntx.AttachCancellable(new CancelImpl(&cancellable_finished));

  cntx.AwaitSafe([this] { pool_->Stop(); });
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
  unsigned num_calls = 0;
  pool_->GetNextContext().AttachCancellable(new TestGlogClient(&num_calls));
  for (unsigned i = 0; i < 32; ++i) {
    LOG(INFO) << "TEST";
  }
  for (int i = 0; num_calls < 32 && i < 5; ++i) {
    SleepForMilliseconds(10);
  }
  pool_->Stop();
  EXPECT_GE(32, num_calls);
}

TEST_F(IoContextTest, EventCount) {
  std::atomic_long val{0};
  fibers_ext::EventCount ec;

  auto cb = [&] {
    SleepForMilliseconds(200);
    val = 1;
    ec.notify();
  };

  pool_->GetNextContext().AwaitSafe([&] {
    std::thread t1(cb);
    ec.await([&] { return val > 0;});
    t1.join();
  });
}

TEST_F(IoContextTest, AwaitOnAll) {
  IoContextPool pool(4);
  pool.Run();
  constexpr unsigned kSz = 50;

  std::thread ts[kSz];
  for (unsigned i = 0; i < kSz; ++i) {
    ts[i] = std::thread([&] {
      pool.AwaitFiberOnAll([] (IoContext&) {});
    });
  }
  for (unsigned i = 0; i < kSz; ++i) {
    ts[i].join();
  }
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
