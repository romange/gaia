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

using fibers_ext::short_id;

namespace {

class RRAlgo : public fibers::algo::algorithm {
 private:
  typedef fibers::scheduler::ready_queue_type rqueue_type;

  rqueue_type rqueue_{};
  std::mutex mtx_{};
  std::condition_variable cnd_{};
  bool flag_{false};

 public:
  RRAlgo() = default;

  RRAlgo(RRAlgo const&) = delete;
  RRAlgo& operator=(RRAlgo const&) = delete;

  void awakened(fibers::context*) noexcept final;

  fibers::context* pick_next() noexcept final;

  bool has_ready_fibers() const noexcept final { return !rqueue_.empty(); }

  void suspend_until(std::chrono::steady_clock::time_point const&) noexcept final;

  void notify() noexcept final;
};

void RRAlgo::awakened(fibers::context* ctx) noexcept {
  DVLOG(1) << "Ready " << short_id(ctx);
  ctx->ready_link(rqueue_);
}

fibers::context* RRAlgo::pick_next() noexcept {
  fibers::context* victim = nullptr;
  if (!rqueue_.empty()) {
    victim = &rqueue_.front();
    rqueue_.pop_front();
    DVLOG(1) << "pick_next " << short_id(victim);
  }
  return victim;
}

void RRAlgo::suspend_until(std::chrono::steady_clock::time_point const& time_point) noexcept {
  if ((std::chrono::steady_clock::time_point::max)() == time_point) {
    std::unique_lock<std::mutex> lk{mtx_};
    cnd_.wait(lk, [&]() { return flag_; });
    flag_ = false;
  } else {
    DVLOG(1) << "wait_until " << time_point.time_since_epoch().count();

    std::unique_lock<std::mutex> lk{mtx_};
    cnd_.wait_until(lk, time_point, [&]() { return flag_; });
    flag_ = false;
  }
}

void RRAlgo::notify() noexcept {
  std::unique_lock<std::mutex> lk{mtx_};
  flag_ = true;
  lk.unlock();
  cnd_.notify_all();
}

class TestSink final : public ::google::LogSink {
 public:
  TestSink() {}

  void send(google::LogSeverity severity, const char* full_filename, const char* base_filename,
            int line, const struct ::tm* tm_time, const char* message, size_t message_len) override;

  void WaitTillSent() override;

  unsigned sends = 0;
  unsigned waits = 0;

 private:
};

void TestSink::send(google::LogSeverity severity, const char* full_filename,
                    const char* base_filename, int line, const struct ::tm* tm_time,
                    const char* message, size_t message_len) {
  // cout << full_filename << "/" << StringPiece(message, message_len) << ": " << severity << endl;
  ++sends;
}

void TestSink::WaitTillSent() { ++waits; }

class TestGlogClient : public GlogAsioSink {
  unsigned& num_calls_;

 public:
  TestGlogClient(unsigned* num_calls) : num_calls_(*num_calls) {}

 protected:
  void HandleItem(const Item& item) override {
    if (0 == strcmp(item.base_filename, _TEST_BASE_FILE_)) {
      ++num_calls_;
    }
  }
};

class CancelImpl final : public IoContext::Cancellable {
  bool cancel_ = false;
  bool& finished_;
  fibers::fiber fb_;

 public:
  CancelImpl(bool* finished) : finished_(*finished) {}

  ~CancelImpl() { CHECK(finished_); }

  void Run() override {
    fb_ = fibers::fiber([this] {
      while (!cancel_) {
        VLOG(1) << "Before sleep_for infiber";
        this_fiber::sleep_for(10ms);
      }
    });

    while (!cancel_) {
      VLOG(1) << "Before sleep_for";
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

  void TearDown() { pool_->Stop(); }
  std::unique_ptr<IoContextPool> pool_;
};

TEST_F(IoContextTest, Basic) {
  io_context cntx(1);  // no locking
  int i = 0;
  asio::post(cntx, [&i] { ++i; });
  EXPECT_EQ(0, i);
  EXPECT_EQ(1, cntx.run_one());
  EXPECT_EQ(1, i);
  EXPECT_EQ(0, cntx.run_one());
}

TEST_F(IoContextTest, Stop) {
  io_context cntx;
  int i = 0;
  auto inc = [&i] { ++i; };
  asio::post(cntx, inc);
  EXPECT_EQ(1, cntx.poll_one());
  EXPECT_EQ(1, i);
  asio::post(cntx, inc);
  cntx.stop();
  EXPECT_EQ(0, cntx.poll_one());
  EXPECT_EQ(1, i);
  cntx.restart();
  EXPECT_EQ(1, cntx.poll_one());
  EXPECT_EQ(2, i);
}

TEST_F(IoContextTest, FiberJoin) {
  IoContext& cntx = pool_->GetNextContext();

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

  int i = cntx.Await([] { return 5; });
  EXPECT_EQ(5, i);

  i = cntx.AwaitSafe([&] { return i + 5; });
  EXPECT_EQ(10, i);
}

TEST_F(IoContextTest, RunAndStop) {}

TEST_F(IoContextTest, RunAndStopFromContext) {
  IoContext& cntx = pool_->GetNextContext();
  cntx.AwaitSafe([this] { pool_->Stop(); });
}

TEST_F(IoContextTest, AttachCancellableStopFromMain) {
  bool cancellable_finished = false;
  IoContext& cntx = pool_->GetNextContext();
  cntx.AttachCancellable(new CancelImpl(&cancellable_finished));
  VLOG(1) << "After AttachCancellable";

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
    ec.await([&] { return val > 0; });
    t1.join();
  });
}

TEST_F(IoContextTest, AwaitOnAll) {
  IoContextPool pool(4);
  pool.Run();
  constexpr unsigned kSz = 50;

  std::thread ts[kSz];
  for (unsigned i = 0; i < kSz; ++i) {
    ts[i] = std::thread([&] { pool.AwaitFiberOnAll([](IoContext&) {}); });
  }
  for (unsigned i = 0; i < kSz; ++i) {
    ts[i].join();
  }
}

TEST_F(IoContextTest, PlainFiberYield) {
  fibers::use_scheduling_algorithm<RRAlgo>();
  bool stop = false;
  fibers::fiber fb_yied{[&] {
    while (!stop)
      this_fiber::yield();
  }};
  fibers::fiber fb_sleep{[&] {
    VLOG(1) << "Before Sleep " << short_id();
    this_fiber::sleep_for(20ms);
    stop = true;
  }};
  fb_yied.join();
  fb_sleep.join();
}

TEST_F(IoContextTest, YieldInIO) {
  IoContext& cntx = pool_->GetNextContext();
  constexpr unsigned kSz = 15;
  fibers::fiber fbs[kSz];
  fibers::fiber sl;
  std::atomic_bool cancel{false};
  auto cb = [&](int i) {
    while (!cancel) {
      VLOG(1) << "Yield " << short_id() << " " << i;
      this_fiber::yield();
    }
  };

  for (unsigned i = 0; i < kSz; ++i) {
    fbs[i] = cntx.LaunchFiber(cb, i);
  }
  sl = cntx.LaunchFiber([] {
    VLOG(1) << "Sleep " << short_id();
    this_fiber::sleep_for(10ms);
  });
  cancel = true;
  sl.join();
  for (unsigned i = 0; i < kSz; ++i) {
    fbs[i].join();
  }
}

TEST_F(IoContextTest, FiberWaits) {
  IoContext& cntx = pool_->GetNextContext();
  fibers_ext::Done done;
  auto fb = cntx.LaunchFiber([&] { done.Wait(); });
  this_fiber::sleep_for(10ms);
  done.Notify();
  fb.join();
}

static void BM_RunOneNoLock(benchmark::State& state) {
  io_context cntx(1);  // no locking

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
    asio::post(cntx, [&i] { ++i; });
    cntx.run_one();
  }
}
BENCHMARK(BM_RunOneNoLock)->Range(1, 64);

static void BM_IOFiberYield(benchmark::State& state) {
  IoContextPool pool(1);
  pool.Run();
  IoContext& cntx = pool.GetNextContext();
  bool cancel = false;
  size_t items1{0}, items2{0};

  fibers::fiber fb1;
  fibers::fiber fb2;
  cntx.Await([&] {
    fb1 = fibers::fiber([&] {
      while (!cancel) {
        ++items1;
        this_fiber::yield();
      }
    });

    fb2 = fibers::fiber([&] {
      while (state.KeepRunning()) {
        ++items2;
        this_fiber::yield();
      }
      cancel = true;
    });
  });
  LOG(INFO) << "After launching fibers";

  fb1.join();
  fb2.join();
  LOG(INFO) << "Processed " << items1 << "/" << items2;

  state.SetItemsProcessed(items1 + items2);
}  // namespace util
BENCHMARK(BM_IOFiberYield);

}  // namespace util
