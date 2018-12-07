// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sentry/sentry.h"

#include "base/logging.h"
#include "util/http/http_testing.h"

namespace util {

using namespace boost;
using namespace std;

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

class TestGlogClient : public GlogClient {
  unsigned &num_calls_;

 public:
  TestGlogClient(unsigned* num_calls) : num_calls_(*num_calls) {
  }

 protected:
  void HandleItem(const Item &item) {
    if (0 == strcmp(item.base_filename, "sentry_test.cc")) {
      ++num_calls_;
    }
  }
};

}  // namespace

class SentryTest : public util::HttpBaseTest {
 protected:
};

using namespace asio::ip;

TEST_F(SentryTest, Sink) {
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

TEST_F(SentryTest, GlogClient) {
  IoContextPool pool(1);
  pool.Run();

  unsigned num_calls = 0;
  pool.GetNextContext().AttachCancellable(new TestGlogClient(&num_calls));
  for (unsigned i = 0; i < 32; ++i)
    LOG(INFO) << "TEST";

  pool.Stop();
  EXPECT_EQ(32, num_calls);
}

}  // namespace util
