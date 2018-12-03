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
  TestSink() {}

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
}

}  // namespace util
