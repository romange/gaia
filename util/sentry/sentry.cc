// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <glog/logging.h>

#include "util/sentry/sentry.h"

namespace util {

class SentrySink final : public ::google::LogSink {
 public:
  SentrySink();
  ~SentrySink();

  void send(google::LogSeverity severity, const char *full_filename, const char *base_filename,
            int line, const struct ::tm *tm_time, const char *message, size_t message_len) override;

  void WaitTillSent() override;

 private:
};

}  // namespace util
