// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <glog/logging.h>
#include <boost/fiber/buffered_channel.hpp>

#include "util/asio/io_context.h"

namespace util {

class GlogClient : public IoContext::Cancellable, ::google::LogSink {
 public:
  GlogClient();
  ~GlogClient();

  void Run() override;
  void Cancel() override;

 protected:
  struct Item {
    const char *full_filename;
    const char *base_filename;
    google::LogSeverity severity;
    int line;
    struct ::tm tm_time;
    std::string message;  // Can cause performance penalty.
  };
  ::boost::fibers::buffered_channel<Item> msg_q_;

  unsigned lost_messages_ = 0;

  virtual bool ShouldIgnore(google::LogSeverity severity, const char *full_filename, int line) {
    return false;
  }

  // Is called from Run loop. Should not block the thread.
  virtual void HandleItem(const Item& item) = 0;

 private:
  void send(google::LogSeverity severity, const char *full_filename, const char *base_filename,
            int line, const struct ::tm *tm_time, const char *message, size_t message_len) override;

  void WaitTillSent() override;
};

}  // namespace util
