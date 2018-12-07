// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/glog_asio_sink.h"

namespace util {
using namespace ::boost;
using namespace ::std;

using namespace fibers;

GlogAsioSink::GlogAsioSink() : msg_q_(64) {
}

GlogAsioSink::~GlogAsioSink() {
}

void GlogAsioSink::Run() {
  google::AddLogSink(this);

  Item item;
  while (true) {
    channel_op_status st = msg_q_.pop(item);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    HandleItem(item);
  }

  LOG_IF(INFO, lost_messages_ > 0) << "GlogAsioSink lost " << lost_messages_ << " lost messages ";
}

void GlogAsioSink::Cancel() {
  google::RemoveLogSink(this);
  msg_q_.close();
}

void GlogAsioSink::send(google::LogSeverity severity, const char* full_filename,
                      const char* base_filename, int line, const struct ::tm* tm_time,
                      const char* message, size_t message_len) {
  if (ShouldIgnore(severity, full_filename, line))
    return;

  // string creation might have potential performance impact.
  channel_op_status st = msg_q_.push_wait_for(
      Item{full_filename, base_filename, severity, line, *tm_time, string{message, message_len}},
      100us);

  if (st != channel_op_status::success) {
    // str_pool_.Release(str);
    ++lost_messages_;
  }
}

void GlogAsioSink::WaitTillSent() {
  /* Noop to reduce send latency */
}

}  // namespace util
