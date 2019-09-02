// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/glog_asio_sink.h"
#include <glog/raw_logging.h>

namespace util {
using namespace ::boost;
using namespace ::std;

using namespace fibers;

GlogAsioSink::GlogAsioSink() : msg_q_(64) {
}

GlogAsioSink::~GlogAsioSink() noexcept {
}

void GlogAsioSink::Run() {
  google::AddLogSink(this);
  RAW_DLOG(INFO, "Started running");
  run_started_.store(true, std::memory_order_seq_cst);
  ec_.notifyAll();

  Item item;
  while (true) {
    channel_op_status st = msg_q_.pop(item);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    // RAW_VLOG(1, "HandleItem : %s:%d", item.base_filename, item.line);
    HandleItem(item);
  }

  LOG_IF(INFO, lost_messages_ > 0) << "GlogAsioSink lost " << lost_messages_ << " lost messages ";
}

void GlogAsioSink::Cancel() {
  google::RemoveLogSink(this);
  msg_q_.close();
}

void GlogAsioSink::WaitTillRun() {
  ec_.await([this] { return run_started_.load(std::memory_order_acquire); });
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
    ++lost_messages_;
  }
  RAW_VLOG(1, "GlogAsioSink::SendExit : %d", int(st));
}

void GlogAsioSink::WaitTillSent() {
  /* Noop to reduce send latency */
}

}  // namespace util
