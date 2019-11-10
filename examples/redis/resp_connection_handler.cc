// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/resp_connection_handler.h"

#include "absl/strings/string_view.h"
#include "base/logging.h"

namespace redis {
using namespace util;
using namespace boost;

static uint8_t* ParseLine(uint8_t* buf, const uint8_t* end, absl::string_view* res) {
  uint8_t* start = buf;

  while (start < end) {
    size_t sz = end - start;
    uint8_t* next = reinterpret_cast<uint8_t*>(memchr(start, '\r', sz));
    if (!next)
      break;

    if (next[1] == '\n') {
      next[0] = '\0';
      *res = absl::string_view{reinterpret_cast<char*>(buf), size_t(next - buf)};
      return next + 2;
    }
    start = next + 1;
  }
  return nullptr;
}

RespConnectionHandler::RespConnectionHandler(util::IoContext* context)
    : ConnectionHandler(context) {}

system::error_code RespConnectionHandler::HandleRequest() {
  system::error_code ec;

  constexpr uint16_t kBufSz = 256;
  uint8 buf[kBufSz + 2];
  buf[256] = '\r';
  buf[257] = '\n';

  size_t sz = socket_->read_some(asio::buffer(buf, kBufSz), ec);
  if (ec)
    return ec;

  uint8_t* next = buf;
  uint8_t* eob = buf + sz;
  while (true) {
    absl::string_view line;
    next = ParseLine(next, eob, &line);
    if (!next)
      break;

    LOG(INFO) << "Got: " << line;
  }
  return system::error_code{};
}

ConnectionHandler* RespListener::NewConnection(util::IoContext& context) {
  return new RespConnectionHandler(&context);
}

}  // namespace redis
