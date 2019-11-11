// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/resp_connection_handler.h"

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "base/logging.h"

namespace redis {
using namespace util;
using namespace boost;

namespace {
uint8_t* ParseLine(uint8_t* buf, const uint8_t* end, absl::string_view* res) {
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

class StaticInstr {
 public:
  StaticInstr() : read_start_(buf_.begin()), read_eob_(buf_.begin()) {
    buf_[kBufSz] = '\r';
    buf_[kBufSz + 1] = '\n';
  }

  system::error_code Parse(FiberSyncSocket* socket, absl::string_view* line);

  asio::mutable_buffer GetRemainder() const {
    return asio::mutable_buffer(read_start_, read_eob_ - read_start_);
  }

 private:
  static constexpr uint16_t kBufSz = 256;
  std::array<uint8_t, kBufSz + 4> buf_;
  uint8_t *read_start_, *read_eob_;
};

system::error_code StaticInstr::Parse(FiberSyncSocket* socket, absl::string_view* line) {
  uint8_t* eob = buf_.begin() + kBufSz;

  system::error_code ec;
  while (read_start_ < eob) {
    size_t read_sz = socket->read_some(asio::mutable_buffer(read_start_, eob - read_start_), ec);
    if (ec)
      return ec;
    read_eob_ = read_start_ + read_sz;
    read_start_ = ParseLine(read_start_, read_eob_, line);

    if (read_start_) {
      char* bb = reinterpret_cast<char*>(buf_.begin());
      *line = absl::StripAsciiWhitespace(absl::string_view{bb, size_t(line->end() - bb)});
      if (!line->empty())
        return system::error_code{};

      if (read_start_ != read_eob_) {
        read_sz = read_eob_ - read_start_;
        memmove(buf_.begin(), read_start_, read_sz);
        read_eob_ = buf_.begin() + read_sz;
      }
    }

    read_start_ = read_eob_;
  }

  return system::errc::make_error_code(system::errc::no_buffer_space);
}

}  // namespace

RespConnectionHandler::RespConnectionHandler(util::IoContext* context)
    : ConnectionHandler(context) {}

/*
   Example commands: echo -e '*1\r\n$4\r\PING\r\n', echo -e 'PING\r\n',
   echo -e ' \r\n*1\r\n$7\r\nCOMMAND\r\n'
*/
system::error_code RespConnectionHandler::HandleRequest() {
  absl::string_view cmd;
  StaticInstr parser;

  system::error_code ec = parser.Parse(&socket_.value(), &cmd);

  if (ec)
    return ec;
  return HandleCmd(cmd, parser.GetRemainder());
}

system::error_code RespConnectionHandler::HandleCmd(absl::string_view cmd,
                                                    asio::mutable_buffer mb) {
  if (cmd[0] == '*') {
    cmd.remove_prefix(1);
    unsigned cnt;
    if (!absl::SimpleAtoi(cmd, &cnt))
      return system::errc::make_error_code(system::errc::invalid_argument);
  }
  return system::error_code{};
}

ConnectionHandler* RespListener::NewConnection(util::IoContext& context) {
  return new RespConnectionHandler(&context);
}

}  // namespace redis
