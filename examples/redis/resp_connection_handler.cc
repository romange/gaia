// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/resp_connection_handler.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

#include "base/logging.h"
#include "examples/redis/redis_command.h"
#include "examples/redis/resp_parser.h"

#define VLOG_CONN(level) VLOG(level) << "[" << conn_id_ << "] "

namespace redis {
using namespace util;
using namespace boost;
using std::string;
namespace errc = system::errc;

namespace {
constexpr uint32_t bitmask(CommandFlags fl) {
  return static_cast<uint32_t>(fl);
}

template <typename... Flags> constexpr uint32_t bitmask(CommandFlags left, Flags... fl) {
  return static_cast<uint32_t>(left) | bitmask(fl...);
}

}  // namespace

RespConnectionHandler::RespConnectionHandler(const std::vector<Command>& commands,
                                             util::IoContext* context)
    : ConnectionHandler(context), commands_(commands) {
  use_flusher_fiber_ = true;
}

void RespConnectionHandler::OnOpenSocket() {
  conn_id_ = socket_->native_handle();
}

/*
   *num tokens in the command,token1, token2 ....
   Example commands: echo -e '*1\r\n$4\r\PING\r\n', echo -e 'PING\r\n',
   echo -e ' \r\n*1\r\n$7\r\nCOMMAND\r\n'
   { echo -e '*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n';  } | nc  localhost 6379
*/
system::error_code RespConnectionHandler::HandleRequest() {
  RespParser parser;

  IoState io_state = IoState::READ_EOL;
  cmd_state_ = CmdState::INIT;

  // states_.push_back(CmdState::INIT);
  VLOG(1) << "RespConnectionHandler::HandleRequest";

  while (true) {
    if (cmd_state_ == CmdState::INIT) {
      if (!args_.empty()) {
        CHECK_EQ(args_.size(), num_args_);
        HandleCommand();
        if (req_ec_)
          return req_ec_;
        args_.clear();
        if (parser.IsReadEof())
          break;
      }
      num_args_ = 1;
    }
    req_ec_ = HandleIoState(&parser, &io_state);
    if (req_ec_)
      break;
  }

  VLOG(1) << "Finished " << req_ec_;

  return req_ec_;
}

system::error_code RespConnectionHandler::HandleIoState(RespParser* parser, IoState* state) {
  absl::string_view cmd;

  system::error_code ec;
  while (true) {
    switch (*state) {
      case IoState::READ_EOL: {
        if (parser->ParseNext(&cmd) == RespParser::LINE_FINISHED) {
          *state = IoState::HANDLE_STRING;
          break;
        }

        auto dest_buf = parser->GetDestBuf();
        if (dest_buf.size() < 2) {
          LOG(ERROR) << "No write space and read buf is " << parser->ReadBuf().size();
          return system::errc::make_error_code(system::errc::no_buffer_space);
        }

        size_t read_sz =
            socket_->read_some(asio::mutable_buffer{dest_buf.data(), dest_buf.size()}, ec);
        if (ec)
          return ec;
        parser->WriteCommit(read_sz);

        break;
      }
      case IoState::READ_N:
        CHECK(parser->IsReadEof());
        asio::read(*socket_, bulk_str_, ec);
        if (ec)
          return ec;
        *state = IoState::READ_EOL;
        break;

      case IoState::HANDLE_STRING: {
        ErrorState es = HandleNextString(cmd, parser);
        system::error_code* ec2 = absl::get_if<system::error_code>(&es);
        if (ec2)
          return *ec2;
        parser->ConsumeLine();

        *state = absl::get<IoState>(es);
        return ec;
      }
    }
  }
}

auto RespConnectionHandler::HandleNextString(absl::string_view blob, RespParser* parser)
    -> ErrorState {
  VLOG_CONN(1) << "Blob: |" << blob << "|";
  namespace errc = system::errc;

  switch (cmd_state_) {
    case CmdState::INIT:
      cmd_state_ = CmdState::ARG_START;
      if (blob[0] == '*') {
        blob.remove_prefix(1);
        if (!absl::SimpleAtoi(blob, &num_args_))
          return errc::make_error_code(errc::invalid_argument);
        return IoState::READ_EOL;
      }
      ABSL_FALLTHROUGH_INTENDED;
    case CmdState::ARG_START:
      if (blob[0] == '$') {
        blob.remove_prefix(1);
        if (!absl::SimpleAtoi(blob, &bulk_size_))
          return errc::make_error_code(errc::invalid_argument);
        cmd_state_ = CmdState::EMPTY_EXPECTED;

        RespParser::Buffer buf = parser->ReadBuf();
        line_buffer_.resize(bulk_size_);
        char* ptr = &line_buffer_.front();

        if (bulk_size_ <= buf.size()) {
          memcpy(ptr, buf.data(), bulk_size_);
          parser->Consume(bulk_size_);

          return IoState::READ_EOL;
        }
        memcpy(ptr, buf.data(), buf.size());
        parser->Reset();
        bulk_str_ = asio::mutable_buffer{ptr + buf.size(), bulk_size_ - buf.size()};
        VLOG_CONN(1) << "Bulk string of size " << bulk_size_ << "/" << bulk_str_.size();

        return IoState::READ_N;
      }
      line_buffer_ = string{blob};
      blob = absl::string_view{};
      ABSL_FALLTHROUGH_INTENDED;
    case CmdState::EMPTY_EXPECTED:
      if (!blob.empty())
        return errc::make_error_code(errc::illegal_byte_sequence);
      args_.push_back(std::move(line_buffer_));
      VLOG_CONN(1) << "Pushed " << args_.back();
      cmd_state_ = args_.size() < num_args_ ? CmdState::ARG_START : CmdState::INIT;

      return IoState::READ_EOL;
      break;
    default:
      LOG(FATAL) << "BUG";
  }

  return system::error_code{};
}

void RespConnectionHandler::HandleCommand() {
  CHECK_EQ(num_args_, args_.size());
  CHECK_GT(num_args_, 0);

  DLOG(INFO) << "Command: " << num_args_ << " " << absl::StrJoin(args_, " ");

  absl::AsciiStrToUpper(&args_.front());
  const auto& upper = args_.front();

  string tmp;
  for (size_t i = 0; i < commands_.size(); ++i) {
    if (upper == commands_[i].name()) {
      commands_[i].Call(args_, &tmp);
      outgoing_buf_.push_back(std::move(tmp));
      return;
    }
  }

  req_ec_ = errc::make_error_code(errc::invalid_argument);
}

bool RespConnectionHandler::FlushWrites() {
  std::unique_lock<fibers::mutex> ul(wr_mu_, std::try_to_lock_t{});
  if (!ul || outgoing_buf_.empty() || !socket_->is_open())
    return false;

  std::vector<std::string> local_buf(std::move(outgoing_buf_));
  write_seq_.resize(local_buf.size());
  for (size_t i = 0; i < local_buf.size(); ++i) {
    write_seq_[i] = asio::buffer(local_buf[i]);
  }

  asio::write(*socket_, write_seq_, req_ec_);

  return true;
}

RespListener::RespListener() {
}

RespListener::~RespListener() {
}

void RespListener::Init() {
  commands_.emplace_back("COMMAND", 0, bitmask(FL_RANDOM, FL_LOADING, FL_STALE));
  commands_.back().SetFunction(
      [this](const auto& args, string* s) { return PrintCommands(args, s); });

  commands_.emplace_back("PING", -1, bitmask(FL_FAST, FL_STALE));
  commands_.back().SetFunction(
      [this](const auto& args, string* s) { return Ping(args, s); });
}

void RespListener::PrintCommands(const Args& args, string* dest) {
  VLOG(1) << "PrintCommands";

  const char kReply[] =
      "*2\r\n"
      "*6\r\n+command\r\n:0\r\n*3\r\n+random\r\n+loading\r\n+stale\r\n"
      ":0\r\n:0\r\n:0\r\n"
      "*6\r\n+ping\r\n:-1\r\n*2\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n";

  dest->assign(kReply, sizeof(kReply) - 1);
}

void RespListener::Ping(const Args& args, string* dest) {
  VLOG(1) << "Ping Handler";

  system::error_code ec;

  if (args.size() > 2) {
    *dest = "-ERR too many arguments\r\n";
  } else if (args.size() == 1) {
    *dest = "+PONG\r\n";
  } else {
    absl::StrAppend(dest, "+", args.back(), "\r\n");
  }
}

ConnectionHandler* RespListener::NewConnection(util::IoContext& context) {
  CHECK(!commands_.empty());

  return new RespConnectionHandler(commands_, &context);
}

}  // namespace redis
