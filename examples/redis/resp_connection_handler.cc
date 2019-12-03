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
}

/*
   *num tokens in the command,token1, token2 ....
   Example commands: echo -e '*1\r\n$4\r\PING\r\n', echo -e 'PING\r\n',
   echo -e ' \r\n*1\r\n$7\r\nCOMMAND\r\n'
   { echo -e '*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n';  } | nc  localhost 6379
*/
system::error_code RespConnectionHandler::HandleRequest() {
  absl::string_view cmd;
  RespParser parser;

  system::error_code ec;
  num_args_ = 1;
  IoState io_state = IoState::READ_EOL;
  // CHECK(states_.empty());
  cmd_state_ = CmdState::INIT;

  // states_.push_back(CmdState::INIT);
  VLOG(1) << "RespConnectionHandler::HandleRequest";

  while (true) {
    switch (io_state) {
      case IoState::READ_EOL: {
        auto dest_buf = parser.GetDestBuf();
        if (dest_buf.size() < 2) {
          return system::errc::make_error_code(system::errc::no_buffer_space);
        }

        size_t read_sz =
            socket_->read_some(asio::mutable_buffer{dest_buf.data(), dest_buf.size()}, ec);
        if (ec)
          return ec;
        RespParser::ParseStatus status = parser.WriteCommit(read_sz, &cmd);
        if (status == RespParser::LINE_FINISHED) {
          io_state = IoState::HANDLE_STRING;
        }
        break;
      }
      case IoState::READ_N:
        CHECK(parser.ReadEof());
        asio::read(*socket_, bulk_str_, ec);
        if (ec)
          return ec;
        io_state = IoState::READ_EOL;
        break;
      case IoState::HANDLE_STRING: {
        ErrorState es = HandleNextString(cmd, &parser);
        system::error_code* ec = absl::get_if<system::error_code>(&es);
        if (ec)
          return *ec;
        IoState next_state = absl::get<IoState>(es);
        if (next_state != IoState::READ_EOL || parser.ParseNext(&cmd) == RespParser::MORE_DATA) {
          io_state = next_state;
        }
        break;
      }
    }

    if (args_.size() == num_args_) {
      ec = HandleCommand();
      if (ec)
        return ec;
      args_.clear();
      num_args_ = 1;
      cmd_state_ = CmdState::INIT;
    }
  }

  VLOG(1) << "Finished";

  return ec;
}

auto RespConnectionHandler::HandleNextString(absl::string_view blob, RespParser* parser)
    -> ErrorState {
  VLOG(1) << "Blob: |" << blob << "|";
  system::error_code ec;
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
        VLOG(1) << "Bulk string of size " << bulk_size_ << "/" << bulk_str_.size();

        return IoState::READ_N;
      } else {
        args_.push_back(string{blob});
        VLOG(1) << "Pushed " << args_.back();
        cmd_state_ = CmdState::ARG_START;
        return IoState::READ_EOL;
      }
      break;
    case CmdState::EMPTY_EXPECTED:
      if (!blob.empty())
        return errc::make_error_code(errc::illegal_byte_sequence);
      args_.push_back(std::move(line_buffer_));
      VLOG(1) << "Pushed " << args_.back();
      cmd_state_ = CmdState::ARG_START;

      return IoState::READ_EOL;
      break;
    default:
      LOG(FATAL) << "BUG";
  }

  return system::error_code{};
}

system::error_code RespConnectionHandler::HandleCommand() {
  CHECK_EQ(num_args_, args_.size());
  CHECK_GT(num_args_, 0);

  LOG(INFO) << "Command: " << num_args_ << " " << absl::StrJoin(args_, " ");

  absl::AsciiStrToUpper(&args_.front());
  const auto& upper = args_.front();

  for (size_t i = 0; i < commands_.size(); ++i) {
    if (upper == commands_[i].name()) {
      return commands_[i].Call(args_, &socket_.value());
    }
  }

  return errc::make_error_code(errc::invalid_argument);
}

RespListener::RespListener() {
}

RespListener::~RespListener() {
}

void RespListener::Init() {
  commands_.emplace_back("COMMAND", 0, bitmask(FL_RANDOM, FL_LOADING, FL_STALE));
  commands_.back().SetFunction(
      [this](const auto& args, util::FiberSyncSocket* s) { return PrintCommands(args, s); });

  commands_.emplace_back("PING", -1, bitmask(FL_FAST, FL_STALE));
  commands_.back().SetFunction(
      [this](const auto& args, util::FiberSyncSocket* s) { return Ping(args, s); });
}

system::error_code RespListener::PrintCommands(const Args& args, util::FiberSyncSocket* s) {
  VLOG(1) << "PrintCommands";

  const char kReply[] =
      "*2\r\n"
      "*6\r\n+command\r\n:0\r\n*3\r\n+random\r\n+loading\r\n+stale\r\n"
      ":0\r\n:0\r\n:0\r\n"
      "*6\r\n+ping\r\n:-1\r\n*2\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n";

  system::error_code ec;
  asio::write(*s, asio::buffer(kReply, sizeof(kReply) - 1), ec);
  return ec;
}

system::error_code RespListener::Ping(const Args& args, util::FiberSyncSocket* s) {
  VLOG(1) << "Ping Handler";

  system::error_code ec;

  const char* str = nullptr;
  string tmp;
  if (args.size() > 2) {
    str = "-ERR too many arguments\r\n";
  } else if (args.size() == 1) {
    str = "+PONG\r\n";
  } else {
    absl::StrAppend(&tmp, "+", args.back(), "\r\n");
    str = tmp.c_str();
  }

  asio::write(*s, asio::buffer(str, strlen(str)), ec);

  return ec;
}

ConnectionHandler* RespListener::NewConnection(util::IoContext& context) {
  CHECK(!commands_.empty());

  return new RespConnectionHandler(commands_, &context);
}

}  // namespace redis
