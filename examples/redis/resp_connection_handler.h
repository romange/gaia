// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "util/asio/connection_handler.h"

namespace redis {

class RespParser;
class Command;
/**
 * @brief Server side handler that talks RESP (REdis Serialization Protocol)
 *
 */
class RespConnectionHandler : public ::util::ConnectionHandler {
 public:
  RespConnectionHandler(const std::vector<Command>& commands, util::IoContext* context);

 protected:
  boost::system::error_code HandleRequest() final;
  void OnOpenSocket() final;
  bool FlushWrites() final;

 private:
  enum class IoState : uint8_t { READ_EOL = 1, READ_N = 2, HANDLE_STRING = 3 };

  boost::system::error_code HandleIoState(RespParser* parser, IoState* state);

  IoState HandleNextString(absl::string_view blob, RespParser* parser);
  void HandleCommand();

  uint32_t num_args_ = 1;
  uint32_t bulk_size_ = 0;

  boost::system::error_code req_ec_;

  enum class CmdState : uint8_t { INIT = 1, ARG_START = 2, EMPTY_EXPECTED = 4 };
  CmdState cmd_state_ = CmdState::INIT;
  std::string line_buffer_;
  ::boost::asio::mutable_buffer bulk_str_;
  const std::vector<Command>& commands_;

  uint32_t conn_id_;
  std::vector<std::string> args_;

  std::vector<std::string> outgoing_buf_;
  ::boost::fibers::mutex wr_mu_;
  std::vector<::boost::asio::const_buffer> write_seq_;
};

class RespListener : public ::util::ListenerInterface {
 public:
  using Args = std::vector<std::string>;

  RespListener();
  ~RespListener();

  void Init();

  util::ConnectionHandler* NewConnection(util::IoContext& context) final;

 private:
  void PrintCommands(const Args& args, std::string* dest);
  void Ping(const Args& args, std::string* dest);

  std::vector<Command> commands_;
};

}  // namespace redis
