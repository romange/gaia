// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "absl/strings/string_view.h"
#include "util/asio/connection_handler.h"

namespace redis {

/**
 * @brief Server side handler that talks RESP (REdis Serialization Protocol)
 *
 */
class RespConnectionHandler : public ::util::ConnectionHandler {
 public:
  RespConnectionHandler(util::IoContext* context);

 protected:
  boost::system::error_code HandleRequest() final;

 private:
  boost::system::error_code HandleCmd(absl::string_view cmd, boost::asio::mutable_buffer suffix);

  std::string line_buffer_;
};

class RespListener : public ::util::ListenerInterface {
 public:
  util::ConnectionHandler* NewConnection(util::IoContext& context) final;
};

}  // namespace redis
