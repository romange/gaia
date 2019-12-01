// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/system/error_code.hpp>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace util {
class FiberSyncSocket;
}  // namespace util

namespace redis {

/*
admin - server admin command
pubsub - pubsub-related command
noscript - deny this command from scripts
random - command has random results, dangerous for scripts
sort_for_script - if called from script, sort output
loading - allow command while database is loading
skip_monitor - do not show this command in MONITOR
asking - cluster related - accept even if importing
movablekeys - keys have no pre-det

*/

enum CommandFlags {
  FL_WRITE = 1,
  FL_READONLY = 2,
  FL_DENYOOM = 4,
  FL_FAST = 8,
  FL_STALE = 0x10,
  FL_LOADING = 0x20,
  FL_RANDOM = 0x40,
  FL_MAX = 0x80,
};

class Command {
 public:
  using Args = std::vector<absl::string_view>;
  using CommandFunction =
      std::function< ::boost::system::error_code(const Args&, util::FiberSyncSocket*)>;

  Command(const std::string& name, int32_t arity, uint32_t flags)
      : name_(name), arity_(arity), flags_(flags) {
  }

  const std::string& name() const {
    return name_;
  }

  int32_t arity() const {
    return arity_;
  }

  uint32_t flags() const {
    return flags_;
  }

  void SetFunction(CommandFunction f) {
    fun_ = f;
  }

  ::boost::system::error_code Call(const Args& args, util::FiberSyncSocket* socket) const {
    return fun_(args, socket);
  }

 private:
  std::string name_;
  int32_t arity_;
  uint32_t flags_;

  CommandFunction fun_;
};

}  // namespace redis
