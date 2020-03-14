// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/system/error_code.hpp>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

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

enum CommandFlag {
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
  static const char* FlagName(CommandFlag fl);
  static uint32_t FlagsCount(uint32_t flags);

  using Args = std::vector<std::string>;
  using CommandFunction = std::function<void(const Args&, std::string*)>;

  Command(const std::string& name, int32_t arity, uint32_t flags)
      : name_(name), arity_(arity), flags_(flags), first_key_arg_(0), last_key_arg_(0),
        key_arg_step_(0) {
  }

  const std::string& name() const { return name_; }

  int32_t arity() const {
    return arity_;
  }

  uint32_t flags() const {
    return flags_;
  }

  void SetFunction(CommandFunction f) {
    fun_ = f;
  }

  void Call(const Args& args, std::string* dest) const;

  void SetKeyArgParams(uint32_t first_key_arg, uint32_t last_key_arg, uint32_t key_arg_step) {
    first_key_arg_ = first_key_arg;
    last_key_arg_ = last_key_arg;
    key_arg_step_ = key_arg_step;
  }

  uint32_t first_key_arg() const {
    return first_key_arg_;
  }

  uint32_t last_key_arg() const {
    return last_key_arg_;
  }

  uint32_t key_arg_step() const {
    return key_arg_step_;
  }

 private:
  std::string name_;
  int32_t arity_;
  uint32_t flags_;
  uint32_t first_key_arg_, last_key_arg_, key_arg_step_;

  CommandFunction fun_;
};

}  // namespace redis
