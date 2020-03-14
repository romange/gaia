// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "examples/redis/redis_command.h"

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/logging.h"

namespace redis {

const char* Command::FlagName(CommandFlag fl) {
  switch (fl) {
    case FL_WRITE:
      return "write";
    case FL_READONLY:
      return "readonly";
    case FL_DENYOOM:
      return "denyoom";
    case FL_FAST:
      return "fast";
    case FL_STALE:
      return "stale";
    case FL_LOADING:
      return "loading";
    case FL_RANDOM:
      return "random";
    default:
      LOG(FATAL) << "Unknown flag " << fl;
  }
}

uint32_t Command::FlagsCount(uint32_t flags) {
  return Bits::CountOnes(flags);
}

void Command::Call(const Args& args, std::string* dest) const {
  CHECK_NE(0, arity_);

  if ((arity_ > 0 && args.size() != size_t(arity_)) ||
      (arity_ < 0 && args.size() < size_t(-arity_))) {
    *dest = absl::StrCat("-ERR wrong number of arguments for '", name_, "' command\r\n");
    return;
  }

  fun_(args, dest);
}

}  // namespace redis
