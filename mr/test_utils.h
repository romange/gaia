// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

#include <boost/fiber/mutex.hpp>

#include "mr/mr.h"

namespace other {
struct StrVal {
  std::string val;
};

}  // namespace other

namespace mr3 {

// For gcc less than 7 we should enclose the specialization into the original namespace.
// https://stackoverflow.com/questions/25594644/warning-specialization-of-template-in-different-namespace
template <> class RecordTraits<other::StrVal> {
 public:
  static std::string Serialize(other::StrVal&& doc) { return std::move(doc.val); }

  bool Parse(std::string&& tmp, other::StrVal* res) {
    res->val = tmp;
    return true;
  }
};

void PrintTo(const ShardId& src, std::ostream* os);

using ShardedOutput = std::unordered_map<ShardId, std::vector<std::string>>;


class TestContext : public RawContext {
  ShardedOutput& outp_;
  ::boost::fibers::mutex& mu_;

 public:
  TestContext(ShardedOutput* outp, ::boost::fibers::mutex* mu) : outp_(*outp), mu_(*mu) {}

  void WriteInternal(const ShardId& shard_id, std::string&& record);
};

}  // namespace mr3
