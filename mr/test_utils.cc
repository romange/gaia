// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/test_utils.h"

namespace mr3 {

void PrintTo(const ShardId& src, std::ostream* os) {
  if (absl::holds_alternative<uint32_t>(src)) {
    *os << absl::get<uint32_t>(src);
  } else {
    *os << absl::get<std::string>(src);
  }
}

void TestContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  std::lock_guard<::boost::fibers::mutex> lk(mu_);

  outp_[shard_id].push_back(record);
}

}  // namespace mr3
