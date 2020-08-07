// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/varz.h"

namespace util {
namespace uring {

VarzValue VarzQps::GetData() const {

  uint32_t qps =  val_.SumTail() / (Counter::WIN_SIZE - 1); // Average over kWinSize values.
  return VarzValue::FromInt(qps);
}


}
}  // namespace util
