// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/yield.h"

namespace util {
namespace fibers {

/// canonical instance
thread_local yield_t yield{};

}  // namespace fibers
}  // namespace util
