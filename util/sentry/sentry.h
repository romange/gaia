// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/asio/io_context.h"

namespace util {

class SentryClient final : public IoContext::Cancellable {
 public:
  void Run() override;
  void Cancel() override;
};

}  // namespace util
