// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "util/stats/varz_node.h"
#include "util/uring/sliding_counter.h"

#define DEFINE_VARZ(type, name) ::util::uring::type name(#name)

namespace util {
namespace uring {

class VarzQps : public VarzListNode {
 public:
  explicit VarzQps(const char* varname) : VarzListNode(varname) {
  }

  void Init(ProactorPool* pp) { val_.Init(pp);}

  void Inc() {
    val_.Inc();
  }

 private:
  virtual AnyValue GetData() const override;

  using Counter = SlidingCounter<7>;

  // 7-seconds window. We gather data based on the fully filled 6.
  Counter val_;
};

}  // namespace uring
}  // namespace util
