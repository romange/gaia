// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <sys/poll.h>
#include <functional>
#include <boost/intrusive/slist.hpp>

namespace util {
namespace uring {

class Proactor;

class FdEvent {
  explicit FdEvent(int fd) noexcept : fd_(fd) {
  }

 public:
  using IoResult = int;

  using CbType = std::function<void(IoResult, Proactor*, FdEvent*)>;

  template <typename U> void Arm(U&& u) {
    cb_ = std::forward<U>(u);
  }

  int handle() const {
    return fd_;
  }

  void DisarmAndDiscard(Proactor* proactor);

  void AddPollin(Proactor* proactor);

 private:
  using connection_hook_t = boost::intrusive::slist_member_hook<
      boost::intrusive::link_mode<boost::intrusive::normal_link>>;

  void Run(IoResult res, Proactor* owner) {
    cb_(res, owner, this);
  }

  int fd_;
  connection_hook_t hook_;
  CbType cb_;  // This lambda might hold auxillary data that is needed to run.

  using member_hook_t = boost::intrusive::member_hook<FdEvent, connection_hook_t, &FdEvent::hook_>;
  friend class Proactor;
};

static_assert(sizeof(FdEvent) == 48, "");  // std::function weighs 32 bytes.

}  // namespace uring
}  // namespace util
