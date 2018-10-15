// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _POSIX_CALL_H
#define _POSIX_CALL_H

#include <string>
#include <string.h>
#include "base/logging.h"

namespace base {

inline std::string PosixStrError(int e) {
  char buf[1024];
  return strerror_r(e, buf, sizeof buf);
}

inline std::string PosixStrError() {
  return PosixStrError(errno);
}

}  // namespace base

#define POSIX_CALL(x) do { \
    int r = x; \
    if (r != 0) \
      LOG(ERROR) << "Error calling " #x << ", msg: " << base::PosixStrError(); \
    } while(false);

#endif  // _POSIX_CALL_H
