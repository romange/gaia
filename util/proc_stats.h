// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef PROC_STATUS_H
#define PROC_STATUS_H

#include <ostream>

#include "base/integral_types.h"

namespace util {

struct ProcessStats {
  uint32 vm_peak = 0;
  uint32 vm_rss = 0;
  uint32 vm_size = 0;

  // Start time of the process in seconds since epoch.
  uint64 start_time_seconds = 0;

  static ProcessStats Read();
};

namespace sys {
  unsigned int NumCPUs();
}  // namespace sys

}  // namespace util

std::ostream& operator<<(std::ostream& os, const util::ProcessStats& stats);

#endif  // PROC_STATUS_H
