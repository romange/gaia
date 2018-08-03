// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/proc_stats.h"

#include <mutex>
#include "base/walltime.h"
#include "strings/numbers.h"
#include "strings/strip.h"

#include <unistd.h>
#include <time.h>

namespace util {

size_t find_nth(StringPiece str, char c, uint32 index) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == c) {
      if (index-- == 0)
        return i;
    }
  }
  return StringPiece::npos;
}


static std::once_flag cpu_once;

static int CPU_NUM = 0;

static void InitCpuInfo() {
  FILE* f = fopen("/proc/cpuinfo", "r");
  const char kModelNameLine[] = "model name";
  if (f == NULL) {
    return;
  }
  char line[1000];
  while (fgets(line, sizeof(line), f) != NULL) {
    const char* sep = strchr(line, ':');
    if (sep == NULL) {
      continue;
    }
    StringPiece str(line, sep - line);
    str = absl::StripAsciiWhitespace(str);
    if (str == kModelNameLine)
      ++CPU_NUM;
  }
  fclose(f);
}

ProcessStats ProcessStats::Read() {
  ProcessStats stats;
  FILE* f = fopen("/proc/self/status", "r");
  if (f == nullptr)
    return stats;
  char* line = nullptr;
  size_t len = 128;
  while (getline(&line, &len, f) != -1) {
    if (!strncmp(line, "VmPeak:", 7)) stats.vm_peak = ParseLeadingUDec32Value(line + 8, 0);
    else if (!strncmp(line, "VmSize:", 7)) stats.vm_size = ParseLeadingUDec32Value(line + 8, 0);
    else if (!strncmp(line, "VmRSS:", 6)) stats.vm_size = ParseLeadingUDec32Value(line + 7, 0);
  }
  fclose(f);
  f = fopen("/proc/self/stat", "r");
  if (f) {
    long jiffies_per_second = sysconf(_SC_CLK_TCK);
    uint64 start_since_boot = 0;
    char buf[512] = {0};
    size_t bytes_read = fread(buf, 1, sizeof buf, f);
    if (bytes_read == sizeof buf) {
      fprintf(stderr, "Buffer is too small %lu\n", sizeof buf);
    } else {
      StringPiece str(buf, bytes_read);
      size_t pos = find_nth(str, ' ', 20);
      if (pos != StringPiece::npos) {
        start_since_boot = ParseLeadingUDec64Value(str.data() + pos + 1, 0);
        start_since_boot /= jiffies_per_second;
      }
    }
    fclose(f);
    if (start_since_boot > 0) {
      f = fopen("/proc/stat", "r");
      if (f) {
        while (getline(&line, &len, f) != -1) {
          if (!strncmp(line, "btime ", 6)) {
            uint64 boot_time = ParseLeadingUDec64Value(line + 6, 0);
            if (boot_time > 0)
              stats.start_time_seconds = boot_time + start_since_boot;
          }
        }
        fclose(f);
      }
    }
  }
  free(line);
  return stats;
}

namespace sys {

unsigned int NumCPUs() {
  std::call_once(cpu_once, InitCpuInfo);
  return CPU_NUM;
}

}  // namespace sys

}  // namespace util

std::ostream& operator<<(std::ostream& os, const util::ProcessStats& stats) {
  os << "VmPeak: " << stats.vm_peak << "kb, VmSize: " << stats.vm_size
     << "kb, VmRSS: " << stats.vm_rss << "kb, Start Time: "
     << base::PrintLocalTime(stats.start_time_seconds);
  return os;
}
