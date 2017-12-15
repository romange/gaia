// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/logging.h"

#include <cstdlib>
#include <unistd.h>
#include <iostream>

namespace base {

void ConsoleLogSink::send(google::LogSeverity severity, const char* full_filename,
                          const char* base_filename, int line,
                          const struct ::tm* tm_time,
                          const char* message, size_t message_len) {
  std::cout.write(message, message_len);
  std::cout << std::endl;
}

ConsoleLogSink* ConsoleLogSink::instance() {
  static ConsoleLogSink sink;
  return &sink;
}

}  // namespace base
