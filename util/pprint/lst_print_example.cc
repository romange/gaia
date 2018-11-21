// Copyright 2013, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <signal.h>
#include <sys/stat.h>

#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

#include "absl/strings/match.h"

#include "base/init.h"
#include "file/list_file.h"
#include "strings/escaping.h"
#include "util/pprint/file_printer.h"
#include "util/pprint/pprint_utils.h"

DECLARE_string(csv);

DECLARE_bool(sizes);
DECLARE_bool(raw);
DECLARE_bool(count);

using namespace util;
using std::string;
using util::Status;

using std::cout;

using namespace file;

void sigpipe_handler(int signal) {
  exit(1);
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  signal(SIGPIPE, sigpipe_handler);

  size_t count = 0;

  // const Reflection* reflection = msg->GetReflection();
  for (int i = 1; i < argc; ++i) {
    StringPiece path(argv[i]);
    LOG(INFO) << "Opening " << path;

    pprint::ListReaderPrinter printer;
    printer.Init(argv[i]);
    auto st = printer.Run();
    CHECK_STATUS(st);
    count += printer.count();
  }
  if (FLAGS_count)
    std::cout << "Count: " << count << std::endl;

  return 0;
}
