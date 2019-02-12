// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"

#include "util/asio/io_context_pool.h"

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);


  return 0;
}
