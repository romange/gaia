#include <cassert>
#include <iostream>
#include <memory>
#include "base/init.h"
#include "base/integral_types.h"
#include "base/logging.h"

// Urghhh: need to think how to make it work.
#define __SANITIZE_ADDRESS__ 0
#include <folly/io/async/EventBase.h>
#include <folly/fibers/FiberManager.h>
#include <folly/fibers/FiberManagerMap.h>

using namespace std;

DEFINE_int32(count, 1000, "");

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  folly::EventBase evb;
  auto& fiberManager = folly::fibers::getFiberManager(evb);
  folly::fibers::Baton baton;

  return 0;
}
