#include "base/init.h"
#include <boost/fiber/all.hpp>

using namespace boost;
int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  fibers::buffered_channel<int> ch(2);
  return 0;
}
