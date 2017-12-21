#include "base/init.h"
#include "base/logging.h"
#include <boost/fiber/all.hpp>
#include <thread>

using namespace boost;

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  fibers::buffered_channel<int> ch(4);
  using fibers::channel_op_status;

  for (unsigned i = 0; i < 3; ++i)
    CHECK(channel_op_status::success == ch.try_push(1));
  CHECK(channel_op_status::full == ch.try_push(1));

  auto f = [&] {
    int j;
    CHECK(channel_op_status::success == ch.pop(j)); };

  boost::fibers::future<void> f1 = boost::fibers::async(fibers::launch::dispatch, f);

  /*std::thread t([&] {
    int j;
    CHECK(channel_op_status::success == ch.pop(j)); });
    */
  CHECK(channel_op_status::success == ch.push(1));
  // t.join();
  f1.get();

  return 0;
}
