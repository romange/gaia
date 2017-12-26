#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/PriorityLifoSemMPMCQueue.h>
#include <folly/init/Init.h>
#include <folly/experimental/EventCount.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#include <iostream>

#include "base/init.h"
#include "base/logging.h"


DEFINE_int32(threads, 4, "");
DEFINE_int32(io_len, 64, "");
DEFINE_bool(async, false, "");

using namespace folly;
using namespace std;

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  // folly::init(&argc, &argv);

  using queue_t = LifoSemMPMCQueue<CPUThreadPoolExecutor::CPUTask, QueueBehaviorIfFull::BLOCK>;
  std::shared_ptr<folly::CPUThreadPoolExecutor> pool 
      = std::make_shared<folly::CPUThreadPoolExecutor>(
    FLAGS_threads, std::make_unique<queue_t>(FLAGS_io_len),
    std::make_shared<folly::NamedThreadFactory>("DiskIOThread"));

  constexpr unsigned kBufSize = 1 << 15;
  char cbuf[kBufSize];

  unsigned sent_requests = 0;
  for (int i = 1; i < argc; ++i) {
    const char* filename = argv[i]; 
    int fd = open(filename, O_RDONLY, 0644);
    CHECK_GT(fd, 0);

    struct stat sbuf;
    CHECK_EQ(0, fstat(fd, &sbuf));
    posix_fadvise(fd, 0, 0, POSIX_FADV_NOREUSE);

    off_t offset = 0;
    std::atomic_long active_requests(0);
    folly::EventCount ec;
    while (offset + kBufSize < sbuf.st_size) {
      ++sent_requests;
      if (FLAGS_async) {
        active_requests.fetch_add(1, std::memory_order_acq_rel);
        pool->add([&, offset, fd] () 
          { 
            // LOG(INFO) << "Offset " << offset;
            CHECK_EQ(kBufSize, pread(fd, cbuf, kBufSize, offset));
            if (1 == active_requests.fetch_sub(1, std::memory_order_acq_rel)) {
              ec.notify(); 
            }
          });
      } else {
        CHECK_EQ(kBufSize, pread(fd, cbuf, kBufSize, offset));
      }
      offset += (1 << 18);
    }

    if (FLAGS_async) {
      ec.await([&] { return active_requests.load(std::memory_order_acquire) == 0; });
    }
    CHECK_EQ(0, active_requests);

    close(fd);
    LOG(INFO) << "Finished " << filename << " with " << sent_requests;
  }
  
  return 0;
}
