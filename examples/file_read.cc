#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/PriorityLifoSemMPMCQueue.h>
#include <folly/init/Init.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include "base/logging.h"

DEFINE_string(file, "", "File to read asynchronously");
DEFINE_int32(threads, 4, "");
DEFINE_bool(async, false, "");

using namespace folly;
using namespace std;

int main(int argc, char **argv) {
  folly::init(&argc, &argv);

  using queue_t = LifoSemMPMCQueue<CPUThreadPoolExecutor::CPUTask, QueueBehaviorIfFull::BLOCK>;
  std::shared_ptr<folly::CPUThreadPoolExecutor> pool 
      = std::make_shared<folly::CPUThreadPoolExecutor>(
    FLAGS_threads, std::make_unique<queue_t>(256),
    std::make_shared<folly::NamedThreadFactory>("DiskIOThread"));

  CHECK(!FLAGS_file.empty());
  int fd = open(FLAGS_file.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);

  struct stat sbuf;
  CHECK_EQ(0, fstat(fd, &sbuf));
  
  off_t offset = 0;
  char cbuf[32];
  while (offset + 20 < sbuf.st_size) {
    if (FLAGS_async) {
      pool->add([offset, fd, &cbuf] {pread(fd, cbuf, 16, offset); 
                                    });
    } else {
      pread(fd, cbuf, 16, offset);
    }
    offset += (1 << 17);
  }

  if (FLAGS_async)
    pool->join();
  close(fd);

  return 0;
}
