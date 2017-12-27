// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/init.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "util/sq_threadpool.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

using namespace boost;

DEFINE_string(file, "", "File to read asynchronously");
DEFINE_int32(threads, 4, "");
DEFINE_int32(io_len, 4, "");

using namespace std;
using util::FileIOManager;
using fibers::channel_op_status;

struct Item {
  strings::MutableByteRange buf;
  FileIOManager::ReadResult res;
};

typedef fibers::buffered_channel<Item> ReadQueue;

int queue_requests = 0;

void ReadRequests(ReadQueue* q) {
  channel_op_status st;
  Item item;

  while(true) {
    uint64 start = base::GetMonotonicMicrosFast();
    st = q->pop(item);
    if (channel_op_status::closed == st)
      break;
    --queue_requests;
    uint64 delta = base::GetMonotonicMicrosFast() - start;
    if (delta > 500) {
      LOG(INFO) << "Stuck for " << delta;
    }
    CHECK(st == channel_op_status::success) << int(st);  
    item.res.get();
    if (queue_requests < 4)
      boost::this_fiber::yield();

  };
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  
  CHECK(!FLAGS_file.empty());
  int fd = open(FLAGS_file.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);
  
  struct stat sbuf;
  CHECK_EQ(0, fstat(fd, &sbuf));
  cout << "File size is " << sbuf.st_size << endl;
  off_t offset = 0;
  
  FileIOManager io_mgr(FLAGS_threads, FLAGS_io_len);  

  constexpr unsigned kThisFiberQueue = 16;
  ReadQueue ch(kThisFiberQueue);
  std::array<uint8_t[16], kThisFiberQueue> buf_array;
  unsigned num_requests = 0;

  // launch::post means - dispatch a new fiber but do not yield now.
  fibers::fiber read_fiber(fibers::launch::post, &ReadRequests, &ch);

  while (offset + 20 < sbuf.st_size) {
    static_assert(sizeof(buf_array.front()) == 16, "");

    strings::MutableByteRange dest(buf_array[num_requests % 8], sizeof(buf_array.front()));
    FileIOManager::ReadResult res = io_mgr.Read(fd, offset, dest);
    channel_op_status st = ch.push(Item{dest, std::move(res)});
    CHECK(st == channel_op_status::success);
    ++queue_requests;

    ++num_requests;
    
    offset += (1 << 17);
  }
  ch.close();
  read_fiber.join();
  
  return 0;
}
