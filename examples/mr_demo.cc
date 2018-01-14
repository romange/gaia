// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <thread>

#include "base/init.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "file/file_util.h"

#include "util/sq_threadpool.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

using namespace boost;

DEFINE_string(dir, "", "File to read asynchronously");
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

class Mr {
  static constexpr unsigned kThisFiberQueue = 16;
  static constexpr unsigned kReadSize = 1 << 15;

  typedef std::unique_ptr<uint8_t[]> ReadBuf;
  FileIOManager* io_mgr_;

  typedef fibers::buffered_channel<string> IncomingQueue;

  IncomingQueue inc_queue_;

  void ThreadFunc() {
    string file_name;

    while (true) {
      channel_op_status st = inc_queue_.pop(file_name);
      if (st == channel_op_status::closed)
        break;
      CHECK(st == channel_op_status::success);
      CONSOLE_INFO << file_name;
    }
  }

  std::thread t_;

  int num_pending_reads_ = 0;
  fibers::mutex m_;
  fibers::condition_variable no_fibers_;
public:
  Mr(FileIOManager* mgr) : io_mgr_(mgr), inc_queue_(2), t_(&Mr::ThreadFunc, this) {
  }

  void Join() {
    inc_queue_.close();
    t_.join();
  }

  void Emplace(string item) {
    inc_queue_.push(item);
  }

 private:
  void Process(const string& filename);
};

void Mr::Process(const string& filename) {
  int fd = open(filename.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);

  struct stat sbuf;
  CHECK_EQ(0, fstat(fd, &sbuf));
  cout << "File size is " << sbuf.st_size << endl;

  std::array<ReadBuf, kThisFiberQueue> buf_array;
  ReadQueue read_channel(kThisFiberQueue);
  for (auto& ptr : buf_array)
      ptr.reset(new uint8_t[kReadSize]);

  off_t offset = 0;
  unsigned num_requests = 0;

  // launch::post means - dispatch a new fiber but do not yield now.
  fibers::fiber read_fiber(fibers::launch::post, &ReadRequests, &read_channel);

  while (offset + kReadSize < sbuf.st_size) {
    strings::MutableByteRange dest(buf_array[num_requests % 8].get(), kReadSize);
    FileIOManager::ReadResult res = io_mgr_->Read(fd, offset, dest);

    channel_op_status st = read_channel.push(Item{dest, std::move(res)});
    CHECK(st == channel_op_status::success);
    ++queue_requests;

    ++num_requests;

    offset += (1 << 17);
  }
  read_channel.close();
  read_fiber.join();
  close(fd);


  std::lock_guard<fibers::mutex> lg(m_);
  --num_pending_reads_;
  if (num_pending_reads_ == 0)
    no_fibers_.notify_one();
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);


  CHECK(!FLAGS_dir.empty());
  std::vector<std::string> files = file_util::ExpandFiles(FLAGS_dir);
  CHECK(!files.empty());

  FileIOManager io_mgr(FLAGS_threads, FLAGS_io_len);

  Mr mr(&io_mgr);
  mr.Emplace(files[0]);
  mr.Join();

  return 0;
}
