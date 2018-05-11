// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <functional>
#include <thread>

#include "base/init.h"
#include "base/logging.h"
#include "base/walltime.h"
#include "file/file_util.h"
#include "strings/stringpiece.h"

#include "util/sq_threadpool.h"

using namespace boost;

DEFINE_int32(threads, 4, "");
DEFINE_int32(io_len, 4, "");

using namespace std;
using util::FileIOManager;
using fibers::channel_op_status;
using namespace std::chrono_literals;

struct Item {
  strings::MutableByteRange buf;
  FileIOManager::ReadResult res;
};

typedef fibers::buffered_channel<Item> ReadQueue;

int queue_requests = 0;


class BufStore {
  typedef std::unique_ptr<uint8_t[]> ReadBuf;
  typedef std::unique_ptr<ReadBuf[]> BufArray;

 public:
  BufStore(unsigned count, size_t buf_capacity)
      : buf_array_(new ReadBuf[count]), buf_capacity_(buf_capacity), q_(count * 2) {
    for (unsigned i = 0; i < count; ++i) {
      buf_array_[i].reset(new uint8_t[buf_capacity]);
      channel_op_status st = q_.push(buf_array_[i].get());
      CHECK(st == channel_op_status::success);
    }
  }

  strings::MutableByteRange Get() {
    uint8_t* ptr = q_.value_pop();
    return strings::MutableByteRange(ptr, buf_capacity_);
  }

  void Return(strings::MutableByteRange mb) {
    channel_op_status st = q_.try_push(mb.data());
    CHECK(st == channel_op_status::success);
  }

 private:
  BufArray buf_array_;
  size_t buf_capacity_;

  // Can be made much easier with something else than buffered_channel.
  // or just vector protected with mutex and cv.
  fibers::buffered_channel<uint8_t*> q_;
};

void ProcessFile(ReadQueue* q, BufStore* store, std::function<void(StringPiece)> line_cb) {
  channel_op_status st;
  Item item;
  string line;

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
    util::StatusObject<size_t> result = item.res.get();
    CHECK(result.ok());
    strings::ByteRange res_buf(item.buf.data(), result.obj);

    while (!res_buf.empty()) {
      auto it = std::find(res_buf.begin(), res_buf.end(), '\n');

      if (it == res_buf.end()) {
        line.append(res_buf.begin(), res_buf.end());
        break;
      }

      if (line.empty()) {
        StringPiece str(reinterpret_cast<const char*>(res_buf.data()),
                        it - res_buf.begin());
        line_cb(str);
      } else {
        line.append(res_buf.begin(), it);
        line_cb(line);
        line.clear();
      }
      res_buf.remove_prefix(it - res_buf.begin() + 1);
    }
    store->Return(item.buf);

    if (queue_requests < 4) {
      boost::this_fiber::yield();
    }
  };

  if (!line.empty()) {
    line_cb(line);
  }
}

class Mr {
  static constexpr unsigned kThisFiberQueue = 16;
  static constexpr unsigned kReadSize = 1 << 15;
  static constexpr unsigned kMaxActiveFibers = 10;


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

      std::unique_lock<boost::fibers::mutex> lock(m_);

      num_fibers_.wait(lock, [this] () {return num_pending_reads_ < kMaxActiveFibers;});
      ++num_pending_reads_;

      fibers::async([this, file_name] () { Process(file_name); });
    }
  }

  std::thread t_;

  unsigned num_pending_reads_ = 0;
  fibers::mutex m_;
  fibers::condition_variable num_fibers_;

 public:
  explicit Mr(FileIOManager* mgr) : io_mgr_(mgr), inc_queue_(2), t_(&Mr::ThreadFunc, this) {}

  void Join() {
    inc_queue_.close();
    t_.join();

    CONSOLE_INFO << "Before joining on num_pending_reads_";

    std::unique_lock<boost::fibers::mutex> lock(m_);
    num_fibers_.wait(lock, [this] () {return num_pending_reads_ == 0;});
  }

  void Emplace(string item) {
    channel_op_status st = inc_queue_.push(item);
    VLOG(1) << "Pushing " << item;
    CHECK(st == channel_op_status::success);
  }

 private:
  void Process(const string& filename);
};

void Mr::Process(const string& filename) {
  int fd = open(filename.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);

  struct stat sbuf;
  CHECK_EQ(0, fstat(fd, &sbuf));
  if ((sbuf.st_mode & S_IFMT) != S_IFREG) {
    LOG(INFO) << "Skipping " << filename;
    goto exit1;
  }

  {
    CONSOLE_INFO << "File size " << filename << " " << sbuf.st_size;

    BufStore buf_store(kThisFiberQueue, kReadSize);
    ReadQueue read_channel(kThisFiberQueue);

    off_t offset = 0;
    unsigned num_requests = 0;
    unsigned num_lines = 0;
    auto line_cb = [&num_lines](StringPiece line) { ++num_lines; };

    // Start a consumer fiber but do not switch yet.
    fibers::fiber read_fiber(fibers::launch::post, &ProcessFile, &read_channel,
                             &buf_store, line_cb);

    // Start sending read requests.
    while (offset < sbuf.st_size) {
      strings::MutableByteRange dest = buf_store.Get();
      FileIOManager::ReadResult res = io_mgr_->Read(fd, offset, dest);

      channel_op_status st = read_channel.push(Item{dest, std::move(res)});
      CHECK(st == channel_op_status::success);
      ++queue_requests;

      ++num_requests;

      offset += (1 << 17);
    }

    read_channel.close();  // Signal that this channel is closed.
    read_fiber.join(); // Wait for the consumer fiber to stop.
    CONSOLE_INFO << filename << " has " << num_lines << " lines with " << num_requests;
  }

exit1:
  close(fd);


  m_.lock();
  unsigned local_num = --num_pending_reads_;
  m_.unlock();
  if (local_num < kMaxActiveFibers)
    num_fibers_.notify_one();
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  std::vector<std::string> files;
  for (int i = 1; i < argc; ++i)
    files.push_back(argv[i]);

  CHECK(!files.empty());
  LOG(INFO) << "Processing " << files.size() << " files";

  FileIOManager io_mgr(FLAGS_threads, FLAGS_io_len);

  Mr mr(&io_mgr);

  for (const auto& item : files) {
    mr.Emplace(item);
  }

  mr.Join();

  return 0;
}
