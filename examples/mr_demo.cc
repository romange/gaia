// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <functional>
#include <thread>

#include "base/init.h"
#include "base/logging.h"
#include "base/object_pool.h"
#include "base/walltime.h"
#include "file/fiber_file.h"
#include "file/file_util.h"
#include "strings/stringpiece.h"

#include "util/asio/io_context_pool.h"
#include "util/fiberqueue_threadpool.h"

using namespace boost;

DEFINE_int32(threads, 4, "");
DEFINE_int32(io_len, 4, "");

using namespace std;
using fibers::channel_op_status;
using namespace std::chrono_literals;
using namespace util;

typedef fibers::buffered_channel<strings::ByteRange> BufQueue;

int queue_requests = 0;

class BufStore {
  typedef std::unique_ptr<uint8_t[]> ReadBuf;
  typedef std::unique_ptr<ReadBuf[]> BufArray;

 public:
  BufStore(unsigned count, size_t buf_capacity)
      : buf_(new uint8_t[count * buf_capacity]), buf_capacity_(buf_capacity) {
    uint8_t* ptr = buf_.get();
    obj_pool_.reserve(count);

    for (unsigned i = 0; i < count; ++i) {
      obj_pool_.emplace_back(ptr, buf_capacity_);
      ptr += buf_capacity_;
    }
  }

  strings::MutableByteRange Get() {
    std::unique_lock<::boost::fibers::mutex> lock(mu_);
    cond_.wait(lock, [&] { return !obj_pool_.empty(); });

    strings::MutableByteRange res = obj_pool_.back();
    obj_pool_.pop_back();

    return res;
  }

  void Return(strings::MutableByteRange mb) {
    std::unique_lock<::boost::fibers::mutex> lock(mu_);
    obj_pool_.push_back(mb);
    lock.unlock();
    cond_.notify_all();
  }

 private:
  fibers::mutex mu_;
  fibers::condition_variable cond_;
  ReadBuf buf_;
  size_t buf_capacity_;
  std::vector<strings::MutableByteRange> obj_pool_;
};

static constexpr unsigned kMaxActiveFibers = 10;
static constexpr unsigned kReadSize = 1 << 15;

class Mr {
  std::unique_ptr<util::FiberQueueThreadPool> fq_pool_;

  typedef fibers::buffered_channel<string> IncomingQueue;

  IncomingQueue inc_queue_;

  fibers::mutex m_;
  fibers::condition_variable num_fibers_;

  struct IoStruct {
    fibers::fiber read_fb;
    fibers::buffered_channel<file::ReadonlyFile*> rd_queue;

    IoStruct() : rd_queue(4) {}
  };

  thread_local static std::unique_ptr<IoStruct> per_io_;
  IoContextPool* pool_;

 public:
  explicit Mr(IoContextPool* pool)
      : fq_pool_(new util::FiberQueueThreadPool(0)), inc_queue_(2), pool_(pool) {
    pool->AwaitOnAll([this](IoContext&) {
      per_io_.reset(new IoStruct);
      per_io_->read_fb = fibers::fiber(&Mr::OpenFile, this);
    });
  }

  void Join() {
    inc_queue_.close();
    pool_->AwaitOnAll([this](IoContext&) { per_io_->read_fb.join(); });
  }

  void Emplace(string item) {
    channel_op_status st = inc_queue_.push(item);
    VLOG(1) << "Pushing " << item;
    CHECK(st == channel_op_status::success);
  }

 private:
  void OpenFile();
  void Process();
};

thread_local std::unique_ptr<Mr::IoStruct> Mr::per_io_;

void Mr::OpenFile() {
  string file_name;

  fibers::fiber puller_fb(&Mr::Process, this);

  while (true) {
    channel_op_status st = inc_queue_.pop(file_name);
    if (st == channel_op_status::closed)
      break;

    CHECK(st == channel_op_status::success);
    CONSOLE_INFO << file_name;
    auto res = file::OpenFiberReadFile(file_name, fq_pool_.get());
    if (!res.ok()) {
      LOG(INFO) << "Skipping " << file_name << " with " << res.status;
      continue;
    }
    LOG(INFO) << "Pushing file " << file_name;

    st = per_io_->rd_queue.push(res.obj);
    CHECK(st == channel_op_status::success);
  }
  per_io_->rd_queue.close();
  puller_fb.join();
}

inline void line_cb(StringPiece str) {}

void ProcessRaw(BufQueue* q, std::function<void(strings::ByteRange)> deleter) {
  string line;
  while (true) {
    strings::ByteRange br;
    channel_op_status st = q->pop(br);
    if (channel_op_status::closed == st)
      break;
    CHECK(st == channel_op_status::success) << int(st);
    auto keep = br;

    while (!br.empty()) {
      auto it = std::find(br.begin(), br.end(), '\n');

      if (it == br.end()) {
        line.append(br.begin(), br.end());
        break;
      }

      if (line.empty()) {
        StringPiece str(reinterpret_cast<const char*>(br.data()), it - br.begin());
        line_cb(str);
      } else {
        line.append(br.begin(), it);
        line_cb(line);
        line.clear();
      }
      br.remove_prefix(it - br.begin() + 1);
    }
    deleter(keep);
  }

  if (!line.empty()) {
    line_cb(line);
  }
}

util::Status ProcessInternal(file::ReadonlyFile* file) {
  size_t offset = 0;
  BufStore store(kMaxActiveFibers, kReadSize);

  BufQueue q(32);
  fibers::fiber fb(&ProcessRaw, &q, [&](strings::ByteRange br) {
    strings::MutableByteRange mbr(const_cast<uint8_t*>(br.data()), kReadSize);
    store.Return(mbr);
  });

  util::Status res;
  while (true) {
    strings::MutableByteRange cur_buf = store.Get();
    auto read_res = file->Read(offset, cur_buf);
    if (!read_res.ok()) {
      res = read_res.status;
      break;
    }
    size_t read_sz = read_res.obj;
    LOG(INFO) << "Read at offset " << offset << "/" << read_sz;

    offset += read_sz;
    size_t orig_size = cur_buf.size();
    cur_buf.reset(cur_buf.data(), read_sz);

    q.push(cur_buf);
    if (read_sz < orig_size)
      break;
  }
  q.close();
  fb.join();

  return res;
}

void Mr::Process() {
  channel_op_status st;

  string line;
  IoStruct& io = *per_io_;
  std::string buf(1 << 16, '\0');

  while (true) {
    file::ReadonlyFile* tmp = nullptr;
    st = io.rd_queue.pop(tmp);
    if (channel_op_status::closed == st)
      break;
    CHECK(st == channel_op_status::success) << int(st);
    LOG(INFO) << "Pulling file XXX";

    std::unique_ptr<file::ReadonlyFile> item(tmp);
    auto status = ProcessInternal(tmp);
    if (!status.ok()) {
      LOG(ERROR) << "Error reading file " << status.ToString();
      continue;
    }
  }
}

// TODO: To implement IO push flow. i.e. instead of the reading loop will be blocked on IO latency,
// between CPU spikes, we will read the file blocks and push them into processing
// fibers. This way, in CPU should balance IO, and we will have maximum IO/CPU utilization.
int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  std::vector<std::string> files;
  for (int i = 1; i < argc; ++i)
    files.push_back(argv[i]);

  CHECK(!files.empty());
  LOG(INFO) << "Processing " << files.size() << " files";

  IoContextPool pool;
  pool.Run();

  Mr mr(&pool);

  for (const auto& item : files) {
    mr.Emplace(item);
  }

  mr.Join();

  return 0;
}
