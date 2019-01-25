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

struct Item {
  strings::MutableByteRange buf;
  util::StatusObject<size_t> res;
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

class Mr {
  static constexpr unsigned kThisFiberQueue = 16;
  static constexpr unsigned kReadSize = 1 << 15;
  static constexpr unsigned kMaxActiveFibers = 10;

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
    pool->AwaitOnAll([this] (IoContext&) {
      per_io_.reset(new IoStruct);
      per_io_->read_fb = fibers::fiber(&Mr::OpenFile, this);
    });
  }

  void Join() {
    inc_queue_.close();
    pool_->AwaitOnAll([this] (IoContext&) { per_io_->read_fb.join(); });
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
    st = per_io_->rd_queue.push(res.obj);
    CHECK(st == channel_op_status::success);
  }
  per_io_->rd_queue.close();
}

inline void line_cb(StringPiece str) {}

util::Status ProcessInternal(strings::MutableByteRange work_buf, file::ReadonlyFile* file) {
  size_t offset = 0;
  size_t buf_size = work_buf.size();
  string line;

  while (true) {
    strings::MutableByteRange local_buf = work_buf;
    GET_UNLESS_ERROR(read_sz, file->Read(offset, local_buf));

    offset += read_sz;
    local_buf.reset(local_buf.data(), read_sz);

    while (!local_buf.empty()) {
      auto it = std::find(local_buf.begin(), local_buf.end(), '\n');

      if (it == local_buf.end()) {
        line.append(local_buf.begin(), local_buf.end());
        break;
      }

      if (line.empty()) {
        StringPiece str(reinterpret_cast<const char*>(local_buf.data()), it - local_buf.begin());
        line_cb(str);
      } else {
        line.append(local_buf.begin(), it);
        line_cb(line);
        line.clear();
      }
      local_buf.remove_prefix(it - local_buf.begin() + 1);
    }
    if (read_sz < buf_size)
      break;
  }

  if (!line.empty()) {
    line_cb(line);
  }
  return Status::OK;
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

    std::unique_ptr<file::ReadonlyFile> item(tmp);
    auto status = ProcessInternal(strings::AsMutableByteRange(buf), tmp);
    if (!status.ok()) {
      LOG(ERROR) << "Error reading file " << status.ToString();
      continue;
    }
  }
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  std::vector<std::string> files;
  for (int i = 1; i < argc; ++i)
    files.push_back(argv[i]);

  CHECK(!files.empty());
  LOG(INFO) << "Processing " << files.size() << " files";

  IoContextPool pool;
  Mr mr(&pool);

  for (const auto& item : files) {
    mr.Emplace(item);
  }

  mr.Join();

  return 0;
}
