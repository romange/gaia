// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "file/file.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace file {

// Fiber-friendly file handler. Returns ReadonlyFile* instance that does not block the current
// thread unlike the regular posix implementation. All the read opearations will run
// in FiberQueueThreadPool.
struct FiberReadOptions : public ReadonlyFile::Options {
  struct Stats {
    size_t cache_bytes = 0;  // read because of prefetch logic.
    size_t disk_bytes = 0;    // read via  ThreadPool calls.
    size_t read_prefetch_cnt = 0;
    size_t preempt_cnt = 0;
  };

  size_t prefetch_size = 0;
  Stats* stats = nullptr;
};

util::StatusObject<ReadonlyFile*> OpenFiberReadFile(
    StringPiece name, util::fibers_ext::FiberQueueThreadPool* tp,
    const FiberReadOptions& opts = FiberReadOptions{}) MUST_USE_RESULT;

struct FiberWriteOptions : public OpenOptions {
  bool consistent_thread = true;  // whether to send the write request to the same pool-thread.
};

util::StatusObject<WriteFile*>
OpenFiberWriteFile(StringPiece name, util::fibers_ext::FiberQueueThreadPool* tp,
                   const FiberWriteOptions& opts = FiberWriteOptions()) MUST_USE_RESULT;

}  // namespace file
