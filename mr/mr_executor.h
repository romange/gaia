// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/fiber.hpp>

#include "mr/mr.h"

#include "util/asio/io_context_pool.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/simple_channel.h"

namespace mr3 {

class GlobalDestFileManager {
  const std::string root_dir_;
  const pb::Output& out_;

  util::fibers_ext::FiberQueueThreadPool* fq_;
  google::dense_hash_map<StringPiece, ::file::WriteFile*> dest_files_;
  UniqueStrings str_db_;
  ::boost::fibers::mutex mu_;

 public:
  using Result = std::pair<StringPiece, ::file::WriteFile*>;

  GlobalDestFileManager(const std::string& root_dir, const pb::Output& out,
                        util::fibers_ext::FiberQueueThreadPool* fq);
  ~GlobalDestFileManager();

  Result Get(StringPiece key);

  util::fibers_ext::FiberQueueThreadPool* pool() { return fq_; }
};

// ThreadLocal context
class ExecutorContext : public RawContext {
 public:
  explicit ExecutorContext(GlobalDestFileManager* mgr);
  ~ExecutorContext();

  void Flush();
 private:
  void WriteInternal(const ShardId& shard_id, std::string&& record) final;

  struct BufferedWriter;

  google::dense_hash_map<StringPiece, BufferedWriter*> custom_shard_files_;

  GlobalDestFileManager* mgr_;
};


class Executor {
  using StringQueue = util::fibers_ext::SimpleChannel<std::string>;
  using FileNameQueue = ::boost::fibers::buffered_channel<std::string>;

  struct PerIoStruct;

 public:
  Executor(const std::string& root_dir, util::IoContextPool* pool);
  ~Executor();

  void Init();
  void Run(const InputBase* input, TableBase* ss);

  // Stops the executor in the middle.
  void Stop();

  void Shutdown();

 private:
  // External, disk thread that reads files from disk and pumps data into record_q.
  // One per IO thread.
  void ProcessFiles(pb::WireFormat::Type tp);
  uint64_t ProcessText(file::ReadonlyFile* fd);

  void MapFiber(TableBase* sb);

  std::string root_dir_;
  util::IoContextPool* pool_;
  FileNameQueue file_name_q_;

  static thread_local std::unique_ptr<PerIoStruct> per_io_;
  std::unique_ptr<util::fibers_ext::FiberQueueThreadPool> fq_pool_;
  std::unique_ptr<GlobalDestFileManager> dest_mgr_;
};

}  // namespace mr3
