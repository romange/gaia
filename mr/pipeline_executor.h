// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/fiber.hpp>
#include <functional>

#include "mr/pipeline.h"

#include "util/asio/io_context_pool.h"
#include "util/fibers/fiberqueue_threadpool.h"

namespace mr3 {

class Pipeline::Executor {
  using StringQueue = util::fibers_ext::SimpleChannel<std::string>;

  using FileInput = std::pair<std::string, const pb::Input*>;
  using FileNameQueue = ::boost::fibers::buffered_channel<FileInput>;

  struct PerIoStruct;

 public:
  Executor(util::IoContextPool* pool, Runner* runner);
  ~Executor();

  void Init();

  void Run(const std::vector<const InputBase*>& inputs,
           detail::TableBase* ss, std::vector<std::string>* out_files);

  // Stops the executor in the middle.
  void Stop();

  void Shutdown();

 private:
  void ProcessInput(const InputBase*);

  // Input managing fiber that reads files from disk and pumps data into record_q.
  // One per IO thread.
  void ProcessInputFiles();

  void MapFiber(detail::TableBase* sb);

  util::IoContextPool* pool_;
  std::unique_ptr<FileNameQueue> file_name_q_;
  Runner* runner_;

  static thread_local std::unique_ptr<PerIoStruct> per_io_;
};

}  // namespace mr3
