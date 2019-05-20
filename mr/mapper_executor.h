// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/fiber/buffered_channel.hpp>
#include <functional>

#include "mr/operator_executor.h"
#include "util/fibers/simple_channel.h"
#include "util/stats/varz_value.h"

namespace mr3 {

class MapperExecutor : public OperatorExecutor {
  using FileInput = std::pair<std::string, const pb::Input*>;
  using FileNameQueue = ::boost::fibers::buffered_channel<FileInput>;

  struct Record {
    enum Operand { BINARY_FORMAT, TEXT_FORMAT, RECORD} op;

    std::string data;
  };

  using RecordQueue = util::fibers_ext::SimpleChannel<Record>;

  struct PerIoStruct;

 public:
  MapperExecutor(util::IoContextPool* pool, Runner* runner);
  ~MapperExecutor();

  void Init() final;

  void Run(const std::vector<const InputBase*>& inputs, detail::TableBase* ss,
           ShardFileMap* out_files) final;

  // Stops the executor in the middle.
  void Stop() final;

 private:
  void PushInput(const InputBase*);

  // Input managing fiber that reads files from disk and pumps data into record_q.
  // One per IO thread.
  void ProcessInputFiles(detail::TableBase* tb);

  static void MapFiber(RecordQueue* record_q, detail::HandlerWrapperBase* hwb);
  util::VarzValue::Map GetStats() const;

  std::unique_ptr<FileNameQueue> file_name_q_;

  static thread_local std::unique_ptr<PerIoStruct> per_io_;
};

}  // namespace mr3
