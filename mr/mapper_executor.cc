// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mapper_executor.h"

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "mr/impl/table_impl.h"
#include "mr/mr.h"
#include "util/asio/io_context_pool.h"
#include "util/fibers/fibers_ext.h"

namespace mr3 {

DEFINE_uint32(map_limit, 0, "");

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

struct MapperExecutor::PerIoStruct {
  unsigned index;
  ::boost::fibers::fiber map_fd;
  ::boost::fibers::fiber process_fd[1];

  RecordQueue record_q;
  std::unique_ptr<RawContext> do_context;
  bool stop_early = false;

  PerIoStruct(unsigned i);

  void Shutdown();
};

MapperExecutor::PerIoStruct::PerIoStruct(unsigned i) : index(i), record_q(256) {}

thread_local std::unique_ptr<MapperExecutor::PerIoStruct> MapperExecutor::per_io_;

void MapperExecutor::PerIoStruct::Shutdown() {
  for (auto& f : process_fd)
    f.join();

  // Must follow process_fd because we need first to push all the records to the queue and
  // then to signal it's closing.
  record_q.StartClosing();

  if (map_fd.joinable()) {
    map_fd.join();
  }
  do_context->Flush();
}

MapperExecutor::MapperExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {}

MapperExecutor::~MapperExecutor() {
  VLOG(1) << "Executor::~Executor";
  CHECK(!file_name_q_);
}

void MapperExecutor::Init() { runner_->Init(); }

void MapperExecutor::Stop() {
  VLOG(1) << "PipelineExecutor StopStart";

  if (file_name_q_) {
    file_name_q_->close();
    pool_->AwaitOnAll([&](IoContext&) {
      per_io_->stop_early = true;
      VLOG(1) << "StopEarly";
    });
  }
  VLOG(1) << "PipelineExecutor StopEnd";
}

void MapperExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  // CHECK_STATUS(tb->InitializationStatus());

  file_name_q_.reset(new FileNameQueue{16});
  runner_->OperatorStart();

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));

    for (auto& f : per_io_->process_fd)
      f = fibers::fiber{&MapperExecutor::ProcessInputFiles, this};

    per_io_->do_context.reset(runner_->CreateContext(tb->op()));
    per_io_->map_fd = fibers::fiber(&MapperExecutor::MapFiber, this, tb);
  });

  for (const auto& input : inputs) {
    PushInput(input);

    if (file_name_q_->is_closed())
      break;
  }

  atomic<uint64_t> parse_errs{0};
  pool_->AwaitOnAll([&](IoContext&) {
    parse_errs.fetch_add(per_io_->do_context->parse_errors, std::memory_order_relaxed);
  });
  LOG_IF(WARNING, parse_errs > 0) << tb->op().op_name() << " had " << parse_errs.load()
                                  << " errors";
  file_name_q_->close();

  // Use AwaitFiberOnAll because Shutdown() blocks the callback.
  pool_->AwaitFiberOnAll([&](IoContext&) {
    per_io_->Shutdown();
    per_io_.reset();
  });

  runner_->OperatorEnd(out_files);
  file_name_q_.reset();
}

void MapperExecutor::PushInput(const InputBase* input) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());

  vector<string> files;
  for (const auto& file_spec : input->msg().file_spec()) {
    runner_->ExpandGlob(file_spec.url_glob(), [&](const auto& str) { files.push_back(str); });
  }

  LOG(INFO) << "Running on input " << input->msg().name() << " with " << files.size() << " files";
  for (const auto& fl_name : files) {
    channel_op_status st = file_name_q_->push(FileInput{fl_name, &input->msg()});
    if (st != channel_op_status::closed) {
      CHECK_EQ(channel_op_status::success, st);
    }
  }
}

void MapperExecutor::ProcessInputFiles() {
  PerIoStruct* trd_local = per_io_.get();
  FileInput file_input;
  uint64_t cnt = 0;

  while (!trd_local->stop_early) {
    channel_op_status st = file_name_q_->pop(file_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    cnt += runner_->ProcessInputFile(
        file_input.first, file_input.second->format().type(),
        [trd_local](auto&& s) { trd_local->record_q.Push(std::move(s)); });
  }
  VLOG(1) << "ProcessInputFiles closing after processing " << cnt << " items";
}

void MapperExecutor::MapFiber(detail::TableBase* sb) {
  VLOG(1) << "Starting MapFiber";

  auto& record_q = per_io_->record_q;
  string record;
  uint64_t record_num = 0;

  std::unique_ptr<detail::HandlerWrapperBase> handler{sb->CreateHandler(per_io_->do_context.get())};
  CHECK_EQ(1, handler->Size());

  RawSinkCb cb = handler->Get(0);

  while (true) {
    bool is_open = record_q.Pop(record);
    if (!is_open)
      break;

    ++record_num;

    if (FLAGS_map_limit && record_num > FLAGS_map_limit) {
      continue;
    }

    VLOG_IF(1, record_num % 1000 == 0) << "Num maps " << record_num;

    // record is a binary input.
    // TODO: to implement binary to type to binary flow:
    // out_cntx-Deserialize<T>(record) -> T -> UDF(T) -> (Shard, U) -> Serialize(U)->string.
    // TODO: we should hold local map for sharded files.
    // if a new shard is needed, locks and accesses a central repository.
    // each sharded file is fiber-safe file.

    // We should have here Shard/string(out_record).
    cb(std::move(record));

    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3
