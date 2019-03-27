// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/pipeline_executor.h"

#include "absl/strings/str_cat.h"
#include "base/logging.h"

#include "util/fibers/fibers_ext.h"

namespace mr3 {

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

Pipeline::Executor::PerIoStruct::PerIoStruct(unsigned i) : index(i), record_q(256) {}

thread_local std::unique_ptr<Pipeline::Executor::PerIoStruct> Pipeline::Executor::per_io_;

void Pipeline::Executor::PerIoStruct::Shutdown() {
  process_fd.join();

  // Must follow process_fd because we need first to push all the records to the queue and
  // then to signal it's closing.
  record_q.StartClosing();

  if (map_fd.joinable()) {
    map_fd.join();
  }
  do_context->Flush();
}

Pipeline::Executor::Executor(util::IoContextPool* pool, Runner* runner)
    : pool_(pool), file_name_q_(16), runner_(runner) {}

Pipeline::Executor::~Executor() {
  VLOG(1) << "Executor::~Executor";
  CHECK(file_name_q_.is_closed());
}

void Pipeline::Executor::Shutdown() {
  VLOG(1) << "Executor::Shutdown::Start";
  file_name_q_.close();

  // Use AwaitFiberOnAll because we block in the function.
  pool_->AwaitFiberOnAll([&](IoContext&) { per_io_->Shutdown(); });

  runner_->Shutdown();

  VLOG(1) << "Executor::Shutdown::End";
}

void Pipeline::Executor::Init() { runner_->Init(); }

void Pipeline::Executor::Stop() {
  file_name_q_.close();
  pool_->AwaitOnAll([&](IoContext&) { per_io_->stop_early = true; });
}

void Pipeline::Executor::Run(const std::vector<const InputBase*>& inputs, TableBase* tb) {
  // CHECK_STATUS(tb->InitializationStatus());

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));

    per_io_->process_fd = fibers::fiber{&Executor::ProcessFiles, this};
    per_io_->do_context.reset(runner_->CreateContext(tb->op()));
    per_io_->map_fd = fibers::fiber(&Executor::MapFiber, this, tb);
  });

  for (const auto& input : inputs) {
    CHECK(input && input->msg().file_spec_size() > 0);
    CHECK(input->msg().has_format());
    LOG(INFO) << "Running on input " << input->msg().name();

    auto cb = [&](const std::string& nm) {
      channel_op_status st = file_name_q_.push(FileInput{nm, &input->msg()});
      if (st !=channel_op_status::closed) {
        CHECK_EQ(channel_op_status::success, st);
      }
    };

    for (const auto& file_spec : input->msg().file_spec()) {
      runner_->ExpandGlob(file_spec.url_glob(), cb);
    }

    if (file_name_q_.is_closed())
      break;
  }
}

void Pipeline::Executor::ProcessFiles() {
  PerIoStruct* trd_local = per_io_.get();
  FileInput file_input;
  uint64_t cnt = 0;

  while (!trd_local->stop_early) {
    channel_op_status st = file_name_q_.pop(file_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    cnt += runner_->ProcessFile(file_input.first, file_input.second->format().type(),
                                &trd_local->record_q);
  }
  VLOG(1) << "ProcessFiles closing after processing " << cnt << " items";
}

void Pipeline::Executor::MapFiber(TableBase* sb) {
  auto& record_q = per_io_->record_q;
  string record;
  uint64_t record_num = 0;

  auto do_fn = sb->SetupDoFn(per_io_->do_context.get());

  while (true) {
    bool is_open = record_q.Pop(record);
    if (!is_open)
      break;

    // record is a binary input.
    // TODO: to implement binary to type to binary flow:
    // out_cntx-Deserialize<T>(record) -> T -> UDF(T) -> (Shard, U) -> Serialize(U)->string.
    // TODO: we should hold local map for sharded files.
    // if a new shard is needed, locks and accesses a central repository.
    // each sharded file is fiber-safe file.

    // We should have here Shard/string(out_record).
    do_fn(std::move(record));
    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3
