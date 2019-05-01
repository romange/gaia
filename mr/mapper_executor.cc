// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mapper_executor.h"

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "mr/impl/table_impl.h"
#include "mr/ptable.h"

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
  ::boost::fibers::fiber process_fd;

  bool stop_early = false;

  PerIoStruct(unsigned i);

  void Shutdown();
};

MapperExecutor::PerIoStruct::PerIoStruct(unsigned i) : index(i) {}

thread_local std::unique_ptr<MapperExecutor::PerIoStruct> MapperExecutor::per_io_;

void MapperExecutor::PerIoStruct::Shutdown() { process_fd.join(); }

MapperExecutor::MapperExecutor(util::IoContextPool* pool, Runner* runner)
    : OperatorExecutor(pool, runner) {}

MapperExecutor::~MapperExecutor() { CHECK(!file_name_q_); }

void MapperExecutor::Init() { runner_->Init(); }

void MapperExecutor::Stop() {
  VLOG(1) << "MapperExecutor Stop[";

  // small race condition at the end, not important since this function called only on SIGTERM
  if (file_name_q_) {
    file_name_q_->close();
    pool_->AwaitOnAll([&](IoContext&) {
      if (per_io_) {  // "file_name_q_->close();"" might cause per_io be already freed.
        per_io_->stop_early = true;
      }
      VLOG(1) << "StopEarly";
    });
  }
  VLOG(1) << "MapperExecutor Stop]";
}

void MapperExecutor::Run(const std::vector<const InputBase*>& inputs, detail::TableBase* tb,
                         ShardFileMap* out_files) {
  // CHECK_STATUS(tb->InitializationStatus());
  file_name_q_.reset(new FileNameQueue{16});
  runner_->OperatorStart();

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    per_io_->process_fd = fibers::fiber{&MapperExecutor::ProcessInputFiles, this, tb};
  });

  for (const auto& input : inputs) {
    PushInput(input);

    if (file_name_q_->is_closed())
      break;
  }

  file_name_q_->close();

  // Use AwaitFiberOnAll because Shutdown() blocks the callback.
  pool_->AwaitFiberOnAll([&](IoContext&) {
    per_io_->Shutdown();
    per_io_.reset();
  });

  const string& op_name = tb->op().op_name();
  LOG(INFO) << op_name << " had " << do_fn_calls_.load() << " calls";
  LOG_IF(WARNING, parse_errors_ > 0)
      << op_name << " had " << parse_errors_.load() << " errors";
  for (const auto& k_v : counter_map_) {
    LOG(INFO) << op_name << "-" << k_v.first << ": " << k_v.second;
  }

  runner_->OperatorEnd(out_files);
  file_name_q_.reset();
}

void MapperExecutor::PushInput(const InputBase* input) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());

  vector<string> files;
  pool_->GetNextContext().AwaitSafe([&] {
    for (const auto& file_spec : input->msg().file_spec()) {
      runner_->ExpandGlob(file_spec.url_glob(), [&](const auto& str) { files.push_back(str); });
    }
  });

  LOG(INFO) << "Running on input " << input->msg().name() << " with " << files.size() << " files";
  for (const auto& fl_name : files) {
    channel_op_status st = file_name_q_->push(FileInput{fl_name, &input->msg()});
    if (st != channel_op_status::closed) {
      CHECK_EQ(channel_op_status::success, st);
    }
  }
}

void MapperExecutor::ProcessInputFiles(detail::TableBase* tb) {
  PerIoStruct* trd_local = per_io_.get();
  FileInput file_input;
  uint64_t cnt = 0;

  std::unique_ptr<RawContext> raw_context(runner_->CreateContext(tb->op()));
  std::unique_ptr<detail::HandlerWrapperBase> handler{tb->CreateHandler(raw_context.get())};
  CHECK_EQ(1, handler->Size());

  RecordQueue record_q(256);

  fibers::fiber map_fd(&MapperExecutor::MapFiber, &record_q, handler->Get(0));

  VLOG(1) << "Starting MapFiber on " << tb->op().output().DebugString();

  while (!trd_local->stop_early) {
    channel_op_status st = file_name_q_->pop(file_input);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    const pb::Input* pb_input = file_input.second;
    auto cb = [&record_q, skip = pb_input->skip_header(),
               record_num = uint64_t{0}](bool is_binary, auto&& s) mutable {
      if (record_num++ < skip)
        return;
      record_q.Push(is_binary, std::move(s));
    };

    cnt += runner_->ProcessInputFile(file_input.first, pb_input->format().type(), cb);
  }
  VLOG(1) << "ProcessInputFiles closing after processing " << cnt << " items";

  // Must follow process_fd because we need first to push all the records to the queue and
  // then to signal it's closing.
  record_q.StartClosing();

  map_fd.join();
  handler->OnShardFinish();
  FinalizeContext(cnt, raw_context.get());
}

void MapperExecutor::MapFiber(RecordQueue* record_q, RawSinkCb cb) {
  std::pair<bool, string> record;
  uint64_t record_num = 0;

  while (true) {
    bool is_open = record_q->Pop(record);
    if (!is_open)
      break;

    ++record_num;

    // TODO: to pass it as argument to Runner::ProcessInputFile.
    if (FLAGS_map_limit && record_num > FLAGS_map_limit) {
      continue;
    }

    VLOG_IF(1, record_num % 1000 == 0) << "Num maps " << record_num;

    cb(record.first, std::move(record.second));

    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3
