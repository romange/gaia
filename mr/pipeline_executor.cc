// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/pipeline_executor.h"

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "mr/impl/table_impl.h"
#include "util/fibers/fibers_ext.h"

namespace mr3 {

DEFINE_uint32(map_limit, 0, "");

using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

namespace {

ShardId GetShard(const pb::Input::FileSpec& fspec) {
  if (fspec.has_shard_id())
    return ShardId{fspec.shard_id()};
  return ShardId{fspec.custom_shard_id()};
}

}  // namespace

struct MapperExecutor::PerIoStruct {
  unsigned index;
  ::boost::fibers::fiber map_fd;
  ::boost::fibers::fiber process_fd[1];

  StringQueue record_q;
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

  bool is_join = tb->op().type() == pb::Operator::HASH_JOIN;

  if (is_join) {
    CHECK_GT(inputs.size(), 1);
    uint32 modn = 0;
    for (const auto& input : inputs) {
      const pb::Output* linked_outp = input->linked_outp();
      CHECK(linked_outp && linked_outp->has_shard_spec());
      CHECK_EQ(linked_outp->shard_spec().type(), pb::ShardSpec::MODN);
      if (!modn) {
        modn = linked_outp->shard_spec().modn();
      } else {
        CHECK_EQ(modn, linked_outp->shard_spec().modn());
      }

      for (const auto& fspec : input->msg().file_spec()) {
        CHECK_GT(fspec.shard_id_ref_case(), 0);  // all inputs have sharding info.
      }
    }

    using IndexedInput = std::pair<uint32_t, const pb::Input::FileSpec*>;
    absl::flat_hash_map<ShardId, std::vector<IndexedInput>> joined_shards;
    for (size_t i = 0; i < inputs.size(); ++i) {
      for (const auto& fspec : inputs[i]->msg().file_spec()) {
        ShardId sid = GetShard(fspec);
        joined_shards[sid].emplace_back(i, &fspec);
      }
    }

  } else {
    for (const auto& input : inputs) {
      PushInput(input);

      if (file_name_q_->is_closed())
        break;
    }
  }

  atomic<uint64_t> parse_errs{0};
  pool_->AwaitOnAll([&](IoContext&) {
    parse_errs.fetch_add(per_io_->do_context->parse_errors, std::memory_order_relaxed);
  });
  LOG_IF(WARNING, parse_errs > 0) << tb->op().op_name() << " had " << parse_errs << " errors";
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
    cnt += runner_->ProcessInputFile(file_input.first, file_input.second->format().type(),
                                     &trd_local->record_q);
  }
  VLOG(1) << "ProcessInputFiles closing after processing " << cnt << " items";
}

void MapperExecutor::MapFiber(detail::TableBase* sb) {
  VLOG(1) << "Starting MapFiber";
  auto& record_q = per_io_->record_q;
  string record;
  uint64_t record_num = 0;

  auto do_fn = sb->SetupDoFn(per_io_->do_context.get());

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
    do_fn(std::move(record));
    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3
