// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr_executor.h"

#include <mutex>

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"

#include "util/fibers/fibers_ext.h"

namespace mr3 {
using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

auto GlobalDestFileManager::Get(StringPiece key) -> Result {
  std::lock_guard<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(key);
  if (it == dest_files_.end()) {
    string file_name = absl::StrCat(key, ".txt");
    key = str_db_.Get(key);
    string fn = file_util::JoinPath(root_dir_, file_name);
    auto res = dest_files_.emplace(key, file::OpenFiberWriteFile(fn, fq_));
    CHECK(res.second && res.first->second);
    it = res.first;
  }

  return Result(it->first, it->second);
}

struct ExecutorContext::Dest {
  file::WriteFile* wr;
  std::string buffer;

  static constexpr size_t kFlushLimit = 1 << 16;
  void operator=(const Dest&) = delete;

  explicit Dest(file::WriteFile* wf) : wr(wf) {}
  Dest(const Dest&) = delete;

  ~Dest();

  void Write(std::initializer_list<StringPiece> src);
};

ExecutorContext::Dest::~Dest() {
  if (!buffer.empty()) {
    CHECK_STATUS(wr->Write(buffer));
  }
}

void ExecutorContext::Dest::Write(std::initializer_list<StringPiece> src) {
  for (auto v : src) {
    buffer.append(v.data(), v.size());
  }
  if (buffer.size() >= kFlushLimit) {
    CHECK_STATUS(wr->Write(buffer));
    buffer.clear();
  }
}

ExecutorContext::ExecutorContext(const pb::Output& out, GlobalDestFileManager* mgr) : mgr_(mgr) {
  custom_shard_files_.set_empty_key(StringPiece{});
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
}

void ExecutorContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  CHECK(absl::holds_alternative<string>(shard_id));

  const string& shard_name = absl::get<string>(shard_id);

  auto it = custom_shard_files_.find(shard_name);
  if (it == custom_shard_files_.end()) {
    auto res = mgr_->Get(shard_name);

    it = custom_shard_files_.emplace(res.first, new Dest{res.second}).first;
  }
  Dest* dest = it->second;
  dest->Write({record, "\n"});
}

ExecutorContext::~ExecutorContext() {
  for (auto& k_v : custom_shard_files_) {
    delete k_v.second;
  }
  VLOG(1) << "~ExecutorContextEnd";
}

struct Executor::PerIoStruct {
  unsigned index;
  fibers::fiber map_fd, process_fd;
  StringQueue record_q;
  std::unique_ptr<ExecutorContext> do_context;

  PerIoStruct(unsigned i);

  void Shutdown();
};

Executor::PerIoStruct::PerIoStruct(unsigned i) : index(i), record_q(256) {}


thread_local std::unique_ptr<Executor::PerIoStruct> Executor::per_io_;


void Executor::PerIoStruct::Shutdown() {
  process_fd.join();

  if (map_fd.joinable()) {
    map_fd.join();
  }
  do_context.reset();  // must be closed before fq_pool_ shutdown.
}

Executor::Executor(const std::string& root_dir, util::IoContextPool* pool)
    : root_dir_(root_dir), pool_(pool), file_name_q_(16),
      fq_pool_(new fibers_ext::FiberQueueThreadPool(0, 128)) {}

Executor::~Executor() {
  VLOG(1) << "Executor::~Executor";
  CHECK(file_name_q_.is_closed());
}

void Executor::Shutdown() {
  VLOG(1) << "Executor::Shutdown::Start";
  file_name_q_.close();

  // Use AwaitFiberOnAll because we block in the function.
  pool_->AwaitFiberOnAll([&](IoContext&) { per_io_->Shutdown(); });
  dest_mgr_.reset();

  fq_pool_->Shutdown();
  VLOG(1) << "Executor::Shutdown::End";
}

void Executor::Init() { file_util::RecursivelyCreateDir(root_dir_, 0750); }

void Executor::Run(const InputBase* input, StringStream* ss) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());
  CHECK_STATUS(ss->InitializationStatus());

  LOG(INFO) << "Running on input " << input->msg().name();

  dest_mgr_.reset(new GlobalDestFileManager(root_dir_, fq_pool_.get()));

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    per_io_->process_fd =
        fibers::fiber{&Executor::ProcessFiles, this, input->msg().format().type()};
    per_io_->do_context.reset(new ExecutorContext(ss->output().msg(), dest_mgr_.get()));
    per_io_->map_fd = fibers::fiber(&Executor::MapFiber, this, ss);
  });

  for (const auto& file_spec : input->msg().file_spec()) {
    std::vector<file_util::StatShort> paths = file_util::StatFiles(file_spec.url_glob());
    for (const auto& v : paths) {
      if (v.st_mode & S_IFREG) {
        channel_op_status st = file_name_q_.push(v.name);
        CHECK_EQ(channel_op_status::success, st);
      }
    }
  }
}

void Executor::ProcessFiles(pb::WireFormat::Type input_type) {
  PerIoStruct* rec = per_io_.get();
  string file_name;
  uint64_t cnt = 0;
  while (true) {
    channel_op_status st = file_name_q_.pop(file_name);
    if (st == channel_op_status::closed)
      break;

    CHECK_EQ(channel_op_status::success, st);
    auto res = file::OpenFiberReadFile(file_name, fq_pool_.get());
    if (!res.ok()) {
      LOG(DFATAL) << "Skipping " << file_name << " with " << res.status;
      continue;
    }
    LOG(INFO) << "Processing file " << file_name;
    std::unique_ptr<file::ReadonlyFile> read_file(res.obj);

    switch (input_type) {
      case pb::WireFormat::TXT:
        cnt += ProcessText(read_file.release());
        break;

      default:
        LOG(FATAL) << "Not implemented " << pb::WireFormat::Type_Name(input_type);
        break;
    }
  }
  VLOG(1) << "ProcessFiles closing after processing " << cnt << " items";
  rec->record_q.StartClosing();
}

uint64_t Executor::ProcessText(file::ReadonlyFile* fd) {
  PerIoStruct* record = per_io_.get();
  std::unique_ptr<util::Source> src(file::Source::Uncompressed(fd));

  file::LineReader lr(src.release(), TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;
  uint64_t cnt = 0;
  while (lr.Next(&result, &scratch)) {
    string tmp{result};
    ++cnt;
    record->record_q.Push(std::move(tmp));
  }
  return cnt;
}

void Executor::MapFiber(StreamBase* sb) {
  auto& record_q = per_io_->record_q;
  string record;
  uint64_t record_num = 0;
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
    sb->Do(std::move(record), per_io_->do_context.get());
    if (++record_num % 1000 == 0) {
      this_fiber::yield();
    }
  }
  VLOG(1) << "MapFiber finished " << record_num;
}

}  // namespace mr3
