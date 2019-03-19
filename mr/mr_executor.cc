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
#include "file/gzip_file.h"

#include "util/fibers/fibers_ext.h"

namespace mr3 {
using namespace std;
using namespace boost;
using namespace util;

using fibers::channel_op_status;

static string FileName(StringPiece base, const pb::Output& out) {
  CHECK_EQ(out.format().type(), pb::WireFormat::TXT);
  string res(base);
  absl::StrAppend(&res, ".txt");
  if (out.has_compress()) {
    if (out.compress().type() == pb::Output::GZIP) {
      absl::StrAppend(&res, ".gz");
    } else {
      LOG(FATAL) << "Not supported " << out.compress().ShortDebugString();
    }
  }
  return res;
}

static file::WriteFile* CreateFile(const std::string& path, const pb::Output& out,
                                   fibers_ext::FiberQueueThreadPool* fq) {
  std::function<file::WriteFile*()> cb;
  if (out.has_compress()) {
    if (out.compress().type() == pb::Output::GZIP) {
      file::WriteFile* gzres{file::GzipFile::Create(path, out.compress().level())};
      cb = [gzres] {
        CHECK(gzres->Open());
        return gzres;
      };
    } else {
      LOG(FATAL) << "Not supported " << out.compress().ShortDebugString();
    }
  } else {
    cb = [&] { return file::Open(path); };
  }
  file::WriteFile* res = fq->Await(std::move(cb));
  CHECK(res);
  return res;
}

GlobalDestFileManager::GlobalDestFileManager(const std::string& root_dir, const pb::Output& out,
                                             fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), out_(out), fq_(fq) {
  dest_files_.set_empty_key(StringPiece());
  CHECK(out.has_shard_type() && out.shard_type() == pb::Output::USER_DEFINED);
}

auto GlobalDestFileManager::Get(StringPiece key) -> Result {
  std::lock_guard<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(key);
  if (it == dest_files_.end()) {
    string file_name = FileName(key, out_);
    key = str_db_.Get(key);
    string full_path = file_util::JoinPath(root_dir_, file_name);
    auto res = dest_files_.emplace(key, CreateFile(full_path, out_, fq_));
    CHECK(res.second && res.first->second);
    it = res.first;
  }

  return Result(it->first, it->second);
}

GlobalDestFileManager::~GlobalDestFileManager() {
  for (auto& k_v : dest_files_) {
    CHECK(k_v.second->Close());
  }
}

struct ExecutorContext::BufferedWriter {
  file::WriteFile* wr;
  std::string buffer;
  unsigned index;

  static constexpr size_t kFlushLimit = 1 << 16;
  void operator=(const BufferedWriter&) = delete;

  explicit BufferedWriter(file::WriteFile* wf, unsigned i) : wr(wf), index(i) {}
  BufferedWriter(const BufferedWriter&) = delete;

  void Flush(fibers_ext::FiberQueueThreadPool* fq) {
    if (buffer.empty())
      return;
    auto status = fq->Await(index, [b = std::move(buffer), wr = this->wr] { return wr->Write(b); });
    CHECK_STATUS(status);
  }

  ~BufferedWriter();

  void Write(std::initializer_list<StringPiece> src, fibers_ext::FiberQueueThreadPool* fq);
};

ExecutorContext::BufferedWriter::~BufferedWriter() { CHECK(buffer.empty()); }

void ExecutorContext::BufferedWriter::Write(std::initializer_list<StringPiece> src,
                                            fibers_ext::FiberQueueThreadPool* fq) {
  for (auto v : src) {
    buffer.append(v.data(), v.size());
  }
  if (buffer.size() >= kFlushLimit) {
    fq->Add(index, [b = std::move(buffer), wr = this->wr] {
      auto status = wr->Write(b);
      CHECK_STATUS(status);
    });
  }
}

ExecutorContext::ExecutorContext(GlobalDestFileManager* mgr) : mgr_(mgr) {
  custom_shard_files_.set_empty_key(StringPiece{});
}

void ExecutorContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  CHECK(absl::holds_alternative<string>(shard_id));

  const string& shard_name = absl::get<string>(shard_id);

  auto it = custom_shard_files_.find(shard_name);
  if (it == custom_shard_files_.end()) {
    auto res = mgr_->Get(shard_name);
    StringPiece key = res.first;

    unsigned index =
        base::MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(key.data()), key.size(), 1);
    it = custom_shard_files_.emplace(res.first, new BufferedWriter{res.second, index}).first;
  }
  it->second->Write({record, "\n"}, mgr_->pool());
}

void ExecutorContext::Flush() {
  for (auto& k_v : custom_shard_files_) {
    k_v.second->Flush(mgr_->pool());
  }
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
  bool stop_early = false;

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
  do_context->Flush();
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

void Executor::Stop() {
  pool_->AwaitOnAll([&](IoContext&) {
    per_io_->stop_early = true;
  });
}

void Executor::Run(const InputBase* input, TableBase* tb) {
  CHECK(input && input->msg().file_spec_size() > 0);
  CHECK(input->msg().has_format());
  CHECK_STATUS(tb->InitializationStatus());

  LOG(INFO) << "Running on input " << input->msg().name();

  dest_mgr_.reset(new GlobalDestFileManager(root_dir_, tb->op().output(), fq_pool_.get()));

  // As long as we do not block in the function we can use AwaitOnAll.
  pool_->AwaitOnAll([&](unsigned index, IoContext&) {
    per_io_.reset(new PerIoStruct(index));
    per_io_->process_fd =
        fibers::fiber{&Executor::ProcessFiles, this, input->msg().format().type()};
    per_io_->do_context.reset(new ExecutorContext(dest_mgr_.get()));
    per_io_->map_fd = fibers::fiber(&Executor::MapFiber, this, tb);
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
  PerIoStruct* trd_local = per_io_.get();
  string file_name;
  uint64_t cnt = 0;

  while (!trd_local->stop_early) {
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
  trd_local->record_q.StartClosing();
}

uint64_t Executor::ProcessText(file::ReadonlyFile* fd) {
  PerIoStruct* trd_local = per_io_.get();
  std::unique_ptr<util::Source> src(file::Source::Uncompressed(fd));

  file::LineReader lr(src.release(), TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;
  uint64_t cnt = 0;
  while (!trd_local->stop_early && lr.Next(&result, &scratch)) {
    string tmp{result};
    ++cnt;
    trd_local->record_q.Push(std::move(tmp));
  }
  return cnt;
}

void Executor::MapFiber(TableBase* sb) {
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
