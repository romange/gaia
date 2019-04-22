// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/local_runner.h"

#include <fcntl.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "base/logging.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "file/list_file_reader.h"

#include "mr/do_context.h"
#include "mr/impl/dest_file_set.h"

#include "util/fibers/fiberqueue_threadpool.h"
#include "util/zlib_source.h"

namespace mr3 {

DEFINE_uint32(local_runner_prefetch_size, 1 << 16, "File input prefetch size");

using namespace util;
using namespace boost;
using namespace std;
using detail::DestFileSet;
using detail::DestHandle;
namespace {

class BufferedWriter {
  DestHandle* dh_;
  std::string buffer_;

  size_t writes = 0, flushes = 0;
  static constexpr size_t kFlushLimit = 1 << 13;

  void operator=(const BufferedWriter&) = delete;

 public:
  explicit BufferedWriter(DestHandle* dh) : dh_(dh) {}

  BufferedWriter(const BufferedWriter&) = delete;
  ~BufferedWriter();

  void Flush();

  void Write(StringPiece src);
};

class LocalContext : public RawContext {
 public:
  explicit LocalContext(const pb::Output& output, DestFileSet* mgr);
  ~LocalContext();

  void Flush();

 private:
  void WriteInternal(const ShardId& shard_id, std::string&& record) final;

  google::dense_hash_map<ShardId, BufferedWriter*> custom_shard_files_;
  const pb::Output& output_;
  DestFileSet* mgr_;
};

BufferedWriter::~BufferedWriter() { CHECK(buffer_.empty()); }

void BufferedWriter::Flush() {
  if (buffer_.empty())
    return;

  dh_->Write(std::move(buffer_));
}

void BufferedWriter::Write(StringPiece src) {
  buffer_.append(src.data(), src.size());

  VLOG_IF(2, ++writes % 1000 == 0) << "BufferedWrite " << writes;

  if (buffer_.size() >= kFlushLimit) {
    VLOG(2) << "Flush " << ++flushes;

    dh_->Write(std::move(buffer_));
  }
}

LocalContext::LocalContext(const pb::Output& out, DestFileSet* mgr) : output_(out), mgr_(mgr) {
  CHECK(mgr_);
  custom_shard_files_.set_empty_key(ShardId{kuint32max});
}

void LocalContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  auto it = custom_shard_files_.find(shard_id);
  if (it == custom_shard_files_.end()) {
    DestHandle* res = mgr_->Get(shard_id, output_);
    it = custom_shard_files_.emplace(shard_id, new BufferedWriter{res}).first;
  }
  record.append("\n");
  it->second->Write(record);
}

void LocalContext::Flush() {
  for (auto& k_v : custom_shard_files_) {
    k_v.second->Flush();
  }
}

LocalContext::~LocalContext() {
  for (auto& k_v : custom_shard_files_) {
    delete k_v.second;
  }
  VLOG(1) << "~LocalContextEnd";
}

}  // namespace

struct LocalRunner::Impl {
  string data_dir;
  std::unique_ptr<DestFileSet> dest_mgr;
  fibers_ext::FiberQueueThreadPool fq_pool;
  std::atomic_bool stop_signal_{false};
  std::atomic_ulong file_cache_hit_bytes{0};

  Impl(const string& d) : data_dir(d), fq_pool(0, 128) {}

  uint64_t ProcessText(file::ReadonlyFile* fd, RawSinkCb cb);
  uint64_t ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb);

  void SetupDestFileSet() {
    CHECK(!dest_mgr);
    dest_mgr.reset(new DestFileSet(data_dir, &fq_pool));
  }
};

uint64_t LocalRunner::Impl::ProcessText(file::ReadonlyFile* fd, RawSinkCb cb) {
  std::unique_ptr<util::Source> src(file::Source::Uncompressed(fd));

  file::LineReader lr(src.release(), TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;

  uint64_t cnt = 0;
  while (!stop_signal_.load(std::memory_order_relaxed) && lr.Next(&result, &scratch)) {
    string tmp{result};
    ++cnt;

    VLOG_IF(2, cnt % 1000 == 0) << "Read " << cnt << " items";
    if (cnt % 1000 == 0) {
      this_fiber::yield();
    }
    cb(false, std::move(tmp));
  }
  VLOG(1) << "ProcessText Read " << cnt << " items";
  return cnt;
}

uint64_t LocalRunner::Impl::ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb) {
  file::ListReader::CorruptionReporter error_fn = [] (size_t bytes, const util::Status& status) {
    LOG(FATAL) << "Lost " << bytes << " bytes, status: " << status;
  };

  file::ListReader list_reader(fd, TAKE_OWNERSHIP, true, error_fn);
  string scratch;
  StringPiece record;
  uint64_t cnt = 0;
  while (list_reader.ReadRecord(&record, &scratch)) {
    cb(true, string(record));
    ++cnt;
    if (cnt % 1000 == 0) {
      this_fiber::yield();
      if (stop_signal_.load(std::memory_order_relaxed)) {
        break;
      }
    }
  }
  return cnt;
}

LocalRunner::LocalRunner(const std::string& data_dir) : impl_(new Impl(data_dir)) {}

LocalRunner::~LocalRunner() {}

void LocalRunner::Init() { file_util::RecursivelyCreateDir(impl_->data_dir, 0750); }

void LocalRunner::Shutdown() {
  impl_->fq_pool.Shutdown();

  LOG(INFO) << "File cached hit bytes " << impl_->file_cache_hit_bytes.load();
}

void LocalRunner::OperatorStart() { impl_->SetupDestFileSet(); }

RawContext* LocalRunner::CreateContext(const pb::Operator& op) {
  return new LocalContext(op.output(), impl_->dest_mgr.get());
}

void LocalRunner::OperatorEnd(ShardFileMap* out_files) {
  VLOG(1) << "LocalRunner::OperatorEnd";
  impl_->dest_mgr->Flush();

  impl_->dest_mgr->GatherAll(
      [out_files](const ShardId& sid, DestHandle* dh) { out_files->emplace(sid, dh->path()); });
  impl_->dest_mgr.reset();
}

void LocalRunner::ExpandGlob(const std::string& glob, std::function<void(const std::string&)> cb) {
  std::vector<file_util::StatShort> paths = file_util::StatFiles(glob);
  for (const auto& v : paths) {
    if (v.st_mode & S_IFREG) {
      cb(v.name);
    }
  }
}

ostream& operator<<(ostream& os, const file::FiberReadOptions::Stats& stats) {
  os << stats.cache_bytes << "/" << stats.disk_bytes << "/" << stats.read_prefetch_cnt << "/"
     << stats.preempt_cnt;
  return os;
}

// Read file and fill queue. This function must be fiber-friendly.
size_t LocalRunner::ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                                     RawSinkCb cb) {
  file::FiberReadOptions::Stats stats;
  file::FiberReadOptions opts;

  opts.prefetch_size = FLAGS_local_runner_prefetch_size;
  opts.stats = &stats;

  auto fl_res = file::OpenFiberReadFile(filename, &impl_->fq_pool, opts);
  if (!fl_res.ok()) {
    LOG(DFATAL) << "Skipping " << filename << " with " << fl_res.status;
    return 0;
  }

  LOG(INFO) << "Processing file " << filename;
  std::unique_ptr<file::ReadonlyFile> read_file(fl_res.obj);
  size_t cnt = 0;
  switch (type) {
    case pb::WireFormat::TXT:
      cnt = impl_->ProcessText(read_file.release(), cb);
      break;
    case pb::WireFormat::LST:
      cnt = impl_->ProcessLst(read_file.release(), cb);
      break;
    default:
      LOG(FATAL) << "Not implemented " << pb::WireFormat::Type_Name(type);
      break;
  }

  VLOG(1) << "Read Stats (disk read/cached/read_cnt/preempts): " << stats;
  impl_->file_cache_hit_bytes.fetch_add(stats.cache_bytes, std::memory_order_relaxed);

  return cnt;
}

void LocalRunner::Stop() {
  CHECK_NOTNULL(impl_)->stop_signal_.store(true, std::memory_order_seq_cst);
}

}  // namespace mr3
