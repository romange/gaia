// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/local_runner.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "base/histogram.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "file/list_file_reader.h"

#include "mr/do_context.h"
#include "mr/impl/local_context.h"

#include "util/asio/io_context_pool.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/gce/gcs.h"

#include "util/zlib_source.h"

namespace mr3 {

DEFINE_uint32(local_runner_prefetch_size, 1 << 16, "File input prefetch size");

using namespace util;
using namespace boost;
using namespace std;
using detail::DestFileSet;
using detail::DestHandle;

constexpr size_t kGcsConnectTimeout = 3000;
namespace {

using namespace intrusive;


}  // namespace

struct LocalRunner::Impl {
 public:
  Impl(IoContextPool* p, const string& d) : io_pool(p), data_dir(d), fq_pool(0, 128) {}

  uint64_t ProcessText(file::ReadonlyFile* fd, RawSinkCb cb);
  uint64_t ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb);

  void Start(const pb::Operator* op);
  void End(ShardFileMap* out_files);

  void ExpandGCS(absl::string_view glob, ExpandCb cb);

  util::StatusObject<file::ReadonlyFile*> OpenReadFile(const std::string& filename,
                                                       file::FiberReadOptions::Stats* stats);

  void ShutDown();

  // private:

  void LazyGcsInit();

  IoContextPool* io_pool;
  string data_dir;
  std::unique_ptr<DestFileSet> dest_mgr;
  fibers_ext::FiberQueueThreadPool fq_pool;
  std::atomic_bool stop_signal_{false};
  std::atomic_ulong file_cache_hit_bytes{0};
  const pb::Operator* current_op = nullptr;

  fibers::mutex gce_mu;
  std::unique_ptr<GCE> gce_handle;

 private:
  struct PerThread {
    vector<unique_ptr<GCS>> gcs_handles;
    // fibers::fiber_specific_ptr<> fs_gcs_handle;
    base::Histogram record_fetch_hist;
  };

  struct handle_keeper {
    PerThread* per_thread;

    handle_keeper(PerThread* pt) : per_thread(pt) {}

    void operator()(GCS* gcs) {
      per_thread->gcs_handles.emplace_back(gcs);
    }
  };

  unique_ptr<GCS, handle_keeper> GetGcsHandle();


  static thread_local std::unique_ptr<PerThread> per_thread_;
};

thread_local std::unique_ptr<LocalRunner::Impl::PerThread> LocalRunner::Impl::per_thread_;

auto LocalRunner::Impl::GetGcsHandle() -> unique_ptr<GCS, handle_keeper> {
  auto* pt = per_thread_.get();
  CHECK(pt);

  for (auto it = pt->gcs_handles.begin(); it != pt->gcs_handles.end(); ++it) {
    if ((*it)->IsOpenSequential()) {
      continue;
    }

    auto res = std::move(*it);
    it->swap(pt->gcs_handles.back());
    pt->gcs_handles.pop_back();

    return unique_ptr<GCS, handle_keeper>(res.release(), pt);
  }

  IoContext* io_context = io_pool->GetThisContext();
  CHECK(io_context) << "Must run from IO context thread";
  GCS* gcs = new GCS(*gce_handle, io_context);
  auto status = gcs->Connect(kGcsConnectTimeout);
  CHECK_STATUS(status);

  return unique_ptr<GCS, handle_keeper>(gcs, pt);
}

uint64_t LocalRunner::Impl::ProcessText(file::ReadonlyFile* fd, RawSinkCb cb) {
  std::unique_ptr<util::Source> src(file::Source::Uncompressed(fd));

  file::LineReader lr(src.release(), TAKE_OWNERSHIP);
  StringPiece result;
  string scratch;

  uint64_t cnt = 0;
  uint64_t start = base::GetMonotonicMicrosFast();
  while (!stop_signal_.load(std::memory_order_relaxed) && lr.Next(&result, &scratch)) {
    string tmp{result};
    ++cnt;
    if (VLOG_IS_ON(1)) {
      int64_t delta = base::GetMonotonicMicrosFast() - start;
      if (delta > 5)  // Filter out uninteresting fast Next calls.
        per_thread_->record_fetch_hist.Add(delta);
    }
    VLOG_IF(2, cnt % 1000 == 0) << "Read " << cnt << " items";
    if (cnt % 1000 == 0) {
      this_fiber::yield();
    }
    cb(std::move(tmp));
    start = base::GetMonotonicMicrosFast();
  }
  VLOG(1) << "ProcessText Read " << cnt << " items";

  return cnt;
}

uint64_t LocalRunner::Impl::ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb) {
  file::ListReader::CorruptionReporter error_fn = [](size_t bytes, const util::Status& status) {
    LOG(FATAL) << "Lost " << bytes << " bytes, status: " << status;
  };

  file::ListReader list_reader(fd, TAKE_OWNERSHIP, true, error_fn);
  string scratch;
  StringPiece record;
  uint64_t cnt = 0;
  while (list_reader.ReadRecord(&record, &scratch)) {
    cb(string(record));
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

void LocalRunner::Impl::Start(const pb::Operator* op) {
  CHECK(!dest_mgr);
  current_op = op;
  string out_dir = file_util::JoinPath(data_dir, op->output().name());
  if (!file::Exists(out_dir)) {
    CHECK(file_util::RecursivelyCreateDir(out_dir, 0750)) << "Could not create dir " << out_dir;
  }
  dest_mgr.reset(new DestFileSet(out_dir, op->output(), &fq_pool));
}

void LocalRunner::Impl::End(ShardFileMap* out_files) {
  auto shards = dest_mgr->GetShards();
  for (const ShardId& sid : shards) {
    out_files->emplace(sid, dest_mgr->ShardFilePath(sid, -1));
  }
  dest_mgr->CloseAllHandles();
  dest_mgr.reset();
  current_op = nullptr;
}

void LocalRunner::Impl::ExpandGCS(absl::string_view glob, ExpandCb cb) {
  absl::string_view bucket, path;
  CHECK(GCS::SplitToBucketPath(glob, &bucket, &path));

  // Lazy init of gce_handle.
  LazyGcsInit();

  auto cb2 = [cb = std::move(cb), bucket](size_t sz, absl::string_view s) {
    cb(sz, GCS::ToGcsPath(bucket, s));
  };

  // std::lock_guard<fibers::mutex> lk(per_thread_->gcs_handle->mu);
  auto gcs = GetGcsHandle();
  auto status = gcs->List(bucket, path, true, cb2);
  CHECK_STATUS(status);
}

util::StatusObject<file::ReadonlyFile*> LocalRunner::Impl::OpenReadFile(
    const std::string& filename, file::FiberReadOptions::Stats* stats) {
  if (!per_thread_) {
    per_thread_.reset(new PerThread);
  }

  if (util::IsGcsPath(filename)) {
    LazyGcsInit();

    auto gcs = GetGcsHandle();
    return gcs->OpenGcsFile(filename);
  }

  file::FiberReadOptions opts;
  opts.prefetch_size = FLAGS_local_runner_prefetch_size;
  opts.stats = stats;

  return file::OpenFiberReadFile(filename, &fq_pool, opts);
}

void LocalRunner::Impl::LazyGcsInit() {
  if (!per_thread_) {
    per_thread_.reset(new PerThread);
  }
  {
    std::lock_guard<fibers::mutex> lk(gce_mu);
    if (!gce_handle) {
      gce_handle.reset(new GCE);
      CHECK_STATUS(gce_handle->Init());
    }
  }
}

void LocalRunner::Impl::ShutDown() {
  if (per_thread_) {
    VLOG(1) << "Histogram Latency: " << per_thread_->record_fetch_hist.ToString();

    per_thread_->gcs_handles.clear();
  }
}

/* LocalRunner implementation
********************************************/

LocalRunner::LocalRunner(IoContextPool* pool, const std::string& data_dir)
    : impl_(new Impl(pool, data_dir)) {}

LocalRunner::~LocalRunner() {}

void LocalRunner::Init() { file_util::RecursivelyCreateDir(impl_->data_dir, 0750); }

void LocalRunner::Shutdown() {
  impl_->fq_pool.Shutdown();
  impl_->io_pool->AwaitFiberOnAll([this](IoContext&) { impl_->ShutDown(); });

  LOG(INFO) << "File cached hit bytes " << impl_->file_cache_hit_bytes.load();
}

void LocalRunner::OperatorStart(const pb::Operator* op) { impl_->Start(op); }

RawContext* LocalRunner::CreateContext() {
  CHECK_NOTNULL(impl_->current_op);

  return new detail::LocalContext(impl_->dest_mgr.get());
}

void LocalRunner::OperatorEnd(ShardFileMap* out_files) {
  VLOG(1) << "LocalRunner::OperatorEnd";
  impl_->End(out_files);
}

void LocalRunner::ExpandGlob(const std::string& glob, ExpandCb cb) {
  if (util::IsGcsPath(glob)) {
    impl_->ExpandGCS(glob, cb);
    return;
  }

  std::vector<file_util::StatShort> paths = file_util::StatFiles(glob);
  for (const auto& v : paths) {
    if (v.st_mode & S_IFREG) {
      cb(v.size, v.name);
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
  auto fl_res = impl_->OpenReadFile(filename, &stats);
  if (!fl_res.ok()) {
    LOG(FATAL) << "Skipping " << filename << " with " << fl_res.status;
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
