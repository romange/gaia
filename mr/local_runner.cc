// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/local_runner.h"

#include "absl/strings/match.h"
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
#include "util/http/https_client_pool.h"

#include "util/stats/varz_stats.h"
#include "util/zlib_source.h"

namespace mr3 {

DEFINE_uint32(local_runner_prefetch_size, 1 << 16, "File input prefetch size");
DEFINE_bool(local_runner_gcs_read_v2, true, "If true, use new gcs_read_file");

DECLARE_uint32(gcs_connect_deadline_ms);

using namespace util;
using namespace boost;
using namespace std;
using detail::DestFileSet;
using http::HttpsClientPool;

namespace {

using namespace intrusive;

}  // namespace

ostream& operator<<(ostream& os, const file::FiberReadOptions::Stats& stats) {
  os << stats.cache_bytes << "/" << stats.disk_bytes << "/" << stats.read_prefetch_cnt << "/"
     << stats.preempt_cnt;
  return os;
}

struct LocalRunner::Impl {
 public:
  Impl(IoContextPool* p, const string& d)
      : io_pool_(p), data_dir(d), fq_pool_(0, 128),
        varz_stats_("local-runner", [this] { return GetStats(); }) {}

  uint64_t ProcessText(const string& fname,  file::ReadonlyFile* fd, RawSinkCb cb);
  uint64_t ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb);

  /// Called from the main thread orchestrating the pipeline run.
  void Start(const pb::Operator* op);

  /// Called from the main thread orchestrating the pipeline run.
  void End(ShardFileMap* out_files);

  /// The functions below are called from IO threads.
  void ExpandGCS(absl::string_view glob, ExpandCb cb);

  StatusObject<file::ReadonlyFile*> OpenLocalFile(const std::string& filename,
                                                  file::FiberReadOptions::Stats* stats);

  StatusObject<file::ReadonlyFile*> OpenGcsFile(const std::string& filename);
  void ShutDown();

  RawContext* NewContext();

  void Break() { stop_signal_.store(true, std::memory_order_seq_cst); }

  class Source;

 private:
  void LazyGcsInit();   // Called from IO threads.

  util::VarzValue::Map GetStats() const;

  IoContextPool* io_pool_;
  string data_dir;
  fibers_ext::FiberQueueThreadPool fq_pool_;
  std::atomic_bool stop_signal_{false};
  std::atomic_ulong file_cache_hit_bytes_{0}, input_gcs_conn_{0};
  const pb::Operator* current_op_ = nullptr;

  fibers::mutex gce_mu_;
  std::unique_ptr<GCE> gce_handle_;

  struct PerThread {
    vector<unique_ptr<GCS>> gcs_handles;

    base::Histogram record_fetch_hist;

    absl::optional<asio::ssl::context> ssl_context;
    absl::optional<HttpsClientPool> api_conn_pool;

    void SetupGce(IoContext* io_context);
  };

  struct handle_keeper {
    PerThread* per_thread;

    handle_keeper(PerThread* pt) : per_thread(pt) {}

    void operator()(GCS* gcs) { per_thread->gcs_handles.emplace_back(gcs); }
  };

  unique_ptr<GCS, handle_keeper> GetGcsHandle();

  static thread_local std::unique_ptr<PerThread> per_thread_;
  util::VarzFunction varz_stats_;

  mutable std::mutex dest_mgr_mu_;
  std::unique_ptr<DestFileSet> dest_mgr_;

  friend class Source;
};

class LocalRunner::Impl::Source {
 public:
  Source(LocalRunner::Impl* impl, const string& fn) : impl_(impl), fname_(fn) {}

  Status Open();

  size_t Process(pb::WireFormat::Type type, RawSinkCb cb);

 private:
  LocalRunner::Impl* impl_;
  const string fname_;
  file::FiberReadOptions::Stats stats_;

  std::unique_ptr<file::ReadonlyFile> rd_file_;
  bool is_gcs_ = false;
};

thread_local std::unique_ptr<LocalRunner::Impl::PerThread> LocalRunner::Impl::per_thread_;

Status LocalRunner::Impl::Source::Open() {
  is_gcs_ = IsGcsPath(fname_);

  StatusObject<file::ReadonlyFile*> fl_res;
  if (is_gcs_) {
    fl_res = impl_->OpenGcsFile(fname_);
  } else {
    fl_res = impl_->OpenLocalFile(fname_, &stats_);
  }

  if (fl_res.ok()) {
    rd_file_.reset(fl_res.obj);
  }
  return fl_res.status;
}

size_t LocalRunner::Impl::Source::Process(pb::WireFormat::Type type, RawSinkCb cb) {
  LOG(INFO) << "Processing file " << fname_;

  size_t cnt = 0;
  switch (type) {
    case pb::WireFormat::TXT:
      cnt = impl_->ProcessText(fname_, rd_file_.release(), cb);
      break;
    case pb::WireFormat::LST:
      cnt = impl_->ProcessLst(rd_file_.release(), cb);
      break;
    default:
      LOG(FATAL) << "Not implemented " << pb::WireFormat::Type_Name(type);
      break;
  }

  if (is_gcs_) {
    impl_->input_gcs_conn_.fetch_sub(1, std::memory_order_acq_rel);
  } else {  // local file
    VLOG(1) << "Read Stats (disk read/cached/read_cnt/preempts): " << stats_;

    impl_->file_cache_hit_bytes_.fetch_add(stats_.cache_bytes, std::memory_order_relaxed);
  }

  return cnt;
}

void LocalRunner::Impl::PerThread::SetupGce(IoContext* io_context) {
  if (ssl_context) {
    return;
  }
  CHECK(io_context);

  ssl_context = GCE::CheckedSslContext();
  api_conn_pool.emplace(GCE::kApiDomain, &ssl_context.value(), io_context);
  api_conn_pool->set_connect_timeout(FLAGS_gcs_connect_deadline_ms);
  api_conn_pool->set_retry_count(3);
}

auto LocalRunner::Impl::GetGcsHandle() -> unique_ptr<GCS, handle_keeper> {
  auto* pt = per_thread_.get();
  CHECK(pt);
  VLOG(1) << "GetGcsHandle: " << pt->gcs_handles.size();

  CHECK(pt->ssl_context.has_value());

  for (auto it = pt->gcs_handles.begin(); it != pt->gcs_handles.end(); ++it) {
    if ((*it)->IsBusy()) {
      continue;
    }

    auto res = std::move(*it);
    it->swap(pt->gcs_handles.back());
    pt->gcs_handles.pop_back();

    return unique_ptr<GCS, handle_keeper>(res.release(), pt);
  }

  IoContext* io_context = io_pool_->GetThisContext();
  CHECK(io_context) << "Must run from IO context thread";

  GCS* gcs = new GCS(*gce_handle_, &pt->ssl_context.value(), io_context);
  CHECK_STATUS(gcs->Connect(FLAGS_gcs_connect_deadline_ms));

  return unique_ptr<GCS, handle_keeper>(gcs, pt);
}

VarzValue::Map LocalRunner::Impl::GetStats() const {
  VarzValue::Map map;

  auto start = base::GetMonotonicMicrosFast();
  std::atomic_uint total_gcs_connections{0};

  io_pool_->AwaitOnAll([&](IoContext&) {
    if (!per_thread_)
      return;
    auto& maybe_pool = per_thread_->api_conn_pool;
    if (maybe_pool.has_value()) {
      total_gcs_connections.fetch_add(maybe_pool->handles_count(), std::memory_order_acq_rel);
    }
  });

  map.emplace_back("input-gcs-connections", VarzValue::FromInt(input_gcs_conn_.load()));
  map.emplace_back("total-gcs-connections", VarzValue::FromInt(total_gcs_connections.load()));
  map.emplace_back("stats-latency", VarzValue::FromInt(base::GetMonotonicMicrosFast() - start));

  return map;
}

uint64_t LocalRunner::Impl::ProcessText(const string& fname, file::ReadonlyFile* fd, RawSinkCb cb) {
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
  VLOG(1) << "ProcessText Read " << cnt << " items from " << fname;

  CHECK_STATUS(lr.status()) << "Line reader failed on file " << fname;

  return cnt;
}

uint64_t LocalRunner::Impl::ProcessLst(file::ReadonlyFile* fd, RawSinkCb cb) {
  file::ListReader::CorruptionReporter error_fn = [](size_t bytes, const util::Status& status) {
    LOG(FATAL) << "Lost " << bytes << " bytes, status: " << status;
  };

#ifdef MR_SKIP_OBJECT_CHECKSUM
  // NOTE(ORI): Used to read older .lst where checksum was calculated per file and not per object.
  file::ListReader list_reader(fd, TAKE_OWNERSHIP, false, error_fn);
#else
  file::ListReader list_reader(fd, TAKE_OWNERSHIP, true, error_fn);
#endif
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
  CHECK(!dest_mgr_);
  current_op_ = op;
  string out_dir = file_util::JoinPath(data_dir, op->output().name());
  if (util::IsGcsPath(out_dir)) {
  } else if (!file::Exists(out_dir)) {
    CHECK(file_util::RecursivelyCreateDir(out_dir, 0750)) << "Could not create dir " << out_dir;
  }

  lock_guard<mutex> lk(dest_mgr_mu_);
  dest_mgr_.reset(new DestFileSet(out_dir, op->output(), io_pool_, &fq_pool_));

  if (util::IsGcsPath(out_dir)) {
    io_pool_->AwaitFiberOnAll([this](IoContext&) { LazyGcsInit();});

    auto api_pool_cb = [this] {
      auto& opt_pool = per_thread_.get()->api_conn_pool;
      CHECK(opt_pool.has_value());
      return &opt_pool.value();
    };

    dest_mgr_->set_gce(gce_handle_.get(), api_pool_cb);
  }
}

void LocalRunner::Impl::End(ShardFileMap* out_files) {
  CHECK(dest_mgr_);

  auto shards = dest_mgr_->GetShards();
  for (const ShardId& sid : shards) {
    out_files->emplace(sid, dest_mgr_->ShardFilePath(sid, -1));
  }
  dest_mgr_->CloseAllHandles(stop_signal_.load(std::memory_order_acquire));

  lock_guard<mutex> lk(dest_mgr_mu_);
  dest_mgr_.reset();
  current_op_ = nullptr;
}

void LocalRunner::Impl::ExpandGCS(absl::string_view glob, ExpandCb cb) {
  absl::string_view bucket, path;
  CHECK(GCS::SplitToBucketPath(glob, &bucket, &path));

  // Lazy init of gce_handle.
  LazyGcsInit();

  auto cb2 = [cb = std::move(cb), bucket](size_t sz, absl::string_view s) {
    cb(sz, GCS::ToGcsPath(bucket, s));
  };
  bool recursive = absl::EndsWith(glob, "**");
  if (recursive) {
    path.remove_suffix(2);
  } else if (absl::EndsWith(glob, "*")) {
    path.remove_suffix(1);
  }

  auto gcs = GetGcsHandle();
  bool fs_mode = !recursive;
  auto status = gcs->List(bucket, path, fs_mode, cb2);
  CHECK_STATUS(status);
}

StatusObject<file::ReadonlyFile*> LocalRunner::Impl::OpenGcsFile(const std::string& filename) {
  CHECK(IsGcsPath(filename));
  LazyGcsInit();

  input_gcs_conn_.fetch_add(1, std::memory_order_acq_rel);
  if (FLAGS_local_runner_gcs_read_v2) {
    auto pt = per_thread_.get();
    return OpenGcsReadFile(filename, *gce_handle_, &pt->api_conn_pool.value());
  } else {
    auto gcs = GetGcsHandle();
    return gcs->OpenGcsFile(filename);
  }
}

StatusObject<file::ReadonlyFile*> LocalRunner::Impl::OpenLocalFile(
    const std::string& filename, file::FiberReadOptions::Stats* stats) {
  if (!per_thread_) {
    per_thread_.reset(new PerThread);
  }
  CHECK(!IsGcsPath(filename));

  file::FiberReadOptions opts;
  opts.prefetch_size = FLAGS_local_runner_prefetch_size;
  opts.stats = stats;

  return file::OpenFiberReadFile(filename, &fq_pool_, opts);
}

void LocalRunner::Impl::LazyGcsInit() {
  if (!per_thread_) {
    per_thread_.reset(new PerThread);
  }
  auto* io_context = io_pool_->GetThisContext();
  per_thread_->SetupGce(io_context);

  std::lock_guard<fibers::mutex> lk(gce_mu_);
  if (gce_handle_)
    return;
  gce_handle_.reset(new GCE);
  CHECK_STATUS(gce_handle_->Init());
  CHECK_STATUS(gce_handle_->RefreshAccessToken(io_context).status);
}

void LocalRunner::Impl::ShutDown() {
  fq_pool_.Shutdown();

  auto cb_per_thread = [this](IoContext&) {
    if (per_thread_) {
      auto pt = per_thread_.get();
      VLOG(1) << "Histogram Latency: " << pt->record_fetch_hist.ToString();

      per_thread_.reset();
    }
  };

  io_pool_->AwaitFiberOnAll(cb_per_thread);

  auto cached_bytes = file_cache_hit_bytes_.load();
  LOG_IF(INFO, cached_bytes) << "File cached hit bytes " << cached_bytes;
}

RawContext* LocalRunner::Impl::NewContext() {
  CHECK_NOTNULL(current_op_);

  return new detail::LocalContext(dest_mgr_.get());
}

/* LocalRunner implementation
********************************************/

LocalRunner::LocalRunner(IoContextPool* pool, const std::string& data_dir)
    : impl_(new Impl(pool, data_dir)) {}

LocalRunner::~LocalRunner() {}

void LocalRunner::Init() {}

void LocalRunner::Shutdown() { impl_->ShutDown(); }

void LocalRunner::OperatorStart(const pb::Operator* op) { impl_->Start(op); }

RawContext* LocalRunner::CreateContext() { return impl_->NewContext(); }

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

// Read file and fill queue. This function must be fiber-friendly.
size_t LocalRunner::ProcessInputFile(const std::string& filename, pb::WireFormat::Type type,
                                     RawSinkCb cb) {
  Impl::Source src(impl_.get(), filename);

  CHECK_STATUS(src.Open()) << filename;
  size_t cnt = src.Process(type, std::move(cb));

  return cnt;
}

void LocalRunner::Stop() { CHECK_NOTNULL(impl_)->Break(); }

}  // namespace mr3
