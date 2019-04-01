// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/local_runner.h"

#include <fcntl.h>

#include "absl/strings/str_cat.h"
#include "base/logging.h"

#include "file/fiber_file.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "file/gzip_file.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/zlib_source.h"

namespace mr3 {

DEFINE_uint32(local_runner_prefetch_size, 1 << 16, "File input prefetch size");

using namespace util;
using namespace boost;
using namespace std;

namespace {

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

struct DestHandle {
  ::file::WriteFile* wf;
  StringSink* str_sink = nullptr;
  std::unique_ptr<ZlibSink> zlib_sink;
  unsigned fq_index;

  auto WriteCb(string&& s) {
    return [b = std::move(s), wf = this->wf] {
      auto status = wf->Write(b);
      CHECK_STATUS(status);
    };
  }

  void Write(string str, fibers_ext::FiberQueueThreadPool* fq);
};

class GlobalDestFileManager {
  const string root_dir_;

  fibers_ext::FiberQueueThreadPool* fq_;

  google::dense_hash_map<StringPiece, DestHandle*> dest_files_;
  UniqueStrings str_db_;
  fibers::mutex mu_;

 public:
  using Result = std::pair<StringPiece, DestHandle*>;

  GlobalDestFileManager(const std::string& root_dir, fibers_ext::FiberQueueThreadPool* fq);
  ~GlobalDestFileManager();

  void Flush();

  Result Get(StringPiece key, const pb::Output& out);

  fibers_ext::FiberQueueThreadPool* pool() { return fq_; }
};

class BufferedWriter {
  DestHandle* dh_;
  std::string buffer_;

  size_t writes = 0, flushes = 0;
  static constexpr size_t kFlushLimit = 1 << 15;
  void operator=(const BufferedWriter&) = delete;

 public:
  explicit BufferedWriter(DestHandle* dh) : dh_(dh) {}

  BufferedWriter(const BufferedWriter&) = delete;
  ~BufferedWriter();

  void Flush(fibers_ext::FiberQueueThreadPool* fq);

  void Write(StringPiece src, fibers_ext::FiberQueueThreadPool* fq);
};

class LocalContext : public RawContext {
 public:
  explicit LocalContext(const pb::Output& output, GlobalDestFileManager* mgr);
  ~LocalContext();

  void Flush();

 private:
  void WriteInternal(const ShardId& shard_id, std::string&& record) final;

  google::dense_hash_map<StringPiece, BufferedWriter*> custom_shard_files_;

  const pb::Output& output_;
  GlobalDestFileManager* mgr_;
};


void DestHandle::Write(string str, fibers_ext::FiberQueueThreadPool* fq) {
  CHECK(!zlib_sink);
  fq->Add(fq_index, WriteCb(std::move(str)));
}

GlobalDestFileManager::GlobalDestFileManager(const std::string& root_dir,
                                             fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), fq_(fq) {
  dest_files_.set_empty_key(StringPiece());
}

auto GlobalDestFileManager::Get(StringPiece key, const pb::Output& out) -> Result {
  CHECK_EQ(out.shard_type(), pb::Output::USER_DEFINED);

  std::lock_guard<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(key);
  if (it == dest_files_.end()) {
    string file_name = FileName(key, out);
    key = str_db_.Get(key);
    string full_path = file_util::JoinPath(root_dir_, file_name);
    DestHandle* dh = new DestHandle;
    dh->wf = CreateFile(full_path, out, fq_);
    CHECK(dh->wf);

    dh->fq_index =
        base::MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(key.data()), key.size(), 1);
    auto res = dest_files_.emplace(key, dh);
    CHECK(res.second);
    it = res.first;
  }

  return Result(it->first, it->second);
}

void GlobalDestFileManager::Flush() {
  for (auto& k_v : dest_files_) {
    DestHandle* dh = k_v.second;
    if (dh->zlib_sink) {
      CHECK_STATUS(dh->zlib_sink->Flush());

      fq_->Add(dh->fq_index, dh->WriteCb(std::move(dh->str_sink->contents())));
    }
    bool res = fq_->Await(dh->fq_index, [wf = dh->wf] { return wf->Close();});
    CHECK(res);
    dh->wf = nullptr;
  }
}

GlobalDestFileManager::~GlobalDestFileManager() {
  for (auto& k_v : dest_files_) {
    delete k_v.second;
  }
}

BufferedWriter::~BufferedWriter() { CHECK(buffer_.empty()); }

void BufferedWriter::Flush(fibers_ext::FiberQueueThreadPool* fq) {
  if (buffer_.empty())
    return;

  dh_->Write(std::move(buffer_), fq);
}

void BufferedWriter::Write(StringPiece src, fibers_ext::FiberQueueThreadPool* fq) {
  buffer_.append(src.data(), src.size());

  VLOG_IF(2, ++writes % 1000 == 0) << "BufferedWrite " << writes;

  if (buffer_.size() >= kFlushLimit) {
    VLOG(2) << "Flush " << ++flushes;

    dh_->Write(std::move(buffer_), fq);
  }
}

LocalContext::LocalContext(const pb::Output& out, GlobalDestFileManager* mgr)
    : output_(out), mgr_(mgr) {
  custom_shard_files_.set_empty_key(StringPiece{});
}

void LocalContext::WriteInternal(const ShardId& shard_id, std::string&& record) {
  CHECK(absl::holds_alternative<string>(shard_id));

  const string& shard_name = absl::get<string>(shard_id);

  auto it = custom_shard_files_.find(shard_name);
  if (it == custom_shard_files_.end()) {
    auto res = mgr_->Get(shard_name, output_);
    StringPiece key = res.first;

    it = custom_shard_files_.emplace(key, new BufferedWriter{res.second}).first;
  }
  record.append("\n");
  it->second->Write(record, mgr_->pool());
}

void LocalContext::Flush() {
  for (auto& k_v : custom_shard_files_) {
    k_v.second->Flush(mgr_->pool());
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
  std::unique_ptr<GlobalDestFileManager> dest_mgr;
  fibers_ext::FiberQueueThreadPool fq_pool;
  std::atomic_bool stop_signal_{false};
  std::atomic_ulong file_cache_hit_bytes{0};

  Impl(const string& d) : data_dir(d), fq_pool(0, 128) {}

  uint64_t ProcessText(file::ReadonlyFile* fd, RecordQueue* queue);
};

uint64_t LocalRunner::Impl::ProcessText(file::ReadonlyFile* fd, RecordQueue* queue) {
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
    queue->Push(std::move(tmp));
  }
  VLOG(1) << "ProcessText Read " << cnt << " items";
  return cnt;
}

LocalRunner::LocalRunner(const std::string& data_dir) : impl_(new Impl(data_dir)) {}

LocalRunner::~LocalRunner() {}

void LocalRunner::Init() {
  file_util::RecursivelyCreateDir(impl_->data_dir, 0750);
  impl_->dest_mgr.reset(new GlobalDestFileManager(impl_->data_dir, &impl_->fq_pool));
}

void LocalRunner::Shutdown() {
  impl_->dest_mgr->Flush();

  impl_->dest_mgr.reset();
  impl_->fq_pool.Shutdown();

  LOG(INFO) << "File cached hit bytes " << impl_->file_cache_hit_bytes.load();
}

RawContext* LocalRunner::CreateContext(const pb::Operator& op) {
  return new LocalContext(op.output(), impl_->dest_mgr.get());
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
size_t LocalRunner::ProcessFile(const std::string& filename, pb::WireFormat::Type type,
                                RecordQueue* queue) {
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
      cnt = impl_->ProcessText(read_file.release(), queue);
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
