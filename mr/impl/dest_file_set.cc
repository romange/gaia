// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <google/protobuf/descriptor.h>

#include "mr/impl/dest_file_set.h"

#include "absl/strings/str_cat.h"
#include "base/hash.h"
#include "base/logging.h"
#include "base/walltime.h"

#include "file/file_util.h"
#include "file/filesource.h"
#include "file/gzip_file.h"
#include "file/proto_writer.h"

#include "util/asio/io_context_pool.h"
#include "util/gce/gcs.h"
#include "util/stats/varz_stats.h"
#include "util/zlib_source.h"
#include "util/zstd_sinksource.h"

namespace mr3 {

util::VarzMapAverage5m dest_files("dest-files-set");

DEFINE_uint32(gcs_connect_deadline_ms, 2000, "Deadline in milliseconds when connecting to GCS");

namespace detail {

using namespace boost;
using namespace std;
using namespace util;

namespace {

constexpr size_t kBufLimit = 1 << 16;

string FileName(StringPiece base, const pb::Output& pb_out, int32 sub_shard) {
  string res(base);
  if (pb_out.shard_spec().has_max_raw_size_mb()) {
    if (sub_shard >= 0) {
      absl::StrAppend(&res, "-", absl::Dec(sub_shard, absl::kZeroPad3));
    } else {
      absl::StrAppend(&res, "-*");
    }
  }

  if (pb_out.format().type() == pb::WireFormat::TXT) {
    absl::StrAppend(&res, ".txt");
    if (pb_out.has_compress()) {
      switch (pb_out.compress().type()) {
        case pb::Output::GZIP:
          absl::StrAppend(&res, ".gz");
          break;
        case pb::Output::ZSTD:
          absl::StrAppend(&res, ".zst");
          break;
        default:
          LOG(FATAL) << "Not supported " << pb_out.compress().ShortDebugString();
      }
    }
  } else if (pb_out.format().type() == pb::WireFormat::LST) {
    CHECK(!pb_out.has_compress()) << "Can not set compression on LST files";
    absl::StrAppend(&res, ".lst");
  } else {
    LOG(FATAL) << "Unsupported format for " << pb_out.ShortDebugString();
  }

  return res;
}

class CompressHandle : public DestHandle {
  void AppendThreadLocal(const std::string& val);

 public:
  CompressHandle(DestFileSet* owner, const ShardId& sid);
  ~CompressHandle() override;

  void Write(StringGenCb str) override;

  // Closes the handle without blocking.
  void Close(bool abort_write) override;

 private:
  void Open() override;
  void WriteThreadLocal(uint64_t start_usec, string data);

  size_t start_delta_ = 0;
  util::StringSink* compress_out_buf_ = nullptr;
  unique_ptr<util::Sink> compress_sink_;

  fibers::mutex zmu_;
};

class LstHandle : public DestHandle {
 public:
  LstHandle(DestFileSet* owner, const ShardId& sid);
  ~LstHandle();

  void Write(StringGenCb cb) final;
  void Close(bool abort_write) final;

 private:
  void Open() override;

  void OpenThreadLocal();
  void CloseThreadLocal(bool abort_write);

  std::unique_ptr<file::ListWriter> lst_writer_;
};

CompressHandle::CompressHandle(DestFileSet* owner, const ShardId& sid) : DestHandle(owner, sid) {
  static std::default_random_engine rnd;

  // Randomize when we flush first for each handle. That should define uniform flushing cycle
  // for all handles.
  start_delta_ = rnd() % (kBufLimit - 1);

  if (owner->output().has_compress()) {
    compress_out_buf_ = new StringSink;
    auto level = owner->output().compress().level();
    if (owner->output().compress().type() == pb::Output::GZIP) {
      compress_sink_.reset(new ZlibSink(compress_out_buf_, level));
    } else if (owner->output().compress().type() == pb::Output::ZSTD) {
      std::unique_ptr<ZStdSink> zsink{new ZStdSink(compress_out_buf_)};
      CHECK_STATUS(zsink->Init(level));
      compress_sink_ = std::move(zsink);
    } else {
      LOG(FATAL) << "Unsupported format " << owner->output().compress().ShortDebugString();
    }
  }
}

CompressHandle::~CompressHandle() {
  WaitForPendingToFinish();
}

void CompressHandle::AppendThreadLocal(const std::string& str) {
  auto status = CHECK_NOTNULL(write_file_)->Write(str);
  CHECK_STATUS(status);

  if (raw_limit_ == kuint64max)  // No output size limit
    return;

  raw_size_ += str.size();
  if (raw_size_ >= raw_limit_) {
    CHECK(write_file_->Close());
    ++sub_shard_;
    raw_size_ = 0;
    full_path_ = owner_->ShardFilePath(sid_, sub_shard_);

    OpenWriteFileLocal();
  }
}

void CompressHandle::Open() {
  // Do not block on opening the file.
  io_queue_->Add([this] { this->OpenWriteFileLocal(); });
}

// CompressHandle::Write runs in "other" threads, no necessarily where we write the data into.
void CompressHandle::Write(StringGenCb cb) {
  absl::optional<string> tmp_str;
  while (true) {
    tmp_str = cb();
    if (!tmp_str)
      break;

    if (compress_sink_) {
      this_fiber::yield();
    }

    // We must lock both the compression and the enquing calls because the order of writing
    // compressed chunks is important and we need to preserve transactional semantics.
    // It seems that compressing in the producer thread gives better performance because
    // the system balances itself: it spends producer CPU on the compression step before
    // enqueing it into io queue that could be full.
    std::unique_lock<fibers::mutex> lk(zmu_);

    if (compress_sink_) {
      strings::ByteRange br = strings::ToByteRange(*tmp_str);
      CHECK_STATUS(compress_sink_->Append(br));

      if (start_delta_ + compress_out_buf_->contents().size() < kBufLimit)
        continue;

      tmp_str->clear();
      tmp_str->swap(compress_out_buf_->contents());
      start_delta_ = 0;
    }

    auto start = base::GetMonotonicMicrosFast();
    auto cb = [start, this, str = std::move(*tmp_str)]() mutable {
      WriteThreadLocal(start, std::move(str));
    };

    bool preempted = io_queue_->Add(std::move(cb));
    lk.unlock();  // unlock the transaction. Must be after io_queue_->Add call.

    auto delta = base::GetMonotonicMicrosFast() - start;
    if (preempted) {
      dest_files.IncBy("io-submit-preempted", delta);
    } else {
      dest_files.IncBy("io-submit-fast", delta);
    }
  }
}

void CompressHandle::WriteThreadLocal(uint64_t start_usec, string data) {
  dest_files.IncBy("io-deque", base::GetMonotonicMicrosFast() - start_usec);

#if 0
  if (compress_sink_) {
    if (data.empty()) {
      CHECK_STATUS(compress_sink_->Flush());
    } else {
      strings::ByteRange br = strings::ToByteRange(data);
      CHECK_STATUS(compress_sink_->Append(br));

      if (start_delta_ + compress_out_buf_->contents().size() < kBufLimit)
        return;
    }

    data.clear();
    data.swap(compress_out_buf_->contents());
    start_delta_ = 0;
  }
#endif
  AppendThreadLocal(data);
}

void CompressHandle::Close(bool abort_write) {
  VLOG(1) << "CompressHandle::Close";

  if (!abort_write) {
    if (compress_sink_) {
      CHECK_STATUS(compress_sink_->Flush());

      auto& buf = compress_out_buf_->contents();
      if (!buf.empty()) {
        auto start = base::GetMonotonicMicrosFast();
        auto cb = [start, this, str = std::move(buf)]() mutable {
          WriteThreadLocal(start, std::move(str));  // Flush and write.
        };
        io_queue_->Add(std::move(cb));
      }
    }
  }

  // TODO: to handle abort_write by changing WriteFile interface to allow optionally drop
  // the pending writes.
  // I do not block on Close to allow fast iteration when closing all the files.
  // During queues shutdown they will block until this handler runs.
  io_queue_->Add([this] {
    if (write_file_) {
      VLOG(1) << "Closing file " << write_file_->create_file_name();
      CHECK(write_file_->Close());
      write_file_ = nullptr;
    }
  });
  // I can not reset write_file_ here since some write may be still pending in the IO thread.
}

LstHandle::LstHandle(DestFileSet* owner, const ShardId& sid) : DestHandle(owner, sid) {}

LstHandle::~LstHandle() {
  VLOG(1) << "Destructing lst " << full_path_;
  WaitForPendingToFinish();
}

void LstHandle::Write(StringGenCb cb) {
  absl::optional<string> tmp_str;

  while (true) {
    tmp_str = cb();
    if (!tmp_str)
      break;
    io_queue_->Add(
        [this, str = std::move(*tmp_str)] { CHECK_STATUS(lst_writer_->AddRecord(str)); });
  }
}

void LstHandle::Open() {
  CHECK(!owner_->output().has_compress());
  io_queue_->Add([this] { this->OpenThreadLocal(); });
}

void LstHandle::OpenThreadLocal() {
  OpenWriteFileLocal();

  namespace gpb = google::protobuf;

  util::Sink* fs = new file::Sink{write_file_, DO_NOT_TAKE_OWNERSHIP};
  lst_writer_.reset(new file::ListWriter{fs});
  if (!owner_->output().type_name().empty()) {
    lst_writer_->AddMeta(file::kProtoTypeKey, owner_->output().type_name());

    const gpb::DescriptorPool* gen_pool = gpb::DescriptorPool::generated_pool();
    const gpb::Descriptor* descr = gen_pool->FindMessageTypeByName(owner_->output().type_name());

    CHECK(descr);  // TODO: should we support more cases besides pb?
    lst_writer_->AddMeta(file::kProtoSetKey, file::GenerateSerializedFdSet(descr));
  }

  CHECK_STATUS(lst_writer_->Init());
}

void LstHandle::CloseThreadLocal(bool abort_write) {
  CHECK_STATUS(lst_writer_->Flush());
  VLOG(1) << "Closing file " << write_file_->create_file_name();
  CHECK(write_file_->Close());
}

void LstHandle::Close(bool abort_write) {
  io_queue_->Add([this, abort_write] { this->CloseThreadLocal(abort_write); });
}

}  // namespace

DestFileSet::DestFileSet(const std::string& root_dir, const pb::Output& out,
                         util::IoContextPool* pool, fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), pb_out_(out), io_pool_(*pool), fq_(*fq) {
  is_gcs_dest_ = util::IsGcsPath(root_dir_);
}

DestFileSet::~DestFileSet() {}

// DestHandle is cached in each of the calling IO threads and the only contention happens
// when a new handle shard is created.
DestHandle* DestFileSet::GetOrCreate(const ShardId& sid) {
  std::lock_guard<fibers::mutex> lk(handles_mu_);
  auto it = dest_files_.find(sid);
  if (it == dest_files_.end()) {
    std::unique_ptr<DestHandle> dh;

    bool is_local_fs = !is_gcs_dest_;
    if (is_local_fs) {
      string shard_name = sid.ToString(absl::string_view{});
      absl::string_view dir_name = file_util::DirName(shard_name);
      if (dir_name.size() != shard_name.size()) {  // If dir name is present in a shard name.
        string sub_dir = file_util::JoinPath(root_dir_, dir_name);
        CHECK_STATUS(file_util::CreateSubDirIfNeeded(sub_dir)) << sub_dir;
      }
    }
    if (pb_out_.format().type() == pb::WireFormat::LST) {
      dh = std::make_unique<LstHandle>(this, sid);
    } else if (pb_out_.format().type() == pb::WireFormat::TXT) {
      dh = std::make_unique<CompressHandle>(this, sid);
    } else {
      LOG(FATAL) << "Unsupported format " << pb_out_.format().ShortDebugString();
    }
    if (pb_out_.shard_spec().has_max_raw_size_mb()) {
      dh->set_raw_limit(size_t(1U << 20) * pb_out_.shard_spec().max_raw_size_mb());
    }

    dh->Open();
    VLOG(1) << "Open destination shard " << dh->full_path();

    auto res = dest_files_.emplace(sid, std::move(dh));
    CHECK(res.second);
    it = res.first;
  }

  return it->second.get();
}

void DestFileSet::CloseAllHandles(bool abort_write) {
  std::lock_guard<fibers::mutex> lk(handles_mu_);

  // DestHandle::Close() does not block which allows us to signal all handles to close
  // without blockign on each one of them.
  // However it may start asynchronous operations that will live after this function exits.
  // This is why we need DestHandle to be shared_ptr - to guard it against destruction during
  // async ops.
  for (auto& k_v : dest_files_) {
    if (k_v.second)  // Can be null if was closed in the middle
      k_v.second->Close(abort_write);
  }

  dest_files_.clear();  // This blocks until all the pending operations finish.
}

std::string DestFileSet::ShardFilePath(const ShardId& key, int32 sub_shard) const {
  string shard_name = key.ToString(absl::StrCat(pb_out_.name(), "-", "shard"));
  string file_name = FileName(shard_name, pb_out_, sub_shard);

  return file_util::JoinPath(root_dir_, file_name);
}

void DestFileSet::CloseHandle(const ShardId& sid) {
  std::unique_lock<fibers::mutex> lk(handles_mu_);
  auto it = dest_files_.find(sid);
  CHECK(it != dest_files_.end());
  auto dh = std::move(it->second);  // we move the handle to destroy it after the closure.
  lk.unlock();
  VLOG(1) << "Closing handle " << ShardFilePath(sid, -1);

  dh->Close(false);
}

std::vector<ShardId> DestFileSet::GetShards() const {
  std::vector<ShardId> res;
  res.reserve(dest_files_.size());

  std::unique_lock<fibers::mutex> lk(handles_mu_);
  transform(begin(dest_files_), end(dest_files_), back_inserter(res),
            [](const auto& pair) { return pair.first; });

  return res;
}

size_t DestFileSet::HandleCount() const {
  std::unique_lock<fibers::mutex> lk(handles_mu_);

  return dest_files_.size();
}

DestHandle::DestHandle(DestFileSet* owner, const ShardId& sid) : owner_(owner), sid_(sid) {
  CHECK(owner_);

  full_path_ = owner_->ShardFilePath(sid, 0);
  queue_index_ = base::Murmur32(full_path_, 120577U);
  io_queue_ = owner_->pool()->GetQueue(queue_index_);

  if (owner_->is_gcs_dest()) {
    size_t net_index = queue_index_ % owner_->io_pool()->size();

    net_context_ = &owner_->io_pool()->at(net_index);
    net_queue_.reset(new fibers_ext::FiberQueue(64));
    net_fiber_ = net_context_->LaunchFiber([this] { GcsWriteFiber(net_context_); });
    io_queue_ = net_queue_.get();
  }
}

DestHandle::~DestHandle() {
}

void DestHandle::WaitForPendingToFinish() {
  if (net_queue_) {
    /// Signal the queue to finish processing.
    /// Notifies but does not block for shutdown. We block when waiting for GcsWriteFiber to exit.
    net_queue_->Shutdown();
    net_fiber_.join();
  } else {
    // We block for the io queue to process all the pending operations.
    io_queue_->Await([] {});
  }
}

void DestHandle::GcsWriteFiber(IoContext* io_context) {
  CHECK(io_context->InContextThread());

  // We want write fiber to have higher priority and initiate write as fast as possible.
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(1);
  this_fiber::properties<IoFiberProperties>().set_name("GcsWriteFiber");

  net_queue_->Run();
}

void DestHandle::OpenWriteFileLocal() {
  VLOG(1) << "Creating file " << full_path_;

  if (is_gcs()) {
    write_file_ =
        CHECKED_GET(OpenGcsWriteFile(full_path_, *owner_->gce(), owner_->GetGceApiPool()));
  } else {
    // I can not use OpenFiberWriteFile here since it supports only synchronous semantics of
    // writing data (i.e. Write(StringPiece) where ownership stays with owner).
    // To support asynchronous writes we need to design an abstract class AsyncWriteFile
    // which should take ownership over data chunks that are passed to it for writing.
    write_file_ = file::Open(full_path_);
  }
  CHECK(write_file_);
}

}  // namespace detail
}  // namespace mr3
