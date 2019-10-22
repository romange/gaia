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
DEFINE_bool(local_runner_gcs_write_v2, true, "");

namespace detail {

// For some reason disabling dest_file_force_gzfile performs slower than
// using GzipFile in the threadpool. I think there is something to dig here and I think
// compress operations should not be part of the FiberQueueThreadPool workload but after spending
// quite some time I am lowering the priority of this.
// TODO: to implement compress directly using zlib interface an not using zlibsink/stringsink
// abstractions.
DEFINE_bool(dest_file_force_gzfile, true, "");

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

inline auto WriteCb(std::string&& s, file::WriteFile* wf) {
  return [b = std::move(s), wf] {
    auto status = wf->Write(b);
    CHECK_STATUS(status);
  };
}

::file::WriteFile* OpenThreadLocal(const pb::Output& output, const std::string& path) {
  auto* wf = file::Open(path);
  CHECK(wf);
  return wf;
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
  void GcsWriteFiber(IoContext* io_context);

  size_t start_delta_ = 0;
  util::StringSink* compress_out_buf_ = nullptr;
  unique_ptr<util::Sink> compress_sink_;

  fibers::mutex zmu_;
  unique_ptr<fibers_ext::FiberQueue> out_queue_;
  unique_ptr<GCS> gcs_;
  fibers::fiber write_fiber_;
  std::atomic_int fiber_state_{0};
  ::file::WriteFile* gcs_file_ = nullptr;
};

class LstHandle : public DestHandle {
 public:
  LstHandle(DestFileSet* owner, const ShardId& sid);

  void Write(StringGenCb cb) final;
  void Close(bool abort_write) override;

 private:
  void Open() override;

  std::unique_ptr<file::ListWriter> lst_writer_;
  boost::fibers::mutex mu_;
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
  if (write_fiber_.joinable()) {
    write_fiber_.join();
  }
}

void CompressHandle::AppendThreadLocal(const std::string& str) {
  auto status = write_file_->Write(str);
  CHECK_STATUS(status);

  if (raw_limit_ == kuint64max)  // No output size limit
    return;

  raw_size_ += str.size();
  if (raw_size_ >= raw_limit_) {
    CHECK(write_file_->Close());
    ++sub_shard_;
    raw_size_ = 0;
    full_path_ = owner_->ShardFilePath(sid_, sub_shard_);
    write_file_ = OpenThreadLocal(owner_->output(), full_path_);
  }
}

void CompressHandle::Open() {
  if (owner_->is_gcs_dest()) {
    size_t index = queue_index_ % owner_->io_pool()->size();

    IoContext& io_context = owner_->io_pool()->at(index);
    out_queue_.reset(new fibers_ext::FiberQueue(32));
    write_fiber_ = io_context.LaunchFiber([this, &io_context] { GcsWriteFiber(&io_context); });
  } else {
    OpenLocalFile();
  }
}

void CompressHandle::GcsWriteFiber(IoContext* io_context) {
  CHECK(io_context->InContextThread());

  // We want write fiber to have higher priority and initiate write as fast as possible.
  this_fiber::properties<IoFiberProperties>().SetNiceLevel(1);

  static thread_local asio::ssl::context ssl_context = GCE::CheckedSslContext();

  if (FLAGS_local_runner_gcs_write_v2) {
    gcs_file_ =
        CHECKED_GET(OpenGcsWriteFile(full_path_, *owner_->gce(), owner_->GetGceApiPool()));
    CHECK(gcs_file_);
  } else {
    absl::string_view bucket, path;
    CHECK(GCS::SplitToBucketPath(full_path_, &bucket, &path));

    gcs_.reset(new util::GCS(*owner_->gce(), &ssl_context, io_context));
    CHECK_STATUS(gcs_->Connect(FLAGS_gcs_connect_deadline_ms));
    CHECK_STATUS(gcs_->OpenForWrite(bucket, path));
  }

  fiber_state_ = 1;

  out_queue_->Run();

  // TODO: to handle abort_write by changing WriteFile interface to allow optionally drop
  // the pending writes.
  if (FLAGS_local_runner_gcs_write_v2) {
    CHECK(gcs_file_->Close());
    gcs_file_ = nullptr;
  }
  gcs_.reset();
}

// CompressHandle::Write runs in "other" threads, no necessarily where we write the data into.
void CompressHandle::Write(StringGenCb cb) {
  absl::optional<string> tmp_str;
  while (true) {
    tmp_str = cb();
    if (!tmp_str)
      break;

    if (compress_sink_) {
      std::unique_lock<fibers::mutex> lk(zmu_);

      strings::ByteRange br = strings::ToByteRange(*tmp_str);
      CHECK_STATUS(compress_sink_->Append(br));

      if (start_delta_ + compress_out_buf_->contents().size() < kBufLimit)
        continue;

      tmp_str->clear();
      tmp_str->swap(compress_out_buf_->contents());
      start_delta_ = 0;
    }

    if (out_queue_) {
      auto start = base::GetMonotonicMicrosFast();
      auto fiber_state = fiber_state_.load(std::memory_order_relaxed);

      auto cb = [start, this, str = std::move(*tmp_str)] {
        dest_files.IncBy("gcs-deque", base::GetMonotonicMicrosFast() - start);
        if (FLAGS_local_runner_gcs_write_v2) {
          CHECK_STATUS(write_file_->Write(str));
        } else {
          CHECK_STATUS(gcs_->Write(strings::ToByteRange(str)));
        }
      };

      bool preempted = out_queue_->Add(std::move(cb));

      auto delta = base::GetMonotonicMicrosFast() - start;
      if (preempted) {
        if (fiber_state == 1) {
          dest_files.IncBy("gcs-submit-preempted", delta);
        } else {
          dest_files.IncBy("gcs-submit-launching", delta);
        }
      } else {  // if(out_queue_)
        dest_files.IncBy("gcs-submit-fast", delta);
      }
      this_fiber::yield();
    } else {
      owner_->pool()->Add(queue_index_,
                          [this, str = std::move(*tmp_str)] { AppendThreadLocal(str); });
    }
  }
}

void CompressHandle::Close(bool abort_write) {
  VLOG(1) << "CompressHandle::Close";

  if (!abort_write) {
    if (compress_sink_) {
      CHECK_STATUS(compress_sink_->Flush());

      auto& buf = compress_out_buf_->contents();
      if (!buf.empty()) {
        // Flush the rest of compressed data.
        if (out_queue_) {  // GCS flow.
          out_queue_->Add([this, str = std::move(buf)] {
            if (FLAGS_local_runner_gcs_write_v2) {
              CHECK_STATUS(gcs_file_->Write(str));
            } else {
              CHECK_STATUS(gcs_->Write(strings::ToByteRange(str)));
            }
          });
        } else {
          owner_->pool()->Add(queue_index_,
                              [this, str = std::move(buf)] { AppendThreadLocal(str); });
        }
      }
    }
  }

  if (out_queue_) {
    // Send GCS closure callback and signal the queue to finish file but do not block on it.
    if (FLAGS_local_runner_gcs_write_v2) {
      // TODO: to handle abort_write by changing WriteFile interface to allow optionally drop
      // the pending writes. We close inside GcsWriteFiber.
    } else {
      out_queue_->Add([this, abort_write] { CHECK_STATUS(gcs_->CloseWrite(abort_write)); });
    }

    /// Notifies but does not block for shutdown. We block when waiting for GcsWriteFiber to exit.
    out_queue_->Shutdown();
  } else {
    CloseWriteFile(abort_write);
  }
}

LstHandle::LstHandle(DestFileSet* owner, const ShardId& sid) : DestHandle(owner, sid) {}

// TODO: Support lst writing via thread_pool to avoit locks on disk I/O.
// Instead of StringGenCb we could support vector<string> and with text case it will be 1-cell
// vector.
void LstHandle::Write(StringGenCb cb) {
  absl::optional<string> tmp_str;
  std::unique_lock<fibers::mutex> lk(mu_);
  while (true) {
    tmp_str = cb();
    if (!tmp_str)
      break;
    CHECK_STATUS(lst_writer_->AddRecord(*tmp_str));
  }
}

void LstHandle::Open() {
  CHECK(!owner_->output().has_compress());
  OpenLocalFile();

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

void LstHandle::Close(bool abort_write) {
  CHECK_STATUS(lst_writer_->Flush());

  CloseWriteFile(abort_write);
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
  std::lock_guard<fibers::mutex> lk(mu_);
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
      dh.reset(new LstHandle{this, sid});
    } else if (pb_out_.format().type() == pb::WireFormat::TXT) {
      dh.reset(new CompressHandle{this, sid});
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
  std::lock_guard<fibers::mutex> lk(mu_);

  // DestHandle::Close() does not block (on GCS) which allows us to signal all handles to close
  // and then wait for them to actually close.
  for (auto& k_v : dest_files_) {
    k_v.second->Close(abort_write);
  }
  dest_files_.clear();  // ~DestHandle() might block until all its resources finished.
}

std::string DestFileSet::ShardFilePath(const ShardId& key, int32 sub_shard) const {
  string shard_name = key.ToString(absl::StrCat(pb_out_.name(), "-", "shard"));
  string file_name = FileName(shard_name, pb_out_, sub_shard);

  return file_util::JoinPath(root_dir_, file_name);
}

void DestFileSet::CloseHandle(const ShardId& sid) {
  DestHandle* dh = nullptr;

  std::unique_lock<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(sid);
  CHECK(it != dest_files_.end());
  dh = it->second.get();
  lk.unlock();
  VLOG(1) << "Closing handle " << ShardFilePath(sid, -1);

  dh->Close(false);
}

std::vector<ShardId> DestFileSet::GetShards() const {
  std::vector<ShardId> res;
  res.reserve(dest_files_.size());

  std::unique_lock<fibers::mutex> lk(mu_);
  transform(begin(dest_files_), end(dest_files_), back_inserter(res),
            [](const auto& pair) { return pair.first; });

  return res;
}

size_t DestFileSet::HandleCount() const {
  std::unique_lock<fibers::mutex> lk(mu_);

  return dest_files_.size();
}

DestHandle::DestHandle(DestFileSet* owner, const ShardId& sid) : owner_(owner), sid_(sid) {
  CHECK(owner_);

  full_path_ = owner_->ShardFilePath(sid, 0);
  queue_index_ = base::Murmur32(full_path_, 120577U);
}

void DestHandle::OpenLocalFile() {
  VLOG(1) << "Creating file " << full_path_;

  // I can not use OpenFiberWriteFile here since it supports only synchronous semantics of
  // writing data (i.e. Write(StringPiece) where ownership stays with owner).
  // To support asynchronous writes we need to design an abstract class AsyncWriteFile
  // which should take ownership over data chunks that are passed to it for writing.
  write_file_ = Await([this] { return OpenThreadLocal(owner_->output(), full_path_); });
}

void DestHandle::CloseWriteFile(bool abort_write) {
  if (!write_file_)
    return;

  bool res = Await([this] {
    VLOG(1) << "Closing file " << write_file_->create_file_name();
    return write_file_->Close();
  });
  CHECK(res);
  write_file_ = nullptr;
}

}  // namespace detail
}  // namespace mr3
