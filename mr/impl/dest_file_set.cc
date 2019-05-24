// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "mr/impl/dest_file_set.h"

#include "absl/strings/str_cat.h"
#include "base/hash.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "file/gzip_file.h"
#include "util/zlib_source.h"
#include "util/zstd_sinksource.h"

namespace mr3 {
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
      switch(pb_out.compress().type()) {
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

class CompressHandle : public DestHandle {
 public:
  CompressHandle(DestFileSet* owner, const ShardId& sid);

  void Write(std::string str) override;
  void Close() override;

 private:
  void Open() override;

  size_t start_delta_ = 0;
  util::StringSink* str_sink_ = nullptr;
  std::unique_ptr<util::Sink> compress_sink_;

  boost::fibers::mutex zmu_;
};

class LstHandle : public DestHandle {
 public:
  LstHandle(DestFileSet* owner, const ShardId& sid);

  void Write(std::string str) override;
  void Close() override;

 private:
  void Open() override;

  std::unique_ptr<file::ListWriter> lst_writer_;
  boost::fibers::mutex mu_;
};


void CompressHandle::Open() {
  wf_ = Await([&] { return file::Open(full_path_); });
  CHECK(wf_);
}

void CompressHandle::Write(std::string str) {
  std::unique_lock<fibers::mutex> lk(zmu_);
  CHECK_STATUS(compress_sink_->Append(strings::ToByteRange(str)));
  if (str_sink_->contents().size() >= kBufLimit - start_delta_) {
    str.clear();
    str.swap(str_sink_->contents());

    owner_->pool()->Add(fq_index_, WriteCb(std::move(str), wf_));
    start_delta_ = 0;
  }
}

void CompressHandle::Close() {
  CHECK_STATUS(compress_sink_->Flush());

  if (!str_sink_->contents().empty()) {
    owner_->pool()->Add(fq_index_, WriteCb(std::move(str_sink_->contents()), wf_));
  }

  DestHandle::Close();
}

LstHandle::LstHandle(DestFileSet* owner, const ShardId& sid) : DestHandle(owner, sid) {}

void LstHandle::Write(std::string str) {
  std::unique_lock<fibers::mutex> lk(mu_);
  CHECK_STATUS(lst_writer_->AddRecord(str));
}

void LstHandle::Open() {
  CHECK(!owner_->output().has_compress());
  DestHandle::Open();

  util::Sink* fs = new file::Sink{wf_, DO_NOT_TAKE_OWNERSHIP};
  lst_writer_.reset(new file::ListWriter{fs});
  CHECK_STATUS(lst_writer_->Init());
}

void LstHandle::Close() {
  CHECK_STATUS(lst_writer_->Flush());

  DestHandle::Close();
}

bool AllowCompressHandle(const pb::Output::Compress& pb_cmpr) {
  return !(FLAGS_dest_file_force_gzfile && pb_cmpr.type() == pb::Output::GZIP);
}

}  // namespace

DestFileSet::DestFileSet(const std::string& root_dir, const pb::Output& out,
                         fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), pb_out_(out), fq_(fq) {}

DestHandle* DestFileSet::GetOrCreate(const ShardId& sid) {
  std::lock_guard<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(sid);
  if (it == dest_files_.end()) {
    std::unique_ptr<DestHandle> dh;

    if (pb_out_.format().type() == pb::WireFormat::LST) {
      dh.reset(new LstHandle{this, sid});
    } else if (pb_out_.has_compress() && pb_out_.format().type() == pb::WireFormat::TXT
        && AllowCompressHandle(pb_out_.compress())) {
      dh.reset(new CompressHandle{this, sid});
    } else {
      dh.reset(new DestHandle{this, sid});
    }
    if (pb_out_.shard_spec().has_max_raw_size_mb()) {
      dh->set_raw_limit(size_t(1U << 20) * pb_out_.shard_spec().max_raw_size_mb());
    }
    dh->Open();

    auto res = dest_files_.emplace(sid, std::move(dh));
    CHECK(res.second);
    it = res.first;
  }

  return it->second.get();
}

void DestFileSet::CloseAllHandles() {
  for (auto& k_v : dest_files_) {
    k_v.second->Close();
  }
  dest_files_.clear();
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

  dh->Close();
}

std::vector<ShardId> DestFileSet::GetShards() const {
  std::vector<ShardId> res;
  res.reserve(dest_files_.size());
  transform(begin(dest_files_), end(dest_files_), back_inserter(res),
            [](const auto& pair) { return pair.first; });

  return res;
}

DestFileSet::~DestFileSet() {}

DestHandle::DestHandle(DestFileSet* owner, const ShardId& sid) : owner_(owner), sid_(sid) {
  CHECK(owner_);

  full_path_ = owner_->ShardFilePath(sid, 0);
  fq_index_ = base::Murmur32(full_path_, 120577U);
}

void DestHandle::AppendThreadLocal(const std::string& str) {
  auto status = wf_->Write(str);
  CHECK_STATUS(status);
  if (raw_limit_ < kuint64max) {
    raw_size_ += str.size();
    if (raw_size_ >= raw_limit_) {
      CHECK(wf_->Close());
      ++sub_shard_;
      raw_size_ = 0;
      full_path_ = owner_->ShardFilePath(sid_, sub_shard_);
      wf_ = OpenThreadLocal(owner_->output(), full_path_);
    }
  }
}

::file::WriteFile* DestHandle::OpenThreadLocal(const pb::Output& output, const std::string& path) {
  if (output.has_compress()) {
    if (output.compress().type() == pb::Output::GZIP) {
      file::GzipFile* gzres = file::GzipFile::Create(path, output.compress().level());
      CHECK(gzres->Open());
      return gzres;
    }
    LOG(FATAL) << "Not supported " << output.compress().ShortDebugString();
  }

  auto* wf = file::Open(path);
  CHECK(wf);
  return wf;
}

void DestHandle::Write(string str) {
  owner_->pool()->Add(fq_index_, [this, str = std::move(str)] { AppendThreadLocal(str); });
}

void DestHandle::Open() {
  VLOG(1) << "Creating file " << full_path_;
  StringPiece dirname = file_util::DirName(full_path_);

  // TODO: change for all functions expecting null-terminated string to explicitly accept it.
  // I call again RecursivelyCreateDir because shards may
  // have subdirectories specified by the user.
  file_util::RecursivelyCreateDir(string(dirname), 0755);

  wf_ = Await([this] { return OpenThreadLocal(owner_->output(), full_path_); });
}

void DestHandle::Close() {
  if (!wf_)
    return;

  bool res = Await([this] {
    VLOG(1) << "Closing file " << wf_->create_file_name();
    return wf_->Close();
  });
  CHECK(res);
  wf_ = nullptr;
}

CompressHandle::CompressHandle(DestFileSet* owner, const ShardId& sid)
    : DestHandle(owner, sid), str_sink_(new StringSink) {

  static std::default_random_engine rnd;

  // Randomize when we flush first for each handle. That should define uniform flushing cycle
  // for all handles.
  start_delta_ = rnd() % (kBufLimit - 1);
  auto level = owner->output().compress().level();
  if (owner->output().compress().type() == pb::Output::GZIP) {
    compress_sink_.reset(new ZlibSink(str_sink_, level));
  } else if (owner->output().compress().type() == pb::Output::ZSTD) {
    std::unique_ptr<ZStdSink> zsink{new ZStdSink(str_sink_)};
    CHECK_STATUS(zsink->Init(level));
    compress_sink_ = std::move(zsink);
  }
}

}  // namespace detail
}  // namespace mr3
