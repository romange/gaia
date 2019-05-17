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

namespace mr3 {
namespace detail {

// For some reason enabling local_runner_zsink as true performs slower than
// using GzipFile in the threadpool. I think there is something to dig here and I think
// compress operations should not be part of the FiberQueueThreadPool workload but after spending
// quite some time I am giving up.
// TODO: to implement compress directly using zlib interface an not using zlibsink/stringsink
// abstractions.
DEFINE_bool(local_runner_zsink, false, "");

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
      if (pb_out.compress().type() == pb::Output::GZIP) {
        absl::StrAppend(&res, ".gz");
      } else {
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

file::WriteFile* CreateFile(const std::string& path, const pb::Output& out,
                            fibers_ext::FiberQueueThreadPool* fq) {
  std::function<file::WriteFile*()> cb;
  if (out.has_compress() && !FLAGS_local_runner_zsink) {
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

}  // namespace

DestFileSet::DestFileSet(const std::string& root_dir, fibers_ext::FiberQueueThreadPool* fq)
    : root_dir_(root_dir), fq_(fq) {}

auto DestFileSet::GetOrCreate(const ShardId& sid, const pb::Output& pb_out) -> Result {
  std::lock_guard<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(sid);
  if (it == dest_files_.end()) {
    string full_path = ShardFilePath(sid, pb_out, 0);
    VLOG(1) << "Creating file " << full_path;

    file::WriteFile* wf = CreateFile(full_path, pb_out, fq_);
    std::unique_ptr<DestHandle> dh;

    if (FLAGS_local_runner_zsink && pb_out.has_compress()) {
      dh.reset(new ZlibHandle{full_path, pb_out, wf, fq_});
    } else {
      dh.reset(new DestHandle{full_path, wf, fq_});
    }
    auto res = dest_files_.emplace(sid, std::move(dh));
    CHECK(res.second);
    it = res.first;
  }

  return Result(it->second.get());
}

void DestFileSet::CloseAllHandles() {
  for (auto& k_v : dest_files_) {
    k_v.second->Close();
  }
  dest_files_.clear();
}

std::string DestFileSet::ShardFilePath(const ShardId& key, const pb::Output& pb_out,
                                       int32 sub_shard) const {
  string shard_name = key.ToString(absl::StrCat(pb_out.name(), "-", "shard"));
  string file_name = FileName(shard_name, pb_out, sub_shard);
  return file_util::JoinPath(root_dir_, file_name);
}

void DestFileSet::CloseHandle(const ShardId& sid) {
  DestHandle* dh = nullptr;

  std::unique_lock<fibers::mutex> lk(mu_);
  auto it = dest_files_.find(sid);
  CHECK(it != dest_files_.end());
  dh = it->second.get();
  lk.unlock();
  VLOG(1) << "Closing handle " << dh->path();

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

DestHandle::DestHandle(const string& path, ::file::WriteFile* wf,
                       fibers_ext::FiberQueueThreadPool* fq)
    : wf_(wf), fq_(fq), full_path_(path) {
  CHECK(wf && fq_);
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(path.data());
  fq_index_ = base::MurmurHash3_x86_32(ptr, path.size(), 1);
}

void DestHandle::Write(string str) { fq_->Add(fq_index_, WriteCb(std::move(str), wf_)); }

void DestHandle::Close() {
  if (!wf_)
    return;

  VLOG(1) << "Closing file " << path();

  bool res = fq_->Await(fq_index_, [this] { return wf_->Close(); });
  CHECK(res);
  wf_ = nullptr;
}

ZlibHandle::ZlibHandle(const string& path, const pb::Output& out, ::file::WriteFile* wf,
                       util::fibers_ext::FiberQueueThreadPool* fq)
    : DestHandle(path, wf, fq), str_sink_(new StringSink) {
  CHECK_EQ(pb::Output::GZIP, out.compress().type());

  static std::default_random_engine rnd;

  zlib_sink_.reset(new ZlibSink(str_sink_, out.compress().level()));

  // Randomize when we flush first for each handle. That should define uniform flushing cycle
  // for all handles.
  start_delta_ = rnd() % (kBufLimit - 1);
}

void ZlibHandle::Write(std::string str) {
  std::unique_lock<fibers::mutex> lk(zmu_);
  CHECK_STATUS(zlib_sink_->Append(strings::ToByteRange(str)));
  str.clear();
  if (str_sink_->contents().size() >= kBufLimit - start_delta_) {
    fq_->Add(fq_index_, WriteCb(std::move(str_sink_->contents()), wf_));
    start_delta_ = 0;
  }
}

void ZlibHandle::Close() {
  CHECK_STATUS(zlib_sink_->Flush());

  if (!str_sink_->contents().empty()) {
    fq_->Add(fq_index_, WriteCb(std::move(str_sink_->contents()), wf_));
  }

  DestHandle::Close();
}

LstHandle::LstHandle(const string& path, ::file::WriteFile* wf,
                     util::fibers_ext::FiberQueueThreadPool* fq)
    : DestHandle(path, wf, fq) {
  util::Sink* fs = new file::Sink{wf, DO_NOT_TAKE_OWNERSHIP};
  lst_writer_.reset(new file::ListWriter{fs});
  CHECK_STATUS(lst_writer_->Init());
}

void LstHandle::Write(std::string str) {
  std::unique_lock<fibers::mutex> lk(mu_);
  CHECK_STATUS(lst_writer_->AddRecord(str));
}

void LstHandle::Close() {
  CHECK_STATUS(lst_writer_->Flush());

  DestHandle::Close();
}

}  // namespace detail
}  // namespace mr3
