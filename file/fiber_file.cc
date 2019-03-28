// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/fiber_file.h"

#include <sys/uio.h>
#include <atomic>

#include "base/hash.h"
#include "base/logging.h"

namespace file {
using namespace util;

namespace {

class FiberReadFile final : public ReadonlyFile {
 public:
  FiberReadFile(const FiberReadOptions& opts, ReadonlyFile* next,
                util::fibers_ext::FiberQueueThreadPool* tp);

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) MUST_USE_RESULT;

  // releases the system handle for this file.
  Status Close() override { return next_->Close(); }

  size_t Size() const { return next_->Size(); }

  int Handle() const { return next_->Handle(); }

 private:
  ssize_t InlineRead(size_t offset, const strings::MutableByteRange& range);

  /*strings::MutableByteRange prefetch_;
  size_t file_prefetch_offset_ = 0;
  std::unique_ptr<uint8_t[]> buf_;
  size_t buf_size_ = 0;*/
  std::unique_ptr<ReadonlyFile> next_;

  fibers_ext::FiberQueueThreadPool* tp_;
  FiberReadOptions::Stats* stats_ = nullptr;
  fibers_ext::Done done_;
  bool nowait_supported_ = true;
};

class WriteFileImpl : public WriteFile {
 public:
  WriteFileImpl(WriteFile* real, ssize_t hash, util::fibers_ext::FiberQueueThreadPool* tp)
      : WriteFile(real->create_file_name()), real_(real), tp_(tp), hash_(hash) {}

  bool Open() final;

  bool Close() final;

  Status Write(const uint8* buffer, uint64 length) final;

 private:
  virtual ~WriteFileImpl() {}

  WriteFile* real_;

  util::fibers_ext::FiberQueueThreadPool* tp_;
  ssize_t hash_;
};

FiberReadFile::FiberReadFile(const FiberReadOptions& opts, ReadonlyFile* next,
                             util::fibers_ext::FiberQueueThreadPool* tp)
    : next_(next), tp_(tp) {
#if 0
  buf_size_ = opts.prefetch_size;
  if (buf_size_) {
    buf_.reset(new uint8_t[buf_size_]);
  }
#endif
  stats_ = opts.stats;
}

ssize_t FiberReadFile::InlineRead(size_t offset, const strings::MutableByteRange& range) {
  ssize_t res;
  iovec io{range.data(), range.size()};

  if (nowait_supported_) {
    res = preadv2(next_->Handle(), &io, 1, offset, RWF_NOWAIT);
    if (res > 0) {
      if (stats_)
        stats_->page_bytes += res;

      if (static_cast<size_t>(res) == io.iov_len)
        return res;

      offset += res;
      io.iov_base = reinterpret_cast<char*>(io.iov_base) + res;
      io.iov_len -= res;
    } else {
      if (errno == EOPNOTSUPP) {
        nowait_supported_ = false;
      } else {
        CHECK_EQ(EAGAIN, errno) << strerror(errno);
      }
    }
  }

  tp_->Add([&] {
    // TBD: Need to loop like in file.cc
    res = pread(next_->Handle(), io.iov_base, io.iov_len, offset);
    done_.Notify();
  });
  done_.Wait();
  done_.Reset();

  if (stats_ && res > 0)
    stats_->tp_bytes += res;

  return res;
}

StatusObject<size_t> FiberReadFile::Read(size_t offset, const strings::MutableByteRange& range) {
  ssize_t res = InlineRead(offset, range);
  if (res < 0)
    return file::StatusFileError();
  return res;
#if 0
  size_t copied = 0;

  if (!prefetch_.empty()) {
    // We could put a smarter check but for sequential access it's enough.
    if (offset == file_prefetch_offset_) {
      size_t copied = std::min(prefetch_.size(), range.size());

      memcpy(range.data(), prefetch_.data(), copied);
      offset += copied;
      file_prefetch_offset_ = offset;
      prefetch_.remove_prefix(copied);

      if (prefetch_.size() > buf_size_ / 4) {
        return copied;
      }

      // with circular_buffer we could fully use iovec interface and that would save us these
      // rotations.
      if (!prefetch_.empty()) {
        memcpy(buf_.get(), prefetch_.data(), prefetch_.size());
        prefetch_.reset(buf_.get(), prefetch_.size());
      }
    } else {
      prefetch_.clear();
    }
  }

  if (copied < range.size()) {
    DCHECK(prefetch_.empty());

    iovec io[2];
    io[0].iov_base = range.data() + copied;
    io[0].iov_len = range.size() - copied;
    io[1].iov_base = buf_.get() + prefetch_.size();
    io[1].iov_len = buf_size_ - prefetch_.size();
    file_prefetch_offset_ = offset + io[0].iov_len;

    ssize_t res = preadv2(next_->Handle(), io, 2, offset, RWF_NOWAIT);
    if (res < io[0].iov_len) {
      if (res < 0) {
        CHECK_EQ(errno, EAGAIN);  // TBD.
      } else {
        io[0].iov_base = reinterpret_cast<char*>(io[0].iov_base) + res;
        io[0].iov_len -= res;
      }
      // TBD: handle large reads (res is less that we specified).
      res = tp_->Await([&] { return preadv2(next_->Handle(), io, 2, offset, 0); });
      if (res < 0)
        return file::StatusFileError();
      CHECK_EQ(res, io[0].iov_len + io[1].iov_len);
      prefetch_.reset(buf_.get(), buf_size_);
    } else {
      prefetch_.reset(buf_.get(), prefetch_.size() + (res - io[0].iov_len));
    }

    return range.size();
  }

  async_prefetch_res_.store(-2, std::memory_order_relaxed);

  iovec io {prefetch_.data(), buf_size_ - prefetch_.size()};
  tp_->Add([this, io, offs = file_prefetch_offset_ + prefetch_.size()] () {
    ssize_t res = preadv2(next_->Handle(), &io, 1, offs , 0);
    async_prefetch_res_.store(res, std::memory_order_release);
    ec_.notify();
  });
#endif
}

bool WriteFileImpl::Open() {
  LOG(FATAL) << "Should not be called";
  return false;
}

bool WriteFileImpl::Close() {
  if (!real_)
    return false;

  bool res = tp_->Await([this] { return real_->Close(); });
  delete this;
  return res;
}

Status WriteFileImpl::Write(const uint8* buffer, uint64 length) {
  auto cb = [&] { return real_->Write(buffer, length); };
  if (hash_ < 0)
    return tp_->Await(std::move(cb));
  else
    return tp_->Await(hash_, std::move(cb));
}

}  // namespace

StatusObject<ReadonlyFile*> OpenFiberReadFile(StringPiece name,
                                              util::fibers_ext::FiberQueueThreadPool* tp,
                                              const FiberReadOptions& opts) {
  StatusObject<ReadonlyFile*> res = ReadonlyFile::Open(name, opts);
  if (!res.ok())
    return res;
  return new FiberReadFile(opts, res.obj, tp);
}

WriteFile* OpenFiberWriteFile(StringPiece name, util::fibers_ext::FiberQueueThreadPool* tp,
                              const FiberWriteOptions& opts) {
  WriteFile* wf = Open(name, opts);
  if (!wf)
    return nullptr;
  ssize_t hash = -1;
  if (opts.consistent_thread)
    hash = base::MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(name.data()), name.size(), 1);
  return new WriteFileImpl(wf, hash, tp);
}

}  // namespace file
