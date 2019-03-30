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

ssize_t read_all(int fd, const iovec* iov, int iovcnt, size_t offset) {
  size_t left = std::accumulate(iov, iov + iovcnt, 0,
                                [](size_t a, const iovec& i2) { return a + i2.iov_len; });

  ssize_t completed = 0;
  iovec tmp_iov[iovcnt];

  std::copy(iov, iov + iovcnt, tmp_iov);
  iovec* next_iov = tmp_iov;
  while (true) {
    ssize_t read = preadv(fd, next_iov, iovcnt, offset);
    if (read <= 0) {
      return read == 0 ? completed : read;
    }

    left -= read;
    completed += read;
    if (left == 0)
      break;

    offset += read;
    while (next_iov->iov_len <= static_cast<size_t>(read)) {
      read -= next_iov->iov_len;
      ++next_iov;
      --iovcnt;
    }
    next_iov->iov_len -= read;
  }
  return completed;
}

class FiberReadFile : public ReadonlyFile {
 public:
  FiberReadFile(const FiberReadOptions& opts, ReadonlyFile* next,
                util::fibers_ext::FiberQueueThreadPool* tp);

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset,
                            const strings::MutableByteRange& range) final MUST_USE_RESULT;

  // releases the system handle for this file.
  Status Close() final;

  size_t Size() const final { return next_->Size(); }

  int Handle() const final { return next_->Handle(); }

 private:
  StatusObject<size_t> ReadAndPrefetch(size_t offset, const strings::MutableByteRange& range);

  strings::MutableByteRange prefetch_;
  size_t file_prefetch_offset_ = -1;
  std::unique_ptr<uint8_t[]> buf_;
  size_t buf_size_ = 0;
  std::unique_ptr<ReadonlyFile> next_;

  fibers_ext::FiberQueueThreadPool* tp_;
  FiberReadOptions::Stats* stats_ = nullptr;
  fibers_ext::Done done_;
  bool pending_prefetch_ = false;
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
  buf_size_ = opts.prefetch_size;
  if (buf_size_) {
    buf_.reset(new uint8_t[buf_size_]);
    prefetch_.reset(buf_.get(), 0);
  }
  stats_ = opts.stats;
}

Status FiberReadFile::Close() {
  if (pending_prefetch_) {
    done_.Wait(AND_RESET);
    pending_prefetch_ = false;
  }
  return next_->Close();
}

StatusObject<size_t> FiberReadFile::ReadAndPrefetch(size_t offset,
                                                    const strings::MutableByteRange& range) {
  size_t copied = 0;

  if (pending_prefetch_ || !prefetch_.empty()) {
    if (pending_prefetch_) {
      done_.Wait(AND_RESET);
      pending_prefetch_ = false;
    }

    DCHECK(prefetch_.empty() || prefetch_.end() <= buf_.get() + buf_size_);

    // We could put a smarter check but for sequential access it's enough.
    if (offset == file_prefetch_offset_) {
      copied = std::min(prefetch_.size(), range.size());

      memcpy(range.data(), prefetch_.data(), copied);
      offset += copied;
      file_prefetch_offset_ = offset;
      prefetch_.remove_prefix(copied);

      if (stats_)
        stats_->prefetch_bytes += copied;

      if (prefetch_.size() > buf_size_ / 8) {
        return copied;
      }

      // with circular_buffer we could fully use iovec interface and that would save us these
      // rotations.
      if (!prefetch_.empty()) {
        memmove(buf_.get(), prefetch_.data(), prefetch_.size());
      }
    } else {
      prefetch_.clear();
    }
  }
  DCHECK(!pending_prefetch_);

  // At this point prefetch_ must point at buf_ and might still contained prefetched slice.
  prefetch_.reset(buf_.get(), prefetch_.size());

  iovec io[2] = {{range.data() + copied, range.size() - copied},
                 {buf_.get() + prefetch_.size(), buf_size_ - prefetch_.size()}};

  if (copied < range.size()) {
    DCHECK(prefetch_.empty());

    ssize_t res;

    tp_->Add([&] {
      res = read_all(next_->Handle(), io, 2, offset);
      done_.Notify();
    });
    done_.Wait(AND_RESET);
    if (res < 0)
      return file::StatusFileError();
    if (static_cast<size_t>(res) <= io[0].iov_len)  // EOF
      return res + copied;

    file_prefetch_offset_ = offset + io[0].iov_len;
    res -= io[0].iov_len;
    prefetch_.reset(buf_.get(), prefetch_.size() + res);

    return range.size();  // Fully read and possibly some prefetched.
  }

  pending_prefetch_ = true;
  struct Pending {
    iovec io;
    size_t offs;
    fibers_ext::Done done;
  } pending{io[1], file_prefetch_offset_ + prefetch_.size(), done_};

  // we filled range but we want to issue a readahead fetch.
  // We must keep reference to done_ in pending because of the shutdown flow.
  tp_->Add([this, pending = std::move(pending)]() mutable {
    ssize_t res = read_all(next_->Handle(), &pending.io, 1, pending.offs);
    // We ignore the error, maximum the system will reread it in the through the main thread.
    if (res > 0) {
      prefetch_.reset(prefetch_.data(), prefetch_.size() + res);
      DCHECK_LE(prefetch_.end() - buf_.get(), buf_size_);
    } else {
      file_prefetch_offset_ = -1;
    }
    pending.done.Notify();
  });

  return range.size();
}

StatusObject<size_t> FiberReadFile::Read(size_t offset, const strings::MutableByteRange& range) {
  StatusObject<size_t> res;
  if (buf_) {
    res = ReadAndPrefetch(offset, range);
    VLOG(1) << "ReadAndPrefetch " << offset << "/" << res.obj;
    return res;
  }

  tp_->Add([&] {
    res = next_->Read(offset, range);
    done_.Notify();
  });
  done_.Wait(AND_RESET);
  VLOG(1) << "Read " << offset << "/" << res.obj;
  return res;
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
