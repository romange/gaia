// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/fiber_file.h"

#include "base/hash.h"
#include "base/logging.h"

namespace file {
using namespace util;

namespace {

class FiberReadFile final : public ReadonlyFile {
 public:
  FiberReadFile(ReadonlyFile* next, util::fibers_ext::FiberQueueThreadPool* tp)
      : next_(next), tp_(tp) {}

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) MUST_USE_RESULT;

  // releases the system handle for this file.
  Status Close() override { return next_->Close(); }

  size_t Size() const override { return next_->Size(); }

 private:
  std::unique_ptr<ReadonlyFile> next_;
  util::fibers_ext::FiberQueueThreadPool* tp_;
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

StatusObject<size_t> FiberReadFile::Read(size_t offset, const strings::MutableByteRange& range) {
  StatusObject<size_t> res = tp_->Await([&] { return next_->Read(offset, range); });
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
                                              const ReadonlyFile::Options& opts) {
  StatusObject<ReadonlyFile*> res = ReadonlyFile::Open(name, opts);
  if (!res.ok())
    return res;
  return new FiberReadFile(res.obj, tp);
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
