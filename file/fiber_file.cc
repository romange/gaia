// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/fiber_file.h"

#include "base/logging.h"

namespace file {
using namespace util;

namespace {

class FiberReadFile final : public ReadonlyFile {
 public:
  FiberReadFile(ReadonlyFile* next, util::FiberQueueThreadPool* tp) : next_(next), tp_(tp) {}

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  StatusObject<size_t> Read(size_t offset, const strings::MutableByteRange& range) MUST_USE_RESULT;

  // releases the system handle for this file.
  Status Close() override { return next_->Close(); }

  size_t Size() const override { return next_->Size(); }

 private:
  std::unique_ptr<ReadonlyFile> next_;
  util::FiberQueueThreadPool* tp_;
};

class WriteFileImpl : public WriteFile {
 public:
  WriteFileImpl(WriteFile* real, util::FiberQueueThreadPool* tp)
      : WriteFile(real->create_file_name()), real_(real), tp_(tp) {}

  bool Open() final;

  bool Close() final;

  Status Write(const uint8* buffer, uint64 length) final;

 private:
  virtual ~WriteFileImpl() {}

  WriteFile* real_;

  util::FiberQueueThreadPool* tp_;
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
  return tp_->Await([&] { return real_->Write(buffer, length); });
}

}  // namespace

StatusObject<ReadonlyFile*> OpenFiberReadFile(StringPiece name, util::FiberQueueThreadPool* tp,
                                              const ReadonlyFile::Options& opts) {
  StatusObject<ReadonlyFile*> res = ReadonlyFile::Open(name, opts);
  if (!res.ok())
    return res;
  return new FiberReadFile(res.obj, tp);
}

WriteFile* OpenFiberWriteFile(StringPiece name, util::FiberQueueThreadPool* tp,
                              const FiberWriteOptions& opts) {
  WriteFile* wf = Open(name, opts);
  if (!wf)
    return nullptr;
  return new WriteFileImpl(wf, tp);
}

}  // namespace file
