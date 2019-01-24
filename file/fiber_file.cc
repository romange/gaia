// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/fiber_file.h"

namespace file {
namespace {

class FiberReadFile final : public ReadonlyFile {
 public:
  FiberReadFile(ReadonlyFile* next, util::FiberQueueThreadPool* tp) : next_(next), tp_(tp) {}

  // Reads upto length bytes and updates the result to point to the data.
  // May use buffer for storing data. In case, EOF reached sets result.size() < length but still
  // returns Status::OK.
  util::StatusObject<size_t> Read(size_t offset,
                                  const strings::MutableByteRange& range) MUST_USE_RESULT;

  // releases the system handle for this file.
  util::Status Close() override { return next_->Close(); }

  size_t Size() const override { return next_->Size(); }

 private:
  std::unique_ptr<ReadonlyFile> next_;
  util::FiberQueueThreadPool* tp_;
};

util::StatusObject<size_t> FiberReadFile::Read(size_t offset,
                                               const strings::MutableByteRange& range) {
  util::StatusObject<size_t> res = tp_->Await([&] { return next_->Read(offset, range); });
  return res;
}

}  // namespace

util::StatusObject<ReadonlyFile*> OpenFiberReadFile(StringPiece name,
                                                    util::FiberQueueThreadPool* tp,
                                                    const ReadonlyFile::Options& opts) {
  util::StatusObject<ReadonlyFile*> res = ReadonlyFile::Open(name, opts);
  if (!res.ok())
    return res;
  return new FiberReadFile(res.obj, tp);
}

}  // namespace file
