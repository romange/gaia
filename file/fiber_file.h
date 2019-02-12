// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "file/file.h"
#include "util/fiberqueue_threadpool.h"

namespace file {

// Fiber-friendly file handler. Returns ReadonlyFile* instance that does not block the current
// thread unlike the regular posix implementation. All the read opearations will run
// in FiberQueueThreadPool.
util::StatusObject<ReadonlyFile*> OpenFiberReadFile(
    StringPiece name, util::FiberQueueThreadPool* tp,
    const ReadonlyFile::Options& opts = ReadonlyFile::Options()) MUST_USE_RESULT;


struct FiberWriteOptions : public OpenOptions {

};

WriteFile* OpenFiberWriteFile(
    StringPiece name, util::FiberQueueThreadPool* tp,
    const FiberWriteOptions& opts = FiberWriteOptions()) MUST_USE_RESULT;


}  // namespace file
