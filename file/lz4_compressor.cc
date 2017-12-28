// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <lz4.h>

#include "util/compressors.h"

#include "base/init.h"

#include "strings/stringpiece.h"

namespace {

using base::Status;
using base::StatusCode;
using strings::charptr;

Status UncompressZ4(const void* src, size_t len, void* dest, size_t* uncompress_size) {
  int res = LZ4_decompress_safe((const char*)src, (char*)dest, len, *uncompress_size);
  if (res <= 0) {
    return Status(StatusCode::INTERNAL_ERROR);
  }
  *uncompress_size = res;
  return Status::OK;
}

size_t BoundFunctionLZ4(size_t len) {
  return LZ4_compressBound(len);
}

Status CompressLZ4(int level, const void* src, size_t len, void* dest,
                    size_t* compress_size) {
  int res = LZ4_compress_fast((const char*)src, (char*)dest, len, *compress_size, 1);
  if (res <= 0) {
    return Status(StatusCode::INTERNAL_ERROR);
  }
  *compress_size = res;
  return Status::OK;
}

}  // namespace

using namespace ::util::compressors;

REGISTER_MODULE_INITIALIZER(lz4codec, {
  internal::Register(LZ4_METHOD, &BoundFunctionLZ4, &CompressLZ4, &UncompressZ4);
});

namespace util {
namespace compressors {

int dummy_lz4() { return 0; }

}
}

// REGISTER_COMPRESS(
