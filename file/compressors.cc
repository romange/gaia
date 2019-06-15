// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "file/compressors.h"

#include <zlib.h>
#include "lz4.h"

#include "base/logging.h"

using util::Status;
using util::StatusCode;

namespace file {
using namespace list_file;

namespace {

size_t BoundFunctionZlib(size_t len) {
  return compressBound(len);
}

Status CompressZlib(int level, const void* src, size_t len, void* dest,
                    size_t* compress_size) {
  // Bytef *dest, uLongf *destLen, const Bytef *source, uLong sourceLen, int level) {
  z_stream stream;
  stream.next_in = (Bytef*)src;
  stream.avail_in = len;
  stream.next_out = (Bytef*)dest;
  stream.avail_out = *compress_size;

  stream.zalloc = 0;
  stream.zfree = 0;
  stream.opaque = (voidpf)0;

  // -15 for writing raw inflate, no headers at all.
  int err = deflateInit2(&stream, level, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY);
  if (err != Z_OK)
    goto err_line2;

  err = deflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    deflateEnd(&stream);
    if (err == Z_OK)
      err = Z_BUF_ERROR;
    goto err_line2;
  }
  *compress_size = stream.total_out;

  err = deflateEnd(&stream);
  if (err == Z_OK)
    return Status::OK;

err_line2:
  return Status(zError(err));
}


Status UncompressZlib(const void* src, size_t len, void* dest, size_t* uncompress_size) {
  z_stream stream;
  int err;

  stream.next_in = (Bytef*)src;
  stream.avail_in = (uInt)len;

  stream.next_out = (Bytef*)dest;
  stream.avail_out = *uncompress_size;

  stream.zalloc = nullptr;
  stream.zfree = nullptr;

  err = inflateInit2(&stream, -15);
  if (err != Z_OK)
    goto err_line;

  err = inflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    inflateEnd(&stream);
    if (err == Z_OK)
      err = Z_BUF_ERROR;
    goto err_line;
  }
  *uncompress_size = stream.total_out;

  err = inflateEnd(&stream);
  if (err == Z_OK)
    return Status::OK;

err_line:
  return Status(zError(err));
}

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



UncompressFunction GetUncompress(CompressMethod m) {
  switch (m) {
    case CompressMethod::kCompressionZlib:
      return UncompressZlib;
    break;
    case CompressMethod::kCompressionLZ4:
      return UncompressZ4;
    break;
    default:;
  }
  return nullptr;
}

CompressFunction GetCompress(CompressMethod m) {
  switch (m) {
    case CompressMethod::kCompressionZlib:
      return CompressZlib;
    break;
    case CompressMethod::kCompressionLZ4:
      return CompressLZ4;
    break;
    default:;
  }
  return nullptr;
}

CompressBoundFunction GetCompressBound(list_file::CompressMethod m) {
  switch (m) {
    case CompressMethod::kCompressionZlib:
      return BoundFunctionZlib;
    break;
    case CompressMethod::kCompressionLZ4:
      return BoundFunctionLZ4;
    break;
    default:;
  }
  return nullptr;
}

}  // namespace file
