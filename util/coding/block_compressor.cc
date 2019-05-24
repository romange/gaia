// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/coding/block_compressor.h"

#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include "base/logging.h"

namespace util {

using strings::ByteRange;
using strings::MutableByteRange;
using std::ostream;

ostream& operator<<(ostream& os, const ZSTD_parameters& p) {
  os << "wlog: " << p.cParams.windowLog << ", clog: " << p.cParams.chainLog << ", strategy: "
     << p.cParams.strategy << ", slog: " << p.cParams.searchLog << ", cntflag: "
     << p.fParams.contentSizeFlag << ", hashlog: " << p.cParams.hashLog;
  return os;
}


#define HANDLE ((ZSTD_CCtx*)zstd_cntx_)

#define CHECK_ZSTDERR(res) do { auto foo = (res); \
    CHECK(!ZSTD_isError(foo)) << ZSTD_getErrorName(foo); } while(false)

BlockCompressor::BlockCompressor() {
  zstd_cntx_ = ZSTD_createCCtx();
}

BlockCompressor::~BlockCompressor() {
  ZSTD_freeCCtx(HANDLE);
}

void BlockCompressor::Start() {
  CHECK(compressed_bufs_.empty() && compressed_blocks_.empty());

  ZSTD_parameters params{ZSTD_getCParams(6, 0, 0), ZSTD_frameParameters()};

  // To have 128KB window size we need to set twice more:
  // effective windowSize = 2^windowLog - block_size.
  params.cParams.windowLog = BLOCK_SIZE_LOG + 1;
  params.cParams.hashLog = BLOCK_SIZE_LOG - 2;

  VLOG(1) << "Starting with " << params;

  size_t res = ZSTD_compressBegin_advanced(HANDLE, nullptr, 0, params, ZSTD_CONTENTSIZE_UNKNOWN);
  CHECK_ZSTDERR(res);

  if (!double_buf_) {
    double_buf_.reset(new uint8_t[BLOCK_SIZE * 2 + 1]);
  }
  pos_ = 0;
  compress_block_size_ = ZSTD_compressBound(BLOCK_SIZE);
}

void BlockCompressor::Add(strings::ByteRange br) {
  if (br.empty())
    return;

  if (compress_block_size_ == 0) {
    Start();
  }

  // TODO: There is room for mem. optimizations here.
  // For example, we could compact consequent compressed blocks together if they end up less
  // than compress_block_size_.
  while (pos_ + br.size() >= BLOCK_SIZE) {
    size_t to_copy = BLOCK_SIZE - pos_;
    memcpy(buf_start() + pos_, br.data(), to_copy);
    pos_ = BLOCK_SIZE;

    br.advance(to_copy);
    Compress();
  }

  if (br.empty())
    return;

  // Copy the data into input buffer.
  memcpy(buf_start() + pos_, br.data(), br.size());
  pos_ += br.size();
}

void BlockCompressor::Finalize() {
  if (compress_block_size_ == 0)
    return;

  // At this point either we have pending data and/or compressed data.
  CompressInternal(true);

  // Signal that next time we should Start() again.
  compress_block_size_ = 0;
}

void BlockCompressor::CompressInternal(bool finalize_frame) {
  if (!finalize_frame && pos_ == 0)
    return;

  std::unique_ptr<uint8_t[]> cbuf(new uint8_t[compress_block_size_]);

  auto func = finalize_frame ? ZSTD_compressEnd : ZSTD_compressContinue;
  size_t res = func(HANDLE, cbuf.get(), compress_block_size_, buf_start(), pos_);
  CHECK_ZSTDERR(res);

  // TODO: We could optimize memory by compacting compressed buffers together.
  compressed_blocks_.emplace_back(cbuf.get(), res);
  compressed_bufs_.push_back(std::move(cbuf));
  compressed_size_ += res;
  VLOG(1) << "Compressed from " << pos_ << " to " << res;

  cur_buf_index_ ^= 1;
  pos_ = 0;
}


strings::MutableByteRange BlockCompressor::BlockBuffer() {
  if (compress_block_size_ == 0) {
    Start();
  }
  return MutableByteRange(buf_start() + pos_, BLOCK_SIZE - pos_);
}


bool BlockCompressor::Commit(size_t sz) {
  DCHECK_LE(sz + pos_, BLOCK_SIZE);
  pos_ += sz;
  if (pos_ == BLOCK_SIZE) {
    CompressInternal(false);
    return true;
  }
  return false;
}

void BlockCompressor::ClearCompressedData() {
  compressed_blocks_.clear();
  compressed_bufs_.clear();
  compressed_size_ = 0;
}


#undef HANDLE
#define HANDLE ((ZSTD_DCtx*)zstd_dcntx_)

BlockDecompressor::BlockDecompressor() {
  zstd_dcntx_ = ZSTD_createDCtx();
}

BlockDecompressor::~BlockDecompressor() {
  ZSTD_freeDCtx(HANDLE);
}

int32_t BlockDecompressor::Decompress(strings::ByteRange br, uint32_t* consumed) {
  VLOG(1) << "Decompress " << br.size() << " bytes, frame_state_: " << frame_state_;

  if (frame_state_ & 2) {
    ZSTD_frameHeader params;
    size_t res = ZSTD_getFrameHeader(&params, br.data(), br.size());
    CHECK_EQ(0, res);
    CHECK_LE(params.windowSize, BLOCK_SIZE * 2);
    frame_state_ = 1;

    CHECK_EQ(0, ZSTD_decompressBegin(HANDLE));
    if (!buf_) {
      buf_.reset(new uint8_t[BLOCK_SIZE*2]);
    }
  }

  *consumed = 0;
  decompress_size_ = 0;

  uint32_t sz = ZSTD_nextSrcSizeToDecompress(HANDLE);

  frame_state_ ^= 1;  // flip buffer index.

  while (sz > 0) {
    if (sz > br.size()) {
      frame_state_ ^= 1;  // revert buffer index.
      return -sz;
    }

    size_t res = ZSTD_decompressContinue(HANDLE, buf_.get() + BLOCK_SIZE * frame_state_, BLOCK_SIZE,
                                         br.data(), sz);
    CHECK_ZSTDERR(res);
    *consumed += sz;
    br.advance(sz);
    sz = ZSTD_nextSrcSizeToDecompress(HANDLE);

    if (res > 0) {
      decompress_size_ = res;
      break;
    }
  }

  if (sz == 0) {
    frame_state_ |= 2;  // We must preserve LSB for subsequent GetDecompressedBlock.
    return 0;
  }

  return 1;
}

strings::ByteRange BlockDecompressor::GetDecompressedBlock() const {
  unsigned bindex = frame_state_ & 1;
  return ByteRange(buf_.get() + BLOCK_SIZE * bindex, decompress_size_);
}

}  // namespace util


