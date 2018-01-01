// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <memory>
#include <vector>
#include "strings/range.h"

/* Zstd based wrapper that has the following properties:
   1. It compresses input data into a zstd frame.
   2. The frame will consist of blocks that each one decompress into output buffers of exactly 128K
      unless it's a last block.
   3. During the decompression a previous block in addition to the current must be kept in RAM
      to allow back referencing in the decompressor.

   The flow sequence is: Add*, Finalize. Can be applied multiple times for the same
   block compressor object.
*/
namespace puma {

class BlockCompressor {
 public:
  enum { BLOCK_SIZE_LOG = 17, BLOCK_SIZE = 1 << BLOCK_SIZE_LOG };

  BlockCompressor();

  ~BlockCompressor();

  void Add(uint8_t b) {
    if (compress_block_size_ == 0) {
      Start();
    }
    buf_start()[pos_++] = b;
    if (pos_ == BLOCK_SIZE) {
      Compress();
    }
  }

  void Add(strings::ByteRange br);

  // Flushes and compresses all the pending data and finalizes the frame.
  // If no data was added, compressed_blocks() will return empty vector.
  void Finalize();

  // Flushes the pending data without finalizing the frame. Allows compression of blocks
  // smaller than BLOCK_SIZE.
  // Finalize should still be called to write a valid zstd frame.
  void Compress() { CompressInternal(false); }

  const std::vector<strings::ByteRange>& compressed_blocks() const {
    return compressed_blocks_;
  }

  size_t compressed_size() const { return compressed_size_; }
  size_t pending_size() const { return pos_; }

  // Zero-copy API - saves redundant memory copy.
  // The flow is to get destination buffer to write directly by calling to BlockBuffer.
  // Then the user should write into this buffer starting from its begin position.
  // Once the data is written a user should call "Commit(size)" with how much data was actually
  // written there.
  strings::MutableByteRange BlockBuffer();

  // Commits the write into the compressor. Returns true if the current block was fully filled and
  // compressed, otherwise false is returned.
  bool Commit(size_t sz);

  // Needed for correctly returning "compressed_size()" after finalizing the frame.
  void ClearCompressedData();

 private:
  void Start();
  void CompressInternal(bool finalize_frame);

  const uint8_t* buf_start() const {
    return double_buf_.get() + (BLOCK_SIZE + 1) * cur_buf_index_;
  }

  uint8_t* buf_start() {
    return double_buf_.get() + (BLOCK_SIZE + 1) * cur_buf_index_;
  }

  void* zstd_cntx_ = nullptr;
  size_t compress_block_size_ = 0;
  size_t pos_ = 0;

  std::vector<std::unique_ptr<uint8_t[]>> compressed_bufs_;
  std::vector<strings::ByteRange> compressed_blocks_;
  size_t compressed_size_ = 0;
  std::unique_ptr<uint8_t[]> double_buf_;
  unsigned cur_buf_index_ = 0; // 0 or 1
};

class BlockDecompressor {
 public:
  enum { BLOCK_SIZE = BlockCompressor::BLOCK_SIZE};

  BlockDecompressor();

  ~BlockDecompressor();

  // Returns 0 if decompression of the frame is ended, 1 if it's still going.
  // In any case "*consumed" will hold how many bytes were consumed from br.
  // If negative number is returned - then last portion of br is too small to decompress
  // In that case, the -(return value) will tell how many input bytes are needed.
  int Decompress(strings::ByteRange br, uint32_t* consumed);

  // Can be called after successfuly Decompress call.
  strings::ByteRange GetDecompressedBlock() const;

 private:
  void* zstd_dcntx_ = nullptr;
  unsigned frame_state_ = 2;  // bit 1 for init state; bit 0 - which block to write to.
  std::unique_ptr<uint8_t[]> buf_;
  size_t decompress_size_ = 0;
};



}  // namespace puma
