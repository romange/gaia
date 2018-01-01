// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/coding/sequence_array.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "base/endian.h"
#include "base/flit.h"
#include "base/logging.h"

namespace flit = base::flit;

namespace util {

namespace {

template<typename C> size_t byte_size(const C& container) {
  return container.size() * sizeof(typename C::value_type);
}

inline size_t GetContentSizeChecked(const uint8_t* src, uint32_t size) {
  size_t content_size = ZSTD_getFrameContentSize(src, size);

  CHECK_NE(ZSTD_CONTENTSIZE_ERROR, content_size);
  CHECK_NE(ZSTD_CONTENTSIZE_UNKNOWN, content_size);
  return content_size;
}

}  // namespace

size_t SequenceArray::GetMaxSerializedSize() const {
  size_t sz1 = len_.size() * flit::Traits<uint32_t>::max_size;
  return ZSTD_compressBound(data_.size()) + ZSTD_compressBound(sz1) + 4;
}

size_t SequenceArray::SerializeTo(uint8* dest) const {
  base::PODArray<uint8_t> len_buf;
  len_buf.reserve(len_.size() * flit::Traits<uint32_t>::max_size);
  size_t len_size = 0;

  for (const uint32_t val : len_) {
    len_size += flit::EncodeT<uint32_t>(val, len_buf.data() + len_size);
  }
  CHECK_LE(len_size, len_buf.capacity());

  uint8* next = dest + 4;
  size_t res1 = ZSTD_compress(next, ZSTD_compressBound(len_size), len_buf.data(), len_size, 1);
  CHECK(!ZSTD_isError(res1)) << ZSTD_getErrorName(res1);

  next += res1;
  LittleEndian::Store32(dest, res1);

  size_t res2 = ZSTD_compress(next, ZSTD_compressBound(data_.size()), data_.data(),
                              data_.size(), 1);
  CHECK(!ZSTD_isError(res2)) << ZSTD_getErrorName(res2);

  VLOG(1) << "SequenceArray::SerializeTo: from " << byte_size(len_) << "/" << data_size()
          << " to " << res1 << "/" << res2 << " bytes";
  return res1 + res2 + 4;
}

void SequenceArray::SerializeFrom(const uint8_t* src, uint32_t count) {
  clear();

  CHECK_GT(count, 4);
  uint32_t len_sz = LittleEndian::Load32(src);

  CHECK_LT(4 + len_sz, count);
  src += 4;

  size_t len_content_size = GetContentSizeChecked(src, len_sz);

  // +8 to allow fast and safe flit parsing.
  std::unique_ptr<uint8_t[]> len_buf(new uint8_t[len_content_size + 8]);

  size_t res = ZSTD_decompress(len_buf.get(), len_content_size, src, len_sz);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  CHECK_EQ(res, len_content_size);

  const uint8_t* next = len_buf.get();
  while (next < len_buf.get() + res) {
    uint32_t val;
    next += flit::ParseT(next, &val);
    len_.push_back(val);
  }
  CHECK_EQ(next, len_buf.get() + res);
  uint32_t buf_sz = count - 4 - len_sz;
  src += len_sz;
  size_t buf_content_size = GetContentSizeChecked(src, buf_sz);
  data_.resize(buf_content_size);

  res = ZSTD_decompress(data_.data(), buf_content_size, src, buf_sz);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  CHECK_EQ(buf_content_size, res);
}


}  // namespace util

