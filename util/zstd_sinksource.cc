// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#define ZSTD_STATIC_LINKING_ONLY

#include <zstd.h>

#include "util/zstd_sinksource.h"

#include "base/logging.h"

using base::StatusCode;
using base::Status;
using base::StatusObject;

namespace util {

/*
static void* my_alloc(void* opaque, size_t size) {
  VLOG(1) << "my_alloc " << size;
  return malloc(size);
}

static void  my_free(void* opaque, void* address) {
  return free(address);
}
*/

#define HANDLE reinterpret_cast<ZSTD_CStream*>(zstd_handle_)

inline Status ZstdStatus(size_t res) {
  return Status(StatusCode::IO_ERROR, ZSTD_getErrorName(res));
}


size_t ZStdSink::CompressBound(size_t src_size) {
  return ZSTD_compressBound(src_size);
}

ZStdSink::ZStdSink(Sink* upstream) : upstream_(upstream) {
  buf_sz_ = ZSTD_CStreamOutSize();
  buf_.reset(new char[buf_sz_]);

  // ZSTD_customMem mem_params{my_alloc, my_free, nullptr};

  zstd_handle_ = ZSTD_createCStream();
  VLOG(1) << "Allocated " << buf_sz_ << " bytes";
}

ZStdSink::~ZStdSink() {
  ZSTD_freeCStream(HANDLE);
}


Status ZStdSink::Init(int level) {
  size_t const res = ZSTD_initCStream_srcSize(HANDLE, level, 0);
  if (ZSTD_isError(res)) {
    return ZstdStatus(res);
  }
  VLOG(1) << "allocated " << ZSTD_sizeof_CStream(HANDLE);
  return Status::OK;
}

Status ZStdSink::Append(const strings::ByteRange& slice) {
  ZSTD_inBuffer input = { slice.data(), slice.size(), 0 };
  while (input.pos < input.size) {
    ZSTD_outBuffer out_buf{ buf_.get(), buf_sz_, 0};
    size_t res = ZSTD_compressStream(HANDLE, &out_buf , &input);
    if (ZSTD_isError(res)) {
      return ZstdStatus(res);
    }
    RETURN_IF_ERROR(upstream_->Append(StringPiece(buf_.get(), out_buf.pos)));
  }
  return Status::OK;
}

Status ZStdSink::Flush() {
  ZSTD_outBuffer out_buf{buf_.get(), buf_sz_, 0};

  size_t res = ZSTD_endStream(HANDLE, &out_buf);
  if (ZSTD_isError(res)) {
    return ZstdStatus(res);
  }
  CHECK_EQ(0, res);
  if (out_buf.pos) {
    RETURN_IF_ERROR(upstream_->Append(StringPiece(buf_.get(), out_buf.pos)));
  }
  return upstream_->Flush();
}


#define DC_HANDLE reinterpret_cast<ZSTD_DStream*>(zstd_handle_)

bool ZStdSource::HasValidHeader(Source* upstream) {
  uint8_t buf[4];
  auto res = upstream->Read(strings::MutableByteRange(buf, arraysize(buf)));
  if (!res.ok() || res.obj != arraysize(buf))
    return false;
  upstream->Prepend(strings::ByteRange(buf, arraysize(buf)));

  return ZSTD_isFrame(buf, arraysize(buf));
}


const unsigned kReadBuf = 1 << 12;

ZStdSource::ZStdSource(Source* upstream)
    : sub_stream_(upstream) {
  CHECK(upstream);
  zstd_handle_ = ZSTD_createDStream();
  size_t const res = ZSTD_initDStream(DC_HANDLE);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  buf_.reset(new uint8_t[kReadBuf]);
}

ZStdSource::~ZStdSource() {
  ZSTD_freeDStream(DC_HANDLE);
}


StatusObject<size_t> ZStdSource::ReadInternal(const strings::MutableByteRange& range) {
  ZSTD_outBuffer output = { range.begin(), range.size(), 0 };
  do {
    if (buf_range_.empty()) {
      auto res = sub_stream_->Read(strings::MutableByteRange(buf_.get(), kReadBuf));
      if (!res.ok())
        return res;
      if (res.obj == 0)
        break;
      buf_range_.reset(buf_.get(), res.obj);
    }

    ZSTD_inBuffer input{buf_range_.begin(), buf_range_.size(), 0 };

    size_t to_read = ZSTD_decompressStream(DC_HANDLE, &output , &input);
    if (ZSTD_isError(to_read)) {
      return ZstdStatus(to_read);
    }

    buf_range_.advance(input.pos);
    if (input.pos < input.size) {
      CHECK_EQ(output.pos, output.size);
      break;
    }
  } while (output.pos < output.size);
  return output.pos;
}

}  // namespace util
