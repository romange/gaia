// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/zlib_source.h"

#include <memory>
#include "strings/strcat.h"
#include "base/logging.h"

namespace util {

inline Status ToStatus(int err, StringPiece msg) {
  return Status(StatusCode::IO_ERROR,
                StrCat("ZLib error ", err, ": ",  msg));
}

static inline int internalInflateInit2(ZlibSource::Format format,
    z_stream* zcontext) {
  int windowBitsFormat = 0;
  switch (format) {
    case ZlibSource::GZIP: windowBitsFormat = 16; break;
    case ZlibSource::AUTO: windowBitsFormat = 32; break;
    case ZlibSource::ZLIB: windowBitsFormat = 0; break;
  }
  return inflateInit2(zcontext, /* windowBits */15 | windowBitsFormat);
}

bool ZlibSource::IsZlibSource(Source* source) {
  std::array<unsigned char, 2> buf;
  auto res = source->Read(strings::MutableByteRange(buf));
  if (!res.ok())
    return false;

  bool is_zlib = res.obj == 2 && (buf[0] == 0x1f) && (buf[1] == 0x8b);
  source->Prepend(strings::ByteRange(buf));

  return is_zlib;
}

static constexpr size_t kBufSize = 8192;

ZlibSource::ZlibSource(
    Source* sub_stream, Format format)
    : sub_stream_(sub_stream), format_(format) {
  zcontext_.zalloc = Z_NULL;
  zcontext_.zfree = Z_NULL;
  zcontext_.opaque = Z_NULL;
  zcontext_.total_out = 0;
  zcontext_.next_in = NULL;
  zcontext_.avail_in = 0;
  zcontext_.total_in = 0;
  zcontext_.msg = NULL;

  int zerror = internalInflateInit2(format_, &zcontext_);
  CHECK_EQ(Z_OK, zerror);
  buf_.reset(new uint8_t[kBufSize]);
}

ZlibSource::~ZlibSource() {
  inflateEnd(&zcontext_);
  delete sub_stream_;
}

StatusObject<size_t> ZlibSource::ReadInternal(const strings::MutableByteRange& range) {
  zcontext_.next_out = range.begin();
  zcontext_.avail_out = range.size();

  while (true) {
    if (zcontext_.avail_in > 0) {
      int zerror = inflate(&zcontext_, Z_NO_FLUSH);
      if (zerror != Z_OK && zerror != Z_STREAM_END) {
        return ToStatus(zerror, zcontext_.msg);
      }

      if (zcontext_.next_out == range.end())
        break;
      DCHECK_EQ(0, zcontext_.avail_in);
    }

    auto res = sub_stream_->Read(strings::MutableByteRange(buf_.get(), kBufSize));
    if (!res.ok())
      return res;

    if (res.obj == 0)
      break;

    DVLOG(1) << "Read " << res.obj << " bytes";

    zcontext_.next_in = buf_.get();
    zcontext_.avail_in = res.obj;
  }

  if (zcontext_.avail_in > 0) {
    CHECK_EQ(0, zcontext_.avail_out);
  }

  return zcontext_.next_out - range.begin();
}

}  // namespace util

