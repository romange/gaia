// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/zlib_source.h"

#include <memory>
#include "base/logging.h"
#include "strings/strcat.h"

namespace util {

using base::Status;
using base::StatusObject;
using base::StatusCode;

bool ZlibSource::IsZlibSource(Source* source) {
  std::array<unsigned char, 2> buf;
  auto res = source->Read(strings::MutableByteRange(buf));
  if (!res.ok())
    return false;

  bool is_zlib = res.obj == 2 && (buf[0] == 0x1f) && (buf[1] == 0x8b);
  source->Prepend(strings::ByteRange(buf));

  return is_zlib;
}

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
}

ZlibSource::~ZlibSource() {
  inflateEnd(&zcontext_);
  delete sub_stream_;
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


StatusObject<size_t> ZlibSource::ReadInternal(const strings::MutableByteRange& range) {
  std::array<unsigned char, 1024> buf;

  zcontext_.next_out = range.begin();
  zcontext_.avail_out = range.size();
  do {
    auto res = sub_stream_->Read(strings::MutableByteRange(buf));
    if (!res.ok())
      return res;

    if (res.obj == 0)
      break;

    bool first_time = zcontext_.next_in == nullptr;
    zcontext_.next_in = buf.begin();
    zcontext_.avail_in = res.obj;
    if (first_time) {
      int zerror = internalInflateInit2(format_, &zcontext_);
      if (zerror != Z_OK) {
        return Status(StatusCode::IO_ERROR,
                      StrCat("ZLib error ", zerror, ": ",  zcontext_.msg));
      }
    }
    int zerror = inflate(&zcontext_, Z_NO_FLUSH);

    if (zerror != Z_OK && zerror != Z_STREAM_END) {
      return Status(StatusCode::IO_ERROR,
                      StrCat("ZLib error ", zerror, ": ",  zcontext_.msg));
    }

    if (zcontext_.avail_in > 0) {
      CHECK_EQ(0, zcontext_.avail_out);

      sub_stream_->Prepend(StringPiece(zcontext_.next_in, zcontext_.avail_in));
    }
  } while (zcontext_.next_out != range.end());

  return zcontext_.next_out - range.begin();
}

}  // namespace util

