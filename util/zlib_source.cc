// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/zlib_source.h"

#include <memory>
#include "base/logging.h"
#include "strings/strcat.h"

namespace util {

inline Status ToStatus(int err, StringPiece msg) {
  if (msg.empty())   // to overcome https://github.com/abseil/abseil-cpp/issues/315
    msg = "";
  return Status(StatusCode::IO_ERROR, StrCat("ZLib error ", err, ": ", msg));
}

static inline int internalInflateInit2(ZlibSource::Format format, z_stream* zcontext) {
  int windowBitsFormat = 0;
  switch (format) {
    case ZlibSource::GZIP:
      windowBitsFormat = 16;
      break;
    case ZlibSource::AUTO:
      windowBitsFormat = 32;
      break;
    case ZlibSource::ZLIB:
      windowBitsFormat = 0;
      break;
  }
  return inflateInit2(zcontext, /* windowBits */ 15 | windowBitsFormat);
}

static inline void InitCtx(z_stream* zcontext) {
  memset(zcontext, 0, sizeof(z_stream));
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

ZlibSource::ZlibSource(Source* sub_stream, Format format)
    : sub_stream_(sub_stream), format_(format) {
  InitCtx(&zcontext_);
  buf_.reset(new uint8_t[kBufSize]);
}

ZlibSource::~ZlibSource() {
  inflateEnd(&zcontext_);  // might return error because zcontext_ might already be freed.
  delete sub_stream_;
}

StatusObject<size_t> ZlibSource::ReadInternal(const strings::MutableByteRange& range) {
  zcontext_.next_out = range.begin();
  zcontext_.avail_out = range.size();

  while (true) {
    if (zcontext_.avail_in > 0) {
      int zerror = inflate(&zcontext_, Z_NO_FLUSH);

      if (zerror != Z_OK) {
        if (zerror == Z_STREAM_END) {
          // There may be multiple zlib-streams and inflate stops when it encounters
          // Z_STREAM_END before all the requested data is inflated.
          CHECK_EQ(0, inflateEnd(&zcontext_));
          if (zcontext_.avail_in) {
            int reset = internalInflateInit2(format_, &zcontext_);
            CHECK_EQ(Z_OK, reset);
          }
          continue;
        }

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
    if (!zcontext_.state) {
      int reset = internalInflateInit2(format_, &zcontext_);
      CHECK_EQ(Z_OK, reset);
    }
  }

  if (zcontext_.avail_in > 0) {
    CHECK_EQ(0, zcontext_.avail_out);
  }

  return zcontext_.next_out - range.begin();
}

ZlibSink::ZlibSink(Sink* sub, unsigned level, size_t buf_size)
    : sub_(sub), buf_(new uint8_t[buf_size]), buf_size_(buf_size) {
  InitCtx(&zcontext_);

  int lev = level == 0 ? Z_DEFAULT_COMPRESSION : level;
  int zerror = deflateInit2(&zcontext_, lev, Z_DEFLATED, 15 | 16, 8, Z_DEFAULT_STRATEGY);
  CHECK_EQ(Z_OK, zerror);

  zcontext_.next_out = buf_.get();
  zcontext_.avail_out = buf_size_;
}

ZlibSink::~ZlibSink() { deflateEnd(&zcontext_); }

Status ZlibSink::Append(const strings::ByteRange& slice) {
  zcontext_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(slice.data()));
  zcontext_.avail_in = slice.size();

  int zerror = deflate(&zcontext_, Z_NO_FLUSH);
  if (zerror != Z_OK)
    return ToStatus(zerror, zcontext_.msg);

  while (zcontext_.avail_out == 0) {
    strings::ByteRange br(buf_.get(), buf_size_);
    auto status = sub_->Append(br);
    if (!status.ok()) asm("int $3");
    RETURN_IF_ERROR(status);

    zcontext_.next_out = buf_.get();
    zcontext_.avail_out = buf_size_;

    int zerror = deflate(&zcontext_, Z_NO_FLUSH);
    if (zerror != Z_OK)
      return ToStatus(zerror, zcontext_.msg);
  }
  CHECK_EQ(0, zcontext_.avail_in);

  return Status::OK;
}

Status ZlibSink::Flush() {
  while (true) {
    int zerror = deflate(&zcontext_, Z_FINISH);
    if (zerror == Z_STREAM_END)
      break;
    if (zerror != Z_OK && zerror != Z_BUF_ERROR) {
      return ToStatus(zerror, zcontext_.msg);
    }

    RETURN_IF_ERROR(sub_->Append(strings::ByteRange{buf_.get(), zcontext_.next_out}));
    zcontext_.next_out = buf_.get();
    zcontext_.avail_out = buf_size_;
  }
  RETURN_IF_ERROR(sub_->Append(strings::ByteRange{buf_.get(), zcontext_.next_out}));

  return sub_->Flush();
}

}  // namespace util
