// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <bzlib.h>

#include "base/logging.h"
#include "util/bzip_source.h"
#include "strings/strcat.h"

namespace util {
using strings::ByteRange;

struct BzipSource::Rep {
  bz_stream stream;

  Rep() { memset(&stream, 0, sizeof(stream)); }
};

BzipSource::BzipSource(Source* sub_source)
    : sub_stream_(sub_source), rep_(new Rep) {
  CHECK_EQ(BZ_OK, BZ2_bzDecompressInit(&rep_->stream, 0, 1));
}

BzipSource::~BzipSource() {
  BZ2_bzDecompressEnd(&rep_->stream);
}

StatusObject<size_t> BzipSource::ReadInternal(const strings::MutableByteRange& range) {
  std::array<unsigned char, 1024> buf;
  char* const begin = reinterpret_cast<char*>(range.begin());

  rep_->stream.next_out = begin;
  rep_->stream.avail_out = range.size();
  do {
    auto res = sub_stream_->Read(strings::MutableByteRange(buf));
    if (!res.ok())
      return res;

    if (res.obj == 0)
      break;

    rep_->stream.next_in = reinterpret_cast<char*>(buf.begin());
    rep_->stream.avail_in = res.obj;
    int res_code = BZ2_bzDecompress(&rep_->stream);
    if (res_code != BZ_OK && res_code != BZ_STREAM_END) {
      return Status(StatusCode::IO_ERROR, absl::StrCat("BZip error ", res_code));
    }

    if (rep_->stream.avail_in > 0) {
      CHECK_EQ(0, rep_->stream.avail_out);

      sub_stream_->Prepend(ByteRange(reinterpret_cast<unsigned char*>(rep_->stream.next_in),
                                     rep_->stream.avail_in));
    }
  } while (reinterpret_cast<unsigned char*>(rep_->stream.next_out) != range.end());

  return rep_->stream.next_out - begin;
}


bool BzipSource::IsBzipSource(Source* source) {
  std::array<unsigned char, 3> buf;
  auto res = source->Read(strings::MutableByteRange(buf));
  if (!res.ok())
    return false;

  bool is_bzip = res.obj == 3 && (buf[0] == 'B') && (buf[1] == 'Z') && (buf[2] == 'h');
  source->Prepend(strings::ByteRange(buf));

  return is_bzip;
}
}  // namespace util
