// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _PROTO_WRITER_H
#define _PROTO_WRITER_H

#include <memory>

#include "strings/stringpiece.h"
#include "base/arena.h"
#include "base/integral_types.h"
#include "util/status.h"

namespace google {
namespace protobuf {
class Descriptor;
class MessageLite;
}  // namespace protobuf
}  // namespace google


namespace file {
class ListWriter;

extern const char kProtoSetKey[];
extern const char kProtoTypeKey[];

class BaseProtoWriter {
public:
  explicit BaseProtoWriter(const ::google::protobuf::Descriptor* dscr);

protected:
  const ::google::protobuf::Descriptor* dscr_;
  std::string fd_set_str_;
};

class ListProtoWriter : public BaseProtoWriter {
 public:
  struct Options {
    enum CompressMethod {ZLIB_COMPRESS = 2, LZ4_COMPRESS = 3} compress_method
          = LZ4_COMPRESS;
    uint8 compress_level = 1;

    // if max_entries_per_file > 0 then
    // ProtoWriter uses filename as prefix for generating upto 10000 shards of data when each
    // contains upto max_entries_per_file entries.
    // The file name will be concatenated with "-%04d.lst" suffix for each shard.
    uint32 max_entries_per_file = 0;

    // Whether to append to the existing file or otherwrite it.
    bool append = false;

    Options() {}
  };

  ListProtoWriter(StringPiece filename, const ::google::protobuf::Descriptor* dscr,
                  const Options& opts = Options());
  ~ListProtoWriter();

  const ListWriter* writer() const { return writer_.get();}

  util::Status Add(const ::google::protobuf::MessageLite& msg);

  util::Status Flush();

 private:
  void CreateWriter(StringPiece name);

  std::string base_name_;
  bool was_init_ = false;
  uint32 entries_per_shard_ = 0;
  uint32 shard_index_ = 0;

  std::unique_ptr<ListWriter> writer_;
  Options options_;
};

std::string GenerateSerializedFdSet(const ::google::protobuf::Descriptor* dscr);

}  // namespace file

#endif  // _PROTO_WRITER_H
