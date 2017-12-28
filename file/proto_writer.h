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

class ProtoWriter {
  std::unique_ptr<ListWriter> writer_;
  
  const ::google::protobuf::Descriptor* dscr_;
  base::Arena arena_;

  bool was_init_ = false;
  uint32 entries_per_shard_ = 0;
  uint32 shard_index_ = 0;
  std::string base_name_, fd_set_str_;
public:
  enum Format {LIST_FILE};

  struct Options {
    Format format;

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

    Options() : format(LIST_FILE) {}
  };

  ProtoWriter(StringPiece filename, const ::google::protobuf::Descriptor* dscr,
              Options opts = Options());

  ~ProtoWriter();

  util::Status Add(const ::google::protobuf::MessageLite& msg);

  util::Status Flush();

  const ListWriter* writer() const { return writer_.get();}

private:
  Options options_;
};


}  // namespace file

#endif  // _PROTO_WRITER_H
