// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <unordered_set>
#include <vector>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include "file/proto_writer.h"

#include "file/list_file.h"
#include "file/filesource.h"
#include "strings/stringprintf.h"

using util::Status;
using std::string;

namespace file {

const char kProtoSetKey[] = "__proto_set__";
const char kProtoTypeKey[] = "__proto_type__";

namespace gpb = ::google::protobuf;

inline list_file::CompressMethod CompressType(ProtoWriter::Options::CompressMethod m) {
  switch(m) {
    case ProtoWriter::Options::LZ4_COMPRESS: return list_file::kCompressionLZ4;
    default: ;
  }
  return list_file::kCompressionZlib;
}

inline static string GetOutputFileName(string base, unsigned index) {
  base += StringPrintf("-%04d.lst", index);
  return base;
}


ProtoWriter::ProtoWriter(StringPiece filename, const gpb::Descriptor* dscr, Options opts)
    : dscr_(dscr), options_(opts) {
  const gpb::FileDescriptor* fd = dscr->file();
  gpb::FileDescriptorSet fd_set;
  std::unordered_set<const gpb::FileDescriptor*> unique_set({fd});
  std::vector<const gpb::FileDescriptor*> stack({fd});
  while (!stack.empty()) {
    fd = stack.back();
    stack.pop_back();
    fd->CopyTo(fd_set.add_file());

    for (int i = 0; i < fd->dependency_count(); ++i) {
      const gpb::FileDescriptor* child = fd->dependency(i);
      if (unique_set.insert(child).second) {
        stack.push_back(child);
      }
    }
  }
  fd_set_str_ = fd_set.SerializeAsString();
  if (opts.format == LIST_FILE) {
    ListWriter::Options opts;
    opts.block_size_multiplier = 4;
    opts.compress_method = CompressType(options_.compress_method);
    opts.compress_level = options_.compress_level;
    opts.append = options_.append;

    string file_name_buf;
    if (options_.max_entries_per_file > 0) {
      base_name_ = filename.as_string();
      file_name_buf = GetOutputFileName(base_name_, 0);
      filename = file_name_buf;
    }
    writer_.reset(new ListWriter(filename, opts));
    writer_->AddMeta(kProtoSetKey, fd_set_str_);
    writer_->AddMeta(kProtoTypeKey, dscr->full_name());
  } else {
    LOG(FATAL) << "Invalid format " << opts.format;
  }
}

ProtoWriter::~ProtoWriter() {
  if (writer_) writer_->Flush();
}

util::Status ProtoWriter::Add(const ::google::protobuf::MessageLite& msg) {
  CHECK(writer_);
  CHECK_EQ(dscr_->full_name(), msg.GetTypeName());
  if (!was_init_) {
    RETURN_IF_ERROR(writer_->Init());
    was_init_ = true;
  }

  if (options_.max_entries_per_file > 0 &&  ++entries_per_shard_ > options_.max_entries_per_file) {
    RETURN_IF_ERROR(writer_->Flush());

    entries_per_shard_ = 0;
    writer_.reset(new ListWriter(GetOutputFileName(base_name_, ++shard_index_)));
    writer_->AddMeta(kProtoSetKey, fd_set_str_);
    writer_->AddMeta(kProtoTypeKey, dscr_->full_name());
    RETURN_IF_ERROR(writer_->Init());
  }
  return writer_->AddRecord(msg.SerializeAsString());
}

util::Status ProtoWriter::Flush() {
  if (writer_) {
    if (!was_init_) {
      RETURN_IF_ERROR(writer_->Init());
      was_init_ = true;
    }
    return writer_->Flush();
  }
  return Status::OK;
}

}  // namespace file
