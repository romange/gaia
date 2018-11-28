// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/proto_writer.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <unordered_set>
#include <vector>

#include "file/filesource.h"
#include "file/list_file.h"
#include "strings/stringprintf.h"

using std::string;
using util::Status;

namespace file {

const char kProtoSetKey[] = "__proto_set__";
const char kProtoTypeKey[] = "__proto_type__";

namespace gpb = ::google::protobuf;

namespace {

inline list_file::CompressMethod CompressType(ListProtoWriter::Options::CompressMethod m) {
  switch (m) {
    case ListProtoWriter::Options::LZ4_COMPRESS:
      return list_file::kCompressionLZ4;
    default:;
  }
  return list_file::kCompressionZlib;
}

inline static string GetOutputFileName(string base, unsigned index) {
  base += StringPrintf("-%04d.lst", index);
  return base;
}

string GenerateSerializedFdSet(const gpb::Descriptor* dscr) {
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
  return fd_set.SerializeAsString();
}

ListWriter::Options GetListOptions(const ListProtoWriter::Options& src) {
  ListWriter::Options res;
  res.block_size_multiplier = 4;
  res.compress_method = CompressType(src.compress_method);
  res.compress_level = src.compress_level;
  res.append = src.append;
  return res;
}

}  // namespace

BaseProtoWriter::BaseProtoWriter(const gpb::Descriptor* dscr)
    : dscr_(dscr) {
  fd_set_str_ = GenerateSerializedFdSet(dscr);
}

ListProtoWriter::ListProtoWriter(StringPiece filename, const ::google::protobuf::Descriptor* dscr,
                                 const Options& options)
    : BaseProtoWriter(dscr), options_(options) {
  string file_name_buf;
  if (options_.max_entries_per_file > 0) {
    base_name_ = strings::AsString(filename);
    file_name_buf = GetOutputFileName(base_name_, 0);
    filename = file_name_buf;
  }
  CreateWriter(filename);
}

ListProtoWriter::~ListProtoWriter() {
  if (writer_)
    writer_->Flush();
}

util::Status ListProtoWriter::Add(const ::google::protobuf::MessageLite& msg) {
  CHECK(writer_);
  CHECK_EQ(dscr_->full_name(), msg.GetTypeName());
  if (!was_init_) {
    RETURN_IF_ERROR(writer_->Init());
    was_init_ = true;
  }

  if (options_.max_entries_per_file > 0 && ++entries_per_shard_ > options_.max_entries_per_file) {
    RETURN_IF_ERROR(writer_->Flush());

    entries_per_shard_ = 0;
    CreateWriter(GetOutputFileName(base_name_, ++shard_index_));
    RETURN_IF_ERROR(writer_->Init());
  }
  return writer_->AddRecord(msg.SerializeAsString());
}

util::Status ListProtoWriter::Flush() {
  if (writer_) {
    if (!was_init_) {
      RETURN_IF_ERROR(writer_->Init());
      was_init_ = true;
    }
    return writer_->Flush();
  }
  return Status::OK;
}

void ListProtoWriter::CreateWriter(StringPiece name) {
  ListWriter::Options opts = GetListOptions(options_);
  writer_.reset(new ListWriter(name, opts));
  writer_->AddMeta(kProtoSetKey, fd_set_str_);
  writer_->AddMeta(kProtoTypeKey, dscr_->full_name());
}

}  // namespace file
