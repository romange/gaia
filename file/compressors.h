// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "util/status.h"
#include "file/list_file_format.h"
#include <functional>

namespace file {

typedef std::function<util::Status(const void* src, size_t len, void* dest,
                                   size_t* uncompress_size)> UncompressFunction;

typedef std::function<util::Status(int level, const void* src, size_t len, void* dest,
                                   size_t* compress_size)> CompressFunction;
typedef std::function<size_t(size_t len)> CompressBoundFunction;


UncompressFunction GetUncompress(list_file::CompressMethod m);
CompressFunction GetCompress(list_file::CompressMethod m);

CompressBoundFunction GetCompressBound(list_file::CompressMethod method);

}  // namespace file
