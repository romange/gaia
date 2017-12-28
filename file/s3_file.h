// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Internal - should not be used directly.
#ifndef _S3_FILE_H
#define _S3_FILE_H

#include "strings/stringpiece.h"
#include "base/status.h"

namespace file {
class ReadonlyFile;

base::StatusObject<ReadonlyFile*> OpenS3File(StringPiece name);
bool ExistsS3File(StringPiece name);

// Save local file to S3. This overwrites any existing object at that key.
base::Status CopyFileToS3(StringPiece file_path, StringPiece s3_dir);


struct StatShort;

base::Status ListFiles(StringPiece path, unsigned max_items, std::vector<std::string>* file_names);
base::Status ListFiles(StringPiece path, unsigned max_items, std::vector<StatShort>* file_names);

}  // namespace file

#endif  // _S3_FILE_H