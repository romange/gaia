// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <fnmatch.h>
#include <libs3.h>
#include <mutex>
#include <unordered_map>

#include "base/logging.h"
#include "file/s3_file.h"

#include "file/file.h"
#include "file/file_util.h"
#include "strings/strcat.h"

namespace file {

using base::Status;
using base::StatusCode;
using strings::Slice;
using std::string;

namespace {

const char kS3NamespacePrefix[] = "s3://";

typedef std::unordered_map<string, S3BucketContext> BucketMap;
struct S3GlobalData;

std::once_flag s3data_once;
S3GlobalData* global_data = nullptr;

inline BucketMap& bucket_map() {
  static BucketMap internal_map;
  return internal_map;
}

inline string safe_cstr(const char* s) {
  return s ? s : "";
}

struct CallbackData {
  volatile S3Status status = S3StatusOK;

  // Read.
  volatile size_t length = 0;
  char* dest_buf = nullptr;
  FILE *infile = nullptr;

  // For S3_list_bucket;
  const S3BucketContext* bcontext = nullptr;
  std::vector<StatShort>* file_arr = nullptr;
  bool is_truncated = false;
  string marker;
};

class S3File : public ReadonlyFile {
  const S3BucketContext* context_ = nullptr;
  std::string key_;
  size_t file_size_ = 0;
public:
  S3File(size_t file_size, StringPiece key, const S3BucketContext* context)
      : context_(context), key_(key.as_string()), file_size_(file_size) {}

  Status Close() override { return Status::OK;}

  // Reads data and returns it in OUTPUT. Set total count of bytes read  into read_size.
  virtual Status Read(size_t offset, size_t length, Slice* result,
                      uint8* buffer) override;

  size_t Size() const override { return file_size_; }

  static S3Status PropertiesCallback(const S3ResponseProperties* properties, void* callbackData) {
    VLOG(1) << "PropertiesCallback " << properties->contentType << " "
            << properties->contentLength << " " << properties->metaDataCount;
    CallbackData* data = reinterpret_cast<CallbackData*>(callbackData);
    data->length = properties->contentLength;
    return S3StatusOK;
  }

  static void CompleteCallback(S3Status status, const S3ErrorDetails* errorDetails,
                               void* callbackData) {
    CallbackData* data = reinterpret_cast<CallbackData*>(callbackData);
    CHECK_NOTNULL(data);
    if (status != S3StatusOK) {
      /*string msg;
      if (errorDetails) {
        msg = safe_cstr(errorDetails->message) + ", more: "  +
          safe_cstr(errorDetails->furtherDetails);
      }*/
      LOG(WARNING) << "Status : " << S3_get_status_name(status);
    }
    data->status = status;
  }

  static S3Status DataCallback(int bufferSize, const char *buffer,
                               void* callbackData);
protected:
  ~S3File() {}
};

struct S3GlobalData {
  string access_key;
  string secret_key;
};

void InitS3Data() {
  CHECK(global_data == nullptr);
  CHECK_EQ(S3StatusOK, S3_initialize(NULL, S3_INIT_ALL, NULL));
  const char* sk = getenv("AWS_SECRET_KEY");
  if (sk) {
    global_data = new S3GlobalData();
    const char* ak = CHECK_NOTNULL(getenv("AWS_ACCESS_KEY"));
    global_data->access_key.assign(ak);
    global_data->secret_key.assign(sk);
  } else {
    // Initialize from IAM tokens of the server.
    CHECK_EQ(S3StatusOK, S3_init_iam_role());
    LOG(INFO) << "Initializing iam tokens";
  }
}

inline S3BucketContext NewBucket() {
  const char* a = global_data ? global_data->access_key.c_str() : nullptr;
  const char* s = global_data ? global_data->secret_key.c_str() : nullptr;
  return S3BucketContext{nullptr, nullptr, S3ProtocolHTTP, S3UriStylePath,
                         a, s};
}

// Returns suffix after the bucket name and S3BucketContext.
std::pair<StringPiece, const S3BucketContext*> GetKeyAndBucket(StringPiece name) {
  DCHECK(name.starts_with(kS3NamespacePrefix)) << name;
  std::call_once(s3data_once, InitS3Data);

  StringPiece tmp = name;
  tmp.remove_prefix(sizeof(kS3NamespacePrefix) - 1);
  size_t pos = tmp.find('/');
  CHECK_NE(StringPiece::npos, pos) << "Invalid filename " << name;

  string bucket_str = tmp.substr(0, pos).as_string();
  BucketMap& bmap = bucket_map();
  auto res = bmap.emplace(bucket_str, NewBucket());
  if (res.second) {
    res.first->second.bucketName = res.first->first.c_str();
  }
  tmp.remove_prefix(pos + 1);
  return std::pair<StringPiece, const S3BucketContext*>(tmp, &res.first->second);
}

Status S3File::Read(size_t offset, size_t length, Slice* result,
                    uint8* buffer) {
  if (offset > file_size_ || length == 0) {
    *result = Slice();
    return Status(StatusCode::RUNTIME_ERROR, "Invalid read range");
  }
  if (offset + length > file_size_) {
    length = file_size_ - offset;
  }
  VLOG(1) << "S3File::Read " << offset << " " << length;

  S3GetObjectHandler handler{{nullptr, S3File::CompleteCallback},
                             S3File::DataCallback};
  CallbackData data;
  data.dest_buf = reinterpret_cast<char*>(buffer);
  size_t try_length = length;
  size_t read_length = 0;

  for (unsigned attempts = 0; attempts < 5; ++attempts) {
    data.status = S3StatusOK;
    data.length = 0;
    S3_get_object(context_, key_.c_str(), nullptr, offset, try_length, nullptr, &handler, &data);
    read_length += data.length;

    if (data.status == S3StatusOK) {
      result->set(reinterpret_cast<char*>(buffer), read_length);
  return Status::OK;
    }
    LOG(WARNING) << "Attempt " << attempts << " failed: " << data.status << ", " << data.length;

    if (data.length >= length) {
      return Status(StatusCode::INTERNAL_ERROR, "Data length is too large");
    }
    offset += data.length;
    try_length -= data.length;
    LOG(WARNING) << "Attempt " << attempts << " failed";
  }
  return Status(StatusCode::IO_ERROR, S3_get_status_name(data.status));
}

S3Status S3File::DataCallback(int bufferSize, const char *buffer, void* callbackData) {
  VLOG(2) << "S3 Read " << bufferSize << " bytes";
  CallbackData* data = reinterpret_cast<CallbackData*>(callbackData);
  memcpy(data->dest_buf, buffer, bufferSize);
  data->dest_buf += bufferSize;
  data->length += bufferSize;
  return S3StatusOK;
}


int PutObjectDataCallback(int buffer_size, char *buffer, void *callbackData) {
  CallbackData *data = static_cast<CallbackData*>(callbackData);
  int ret = 0;
  if (data->length) {
    int to_read = std::min<size_t>(size_t(data->length), buffer_size);
    ret = fread(buffer, 1, to_read, data->infile);
  }
  data->length -= ret;
  return ret;
}

}  // namespace

base::StatusObject<ReadonlyFile*> OpenS3File(StringPiece name) {
  auto res = GetKeyAndBucket(name);
  const S3BucketContext* context = res.second;
  CHECK(!res.first.empty()) << "Missing file name after the bucket";
  StringPiece key = res.first;

  S3ResponseHandler handler{S3File::PropertiesCallback, S3File::CompleteCallback};
  CallbackData data;

  for (unsigned attempts = 0; attempts < 5; ++attempts) {
    S3_head_object(context, key.data(), nullptr, &handler, &data);
    if (data.status == S3StatusOK) {

  return new S3File(data.length, key, context);
}
    LOG(WARNING) << "Failed to open file from s3 " << name << ". Attempt " << attempts << " failed";
  }
  return Status(StatusCode::IO_ERROR, S3_get_status_name(data.status));
}

bool IsInS3Namespace(StringPiece name) {
  return name.starts_with(kS3NamespacePrefix);
}

bool ExistsS3File(StringPiece name) {
  S3ResponseHandler handler{nullptr, S3File::CompleteCallback};
  CallbackData data;
  auto res = GetKeyAndBucket(name);
  for (unsigned attempts = 0; attempts < 5; ++attempts) {
  S3_head_object(res.second, res.first.data(), nullptr, &handler, &data);
    if (data.status == S3StatusOK) {
      return true;
    } else if (data.status == S3StatusHttpErrorNotFound) {
      return false;
    }
    LOG(WARNING) << "Failed to find if s3 file exists " << name << ". Attempt "
                 << attempts << " failed";
  }
  return (data.status == S3StatusOK);
}


Status CopyFileToS3(StringPiece file_path, StringPiece s3_dir) {
  StringPiece file_name = file_util::GetNameFromPath(file_path);
  if (file_name.empty()) {
    return Status(StrCat("Invalid path: ", file_path));
  }

  string s3_path = file_util::JoinPath(s3_dir, file_name);
  auto res = GetKeyAndBucket(s3_path);
  int64_t file_length = file_util::LocalFileSize(file_path);
  if (file_length == -1) {
    return Status(StrCat("Failed to fetch file size: ", file_path));
  }

  CallbackData data;
  data.length = file_length;

  if (!(data.infile = fopen(file_path.data(), "r"))) {
    return Status(StrCat("Failed to open source file: ", file_path));
  }

  S3ResponseHandler response_handler{S3File::PropertiesCallback, S3File::CompleteCallback};
  S3PutObjectHandler obj_handler = { response_handler, &PutObjectDataCallback };

  for (unsigned attempts = 0; attempts < 5; ++attempts) {
  S3_put_object(res.second, res.first.data(), file_length, nullptr, nullptr,
                &obj_handler, &data);
    if (data.status == S3StatusOK) {
      break;
    }
    LOG(WARNING) << "Failed to put file to s3 " << file_name << ". Attempt " << attempts << " failed";
  }

  CHECK(data.infile != 0);
  if (fclose(data.infile) != 0) {
    LOG(WARNING) << "Failed to close file: " << file_path;
  }
  if (data.status != S3StatusOK) {
    return Status(StatusCode::IO_ERROR, S3_get_status_name(data.status));
  }
  return Status::OK;
}


static S3Status MyListFileCb(int isTruncated, const char *nextMarker,
                             int contentsCount, const S3ListBucketContent *contents,
                             int commonPrefixesCount, const char **commonPrefixes,
                             void *callbackData) {
  VLOG(2) << "MyListFileCb: " << isTruncated << " " << contentsCount << " " << nextMarker
          << " " << commonPrefixesCount;

  CallbackData* data = static_cast<CallbackData*>(callbackData);

  data->is_truncated = isTruncated;
  if (isTruncated) {
    data->marker.assign(nextMarker);
  }

  for (int i = 0; i < contentsCount; ++i) {
    string tmp = StrCat(kS3NamespacePrefix, data->bcontext->bucketName, "/", contents[i].key);
    VLOG(2) << "Key: " << tmp << " "
            << contents[i].size;
    if (tmp.back() != '/') {
      data->file_arr->push_back(
        StatShort{std::move(tmp), contents[i].lastModified, off_t(contents[i].size) });
    }
  }

  for (int i = 0; i < commonPrefixesCount; ++i) {
    VLOG(3) << "commonPrefixes: " << commonPrefixes[i];
  }
  return S3StatusOK;
}

static Status ListFilesInternal(StringPiece path, unsigned max_items, std::vector<StatShort>* file_names) {
  if (!IsInS3Namespace(path)) {
    return Status("Non-S3 path");
  }

  auto res = GetKeyAndBucket(path);
  const S3BucketContext* context = res.second;

  CallbackData callback_data;
  callback_data.file_arr = file_names;
  callback_data.bcontext = context;

  S3ResponseHandler response_handler{S3File::PropertiesCallback, S3File::CompleteCallback};
  S3ListBucketHandler handler{response_handler, &MyListFileCb};

  string prefix = res.first.as_string();

  VLOG(1) << prefix << " " << max_items;
  const char* marker = nullptr;
  do {
    for (unsigned attempts = 0; attempts < 5; ++attempts) {
    S3_list_bucket(context, prefix.c_str(), marker, "/",
                   max_items, (S3RequestContext*)nullptr, &handler, &callback_data);
      if (callback_data.status == S3StatusOK) {
        break;
      }
      LOG(WARNING) << "Failed to list s3 bucket " << prefix << ". Attempt " << attempts << " failed";
    }
    if (callback_data.status != S3StatusOK) {
      return Status(StatusCode::IO_ERROR, S3_get_status_name(callback_data.status));
    }
    marker = callback_data.marker.c_str();
  } while (callback_data.is_truncated);

  return Status::OK;
}

Status ListFiles(StringPiece path, unsigned max_items, std::vector<StatShort>* file_names) {
  auto pos = path.find('*', 0);
  if (pos != std::string::npos) {  // If path contains '*'.
    StringPiece pattern = StringPiece(path, pos, path.length());
    StringPiece path_without_star = StringPiece(path, 0, pos);
    RETURN_IF_ERROR(ListFilesInternal(path_without_star, max_items, file_names));
    // Remove files that do not match pattern.
    auto out_it = file_names->begin();
    auto pattern_str = pattern.as_string().c_str();
    for (auto inp_it = file_names->begin(); inp_it != file_names->end();) {
      if (0 != fnmatch(pattern_str, inp_it->name.c_str(), FNM_EXTMATCH)) {
        ++inp_it;
      } else {
        *out_it++ = *inp_it++;
      }
    }
    auto new_length = out_it - file_names->begin();
    file_names->resize(new_length);
  } else {
    RETURN_IF_ERROR(ListFilesInternal(path, max_items, file_names));
  }
  return Status::OK;
}


base::Status ListFiles(StringPiece path, unsigned max_items, std::vector<std::string>* file_names) {
  std::vector<StatShort> res;
  RETURN_IF_ERROR(ListFiles(path, max_items, &res));
  for (auto& val : res) {
    file_names->push_back(std::move(val.name));
  }
  return Status::OK;
}

}  // namespace file
