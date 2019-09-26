// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "file/file.h"

namespace util {

namespace http {

class HttpsClientPool;

}  // namespace http

class GCE;

/**
 * @brief Opens read-only, GCS-backed file.
 *
 * It's single threaded and fiber-friendly. Must be called within IoContext thread sponsoring
 * HttpsClientPool. Only sequential read files are currently supported.
 *
 * @param full_path - aka "gs://my_bucket/path/obj_name"
 * @param gce
 * @param pool
 * @param opts
 * @return StatusObject<file::ReadonlyFile*>
 */
StatusObject<file::ReadonlyFile*> OpenGcsReadFile(
    absl::string_view full_path, const GCE& gce, http::HttpsClientPool* pool,
    const file::ReadonlyFile::Options& opts = file::ReadonlyFile::Options{});

}  // namespace util
