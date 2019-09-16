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

/*! @brief Opens read-only, GCS-backed file.
 *
 * Single threaded and fiber-friendly. Must be called within IoContext thread sponsoring
 * HttpsClientPool. Only sequential files are currently supported.
 *
 */
StatusObject<file::ReadonlyFile*> OpenGcsReadFile(
    absl::string_view full_path, const GCE& gce, http::HttpsClientPool* pool,
    const file::ReadonlyFile::Options& opts = file::ReadonlyFile::Options{});

}  // namespace util
