// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "strings/escaping.h"

#include "util/gce/detail/gcs_utils.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

using namespace boost;
using namespace http;
using namespace ::std;
namespace h2 = detail::h2;
using file::WriteFile;

namespace {

class GcsWriteFile : public WriteFile {
 public:
  GcsWriteFile(absl::string_view name, const GCE& gce, HttpsClientPool* pool)
      : WriteFile(name), gce_(gce), pool_(pool) {}

  bool Close() final;

  bool Open() final;

 private:
  const GCE& gce_;
  HttpsClientPool* pool_;
  const string obj_url_;
};

bool GcsWriteFile::Close() {
  return true;
}

bool GcsWriteFile::Open() {
  return true;
}

}  // namespace

}  // namespace util
