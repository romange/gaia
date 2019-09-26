// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gcs.h"

#include <boost/beast/http/dynamic_body.hpp>
// #include <boost/beast/http/parser.hpp>

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

class GcsWriteFile : public WriteFile, detail::GcsFileBase {
 public:
  /**
   * @brief Construct a new Gcs Write File object.
   *
   * @param name - aka "gs://somebucket/path_to_obj"
   * @param gce - initialized GCE object for access token.
   * @param pool - https connection pool connected to google api server.
   */
  GcsWriteFile(absl::string_view name, const GCE& gce, HttpsClientPool* pool)
      : WriteFile(name), detail::GcsFileBase(gce, pool) {}

  bool Close() final;

  bool Open() final;

 private:
  string obj_url_;
};

bool GcsWriteFile::Close() { return true; }

bool GcsWriteFile::Open() { return true; }

}  // namespace

StatusObject<file::WriteFile*> OpenGcsWriteFile(absl::string_view full_path, const GCE& gce,
                                                http::HttpsClientPool* pool) {
  absl::string_view bucket, obj_path;
  CHECK(GCS::SplitToBucketPath(full_path, &bucket, &obj_path));

  string url = "/upload/storage/v1/b/";
  absl::StrAppend(&url, bucket, "/o?uploadType=resumable&name=");
  strings::AppendEncodedUrl(obj_path, &url);
  string token = gce.access_token();

  auto req = detail::PrepareGenericRequest(h2::verb::post, url, token);
  h2::response<h2::dynamic_body> resp_msg;
  req.prepare_payload();

  return nullptr;
  #if 0
  string upload_id;

  RETURN_IF_ERROR(SendWithToken(&req, &resp_msg));
    if (resp_msg.result() != h2::status::ok) {
      return HttpError(resp_msg);
    }
    auto it = resp_msg.find(h2::field::location);
    if (it == resp_msg.end()) {
      return Status(StatusCode::PARSE_ERROR, "Can not find location header");
    }
    upload_id = string(it->value());
  #endif
}

}  // namespace util
