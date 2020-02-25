// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "base/logging.h"
#include "util/aws/aws.h"
#include "util/aws/s3.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

namespace util {

// TODO: the same like in gcs_utils.h
inline Status ToStatus(const ::boost::system::error_code& ec) {
  return ec ? Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()))
            : Status::OK;
}

inline const char* as_char(const xmlChar* var) {
  return reinterpret_cast<const char*>(var);
}

using http::HttpsClientPool;
using namespace boost;
namespace h2 = beast::http;

constexpr char kS3Domain[] = "s3.amazonaws.com";

S3::S3(const AWS& aws, http::HttpsClientPool* pool) : aws_(aws), pool_(pool) {
}

auto S3::ListBuckets() -> ListBucketResult {
  HttpsClientPool::ClientHandle handle = pool_->GetHandle();

  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  h2::response<h2::string_body> resp;

  aws_.Sign(kS3Domain, &req);

  system::error_code ec = handle->Send(req, &resp);

  if (ec) {
    return ToStatus(ec);
  }

  std::vector<std::string> res;

  xmlDocPtr doc = xmlReadMemory(resp.body().data(), resp.body().size(), NULL, NULL, 0);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto register_res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                         BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(register_res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(
      BAD_CAST "/NS:ListAllMyBucketsResult/NS:Buckets/NS:Bucket/NS:Name", xpathCtx);
  CHECK(xpathObj);
  xmlNodeSetPtr nodes = xpathObj->nodesetval;
  if (nodes) {
    int size = nodes->nodeNr;
    for (int i = 0; i < size; ++i) {
      xmlNodePtr cur = nodes->nodeTab[i];
      CHECK_EQ(XML_ELEMENT_NODE, cur->type);
      CHECK(cur->ns);
      CHECK(nullptr == cur->content);

      if (cur->children && cur->last == cur->children && cur->children->type == XML_TEXT_NODE) {
        CHECK(cur->children->content);
        res.push_back(as_char(cur->children->content));
      }
    }
  }

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);

  return res;
}

}  // namespace util
