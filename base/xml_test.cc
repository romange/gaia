// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "absl/strings/str_format.h"
#include "base/gtest.h"
#include "base/logging.h"

#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

namespace base {

class XmlTest {
 protected:
  static void TearDownTestCase() {
    xmlCleanupParser();
  }
};

inline const char* as_char(const xmlChar* var) {
  return reinterpret_cast<const char*>(var);
}

void print_xpath_nodes(xmlNodeSetPtr nodes) {
  int size = (nodes) ? nodes->nodeNr : 0;

  LOG(INFO) << absl::StrFormat("Result (%d nodes):", size);
  for (int i = 0; i < size; ++i) {
    xmlNodePtr cur = nodes->nodeTab[i];
    CHECK_EQ(XML_ELEMENT_NODE, cur->type);
    CHECK(cur->ns);

    const char *s1 = reinterpret_cast<const char*>(cur->ns->href),
               *s2 = reinterpret_cast<const char*>(cur->name),
               *s3 = reinterpret_cast<const char*>(cur->ns->prefix);
    xmlChar* content = xmlNodeGetContent(cur);

    LOG(INFO) << absl::StrFormat("= element node \"%s:%s:%s:%s\"", s1, s3, s2, as_char(content));
    xmlFree(content);
  }
}


TEST(XmlTest, Basic) {
  constexpr char kContent[] =
      R"(<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>booah</Name><Prefix>qdata/racoon.jpg</Prefix><Marker></Marker><MaxKeys>1000</MaxKeys>
<IsTruncated>false</IsTruncated><Contents><Key>qdata/racoon.jpg</Key>
<LastModified>2020-01-24T08:15:21.000Z</LastModified>
<ETag>&quot;f70ce61188d49a9d28d6495622b13a21&quot;</ETag><Size>71112</Size>
<Owner><ID>0deadbeef</ID>
<DisplayName>roman</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>

  )";

  xmlDocPtr doc = xmlReadMemory(kContent, sizeof(kContent), NULL, NULL, 0);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST "/NS:ListBucketResult/*", xpathCtx);
  CHECK(xpathObj);

  /* Print results */
  print_xpath_nodes(xpathObj->nodesetval);

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);
}

}  // namespace base
