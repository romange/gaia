// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "absl/strings/str_format.h"
#include "base/gtest.h"
#include "base/logging.h"

#include <libxml/parser.h>
#include <libxml/xpath.h>

namespace base {

class XmlTest {
 protected:
  static void TearDownTestCase() {
    xmlCleanupParser();
  }
};

void print_xpath_nodes(xmlNodeSetPtr nodes, FILE* output) {
  xmlNodePtr cur;
  int i;

  int size = (nodes) ? nodes->nodeNr : 0;

  LOG(INFO) << absl::StrFormat("Result (%d nodes):", size);
  for (i = 0; i < size; ++i) {
    if (nodes->nodeTab[i]->type == XML_NAMESPACE_DECL) {
      xmlNsPtr ns;

      ns = (xmlNsPtr)nodes->nodeTab[i];
      cur = (xmlNodePtr)ns->next;
      if (cur->ns) {
        LOG(INFO) << absl::StrFormat("= namespace \"%s\"=\"%s\" for node %s:%s", ns->prefix,
                                     ns->href, cur->ns->href, cur->name);
      } else {
        LOG(INFO) << absl::StrFormat("= namespace \"%s\"=\"%s\" for node %s", ns->prefix, ns->href,
                                     cur->name);
      }
    } else if (nodes->nodeTab[i]->type == XML_ELEMENT_NODE) {
      cur = nodes->nodeTab[i];
      if (cur->ns) {
        const char* s1 = reinterpret_cast<const char*>(cur->ns->href),
                   *s2 = reinterpret_cast<const char*>(cur->name);

        LOG(INFO) << absl::StrFormat("= element node \"%s:%s\"", s1, s2);
      } else {
        LOG(INFO) << absl::StrFormat("= element node \"%s\"", cur->name);
      }
    } else {
      cur = nodes->nodeTab[i];
      LOG(INFO) << absl::StrFormat("= node \"%s\": type %d", cur->name, cur->type);
    }
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
  xmlXPathObjectPtr xpathObj =
      xmlXPathEvalExpression(reinterpret_cast<const unsigned char*>("//*"), xpathCtx);
  CHECK(xpathObj);

  /* Print results */
  print_xpath_nodes(xpathObj->nodesetval, stderr);

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);
}

}  // namespace base
