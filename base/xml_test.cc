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
    CHECK(nullptr == cur->content);

    const char* ns_href = as_char(cur->ns->href);
    const char* ns_pref = as_char(cur->ns->prefix);
    const char* name = as_char(cur->name);

    xmlChar* content = nullptr;
    if (cur->children && cur->last == cur->children && cur->children->type == XML_TEXT_NODE) {
      CHECK(cur->children->content);
      content = cur->children->content;
    }
    LOG(INFO) << absl::StrFormat("= element node \"%s:%s:%s:%s\"", ns_pref, ns_href, name,
                                 as_char(content));
    // xmlFree(content);
    LOG(INFO) << absl::StrFormat("Children %p last %p parent %p", cur->children, cur->last,
                                 cur->parent);
  }
}

constexpr char kContent[] =
    R"(<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>booah</Name><Prefix>qdata/racoon.jpg</Prefix><Marker></Marker><MaxKeys>1000</MaxKeys>
<IsTruncated>false</IsTruncated><Contents>
<Key>qdata/racoon.jpg</Key>
<LastModified>2020-01-24T08:15:21.000Z</LastModified>
<ETag>&quot;f70ce61188d49a9d28d6495622b13a21&quot;</ETag><Size>71112</Size>
<Owner><ID>0deadbeef</ID>
<DisplayName>roman</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents>
<Contents>
		<Key>qdata/racoon.jpg2</Key>
		<LastModified>2020-02-10T21:23:04.000Z</LastModified>
		<ETag>&quot;5a5db17408ca2ecd6c2ab5e91195bd5d-16&quot;</ETag>
		<Size>1067915050</Size>
		<Owner>
			<ID>0deadbeef</ID>
			<DisplayName>roman</DisplayName>
		</Owner>
		<StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>

  )";

TEST(XmlTest, XPath) {
  xmlDocPtr doc = xmlReadMemory(kContent, sizeof(kContent), NULL, NULL, 0);
  CHECK(doc);

  xmlXPathContextPtr xpathCtx = xmlXPathNewContext(doc);

  auto res = xmlXPathRegisterNs(xpathCtx, BAD_CAST "NS",
                                BAD_CAST "http://s3.amazonaws.com/doc/2006-03-01/");
  CHECK_EQ(res, 0);

  xmlXPathObjectPtr xpathObj = xmlXPathEvalExpression(BAD_CAST "/NS:ListBucketResult//*", xpathCtx);
  CHECK(xpathObj);

  /* Print results. TODO: to extract to array of objects */
  print_xpath_nodes(xpathObj->nodesetval);

  xmlXPathFreeObject(xpathObj);
  xmlXPathFreeContext(xpathCtx);
  xmlFreeDoc(doc);
}

}  // namespace base
