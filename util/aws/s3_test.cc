// Copyright 2020, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/aws/s3.h"

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/aws/aws.h"

using namespace std;
using namespace testing;

namespace util {

class S3Test : public testing::Test {
 protected:
  void SetUp() override {
  }

  void TearDown() {
  }

  static void SetUpTestCase() {
  }
};

const char* kListBucketRes =
    R"(<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult
	xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Owner>
		<ID>abcdef</ID>
		<DisplayName>Jessie</DisplayName>
	</Owner>
	<Buckets>
		<Bucket>
			<Name>b1</Name>
			<CreationDate>2020-03-17T14:27:07.000Z</CreationDate>
		</Bucket>
		<Bucket>
			<Name>b2</Name>
			<CreationDate>2020-03-17T14:41:59.000Z</CreationDate>
		</Bucket>
		<Bucket>
			<Name>b3</Name>
			<CreationDate>2020-03-21T10:25:27.000Z</CreationDate>
		</Bucket>
		<Bucket>
			<Name>b4</Name>
			<CreationDate>2020-04-02T04:35:33.000Z</CreationDate>
		</Bucket>
	</Buckets>
</ListAllMyBucketsResult>
)";

const char* kListObjRes =
    R"(<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult
	xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>bucketname</Name>
	<Prefix></Prefix>
	<Marker></Marker>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>a1</Key>
		<LastModified>2020-05-03T20:50:05.000Z</LastModified>
		<ETag>&quot;57efef06e2ac7c701d0acc2e544a48eb&quot;</ETag>
		<Size>183600</Size>
		<Owner>
			<ID>abcdef</ID>
			<DisplayName>Jessie</DisplayName>
		</Owner>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
	<Contents>
		<Key>a2</Key>
		<LastModified>2020-05-03T20:50:06.000Z</LastModified>
		<ETag>&quot;7b6c64f45d6ae8450facb3d2ebd4bf6d&quot;</ETag>
		<Size>13611950</Size>
		<Owner>
			<ID>abcdef</ID>
			<DisplayName>Jessie</DisplayName>
		</Owner>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
	<Contents>
		<Key>a3</Key>
		<LastModified>2020-05-07T15:07:15.000Z</LastModified>
		<ETag>&quot;4310b84639450284015cfcbe68870d37-2&quot;</ETag>
		<Size>26171024</Size>
		<Owner>
			<ID>abcdef</ID>
			<DisplayName>Jessie</DisplayName>
		</Owner>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
</ListBucketResult>
)";

TEST_F(S3Test, ParseBucketListResp) {
  auto res = detail::ParseXmlListBuckets(kListBucketRes);
  EXPECT_THAT(res, ElementsAre("b1", "b2", "b3", "b4"));
}

TEST_F(S3Test, ParseListObjResp) {
  vector<pair<size_t, string>> res;

  detail::ParseXmlListObj(
      kListObjRes, [&](size_t sz, absl::string_view name) { res.emplace_back(sz, string(name)); });

  EXPECT_THAT(res, ElementsAre(Pair(183600, "a1"), Pair(13611950, "a2"), Pair(26171024, "a3")));
}

TEST_F(S3Test, Sha256) {
	char buf[65];
	detail::Sha256String(absl::string_view{}, buf);

	EXPECT_STREQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", buf);
}


void BM_Sha256(benchmark::State& state) {
	string str(1024, 'a');
	char buf[65];

  while (state.KeepRunning()) {
		detail::Sha256String(str, buf);
  }
}
BENCHMARK(BM_Sha256);

}  // namespace util
