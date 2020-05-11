// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <openssl/hmac.h>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/future.hpp>

#include "absl/strings/str_cat.h"
#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "strings/escaping.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/aws/aws.h"
#include "util/aws/s3.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

DEFINE_string(prefix, "", "In form of 'bucket/someprefix' without s3:// part");
DEFINE_string(region, "us-east-1", "");
DEFINE_string(write_file, "", "bucket/someobj without 's3://' part");

DEFINE_bool(get, false, "");
DEFINE_bool(list_recursive, false, "If true, will recursively list all objects in the bucket");

/***
 *  We do not need SSL for working with s3, connecting to port 80 also works: s3cmd  --debug --no-ssl ls
 *  We should be able to retry with correct region per bucket operation.
 *
 * <Error><Code>AuthorizationHeaderMalformed</Code>
   <Message>The authorization header is malformed; the region 'eu-west-1' is wrong; expecting 'us-east-1'
   </Message>
   <Region>us-east-1</Region>
   <RequestId>9AB4D15F1C4F2F8E</RequestId>
   <HostId>n7h1hPY8qs7a40qT1QjWbydm/CE3r9Jqb4rRNUAkVZVkXQezqmNOBvpzwxMMnm7NRZXkEGBT6sg=</HostId>
   </Error>
 *
 **/

// TODO: those are used in gcs_utils as well. CreateSslContext is used in gce.
using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

const char kRootDomain[] = "s3.amazonaws.com";

void ListObjects(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  size_t pos = FLAGS_prefix.find('/');
  CHECK_NE(string::npos, pos);
  string bucket = FLAGS_prefix.substr(0, pos);
  string prefix = FLAGS_prefix.substr(pos + 1);
  LOG(INFO) << "Listing bucket " << bucket << ", prefix " << prefix;

  string domain = absl::StrCat(bucket, ".", kRootDomain);

  http::HttpsClientPool pool{domain, ssl_cntx, io_context};
  pool.set_connect_timeout(2000);

  S3Bucket s3bucket(*aws, &pool);
  auto cb = [](size_t sz, absl::string_view name) {
    cout << name << ":" << sz << endl;
  };

  S3Bucket::ListObjectResult result = s3bucket.List(prefix, !FLAGS_list_recursive, cb);
  CHECK_STATUS(result);
}

void Get(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  size_t pos = FLAGS_prefix.find('/');
  CHECK_NE(string::npos, pos);
  string bucket = FLAGS_prefix.substr(0, pos);

  string domain = absl::StrCat(bucket, ".", kRootDomain);
  string key = FLAGS_prefix.substr(pos + 1);

  http::HttpsClientPool pool{domain, ssl_cntx, io_context};
  pool.set_connect_timeout(2000);

  StatusObject<file::ReadonlyFile*> res = OpenS3ReadFile(key, *aws, &pool);
  CHECK_STATUS(res.status);

  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  std::unique_ptr<file::ReadonlyFile> file{res.obj};

  size_t ofs = 0;
  strings::MutableByteRange mbr(buf.get(), kBufSize);
  while (true) {
    auto res = file->Read(ofs, mbr);
    CHECK_STATUS(res.status);
    ofs += res.obj;
    if (res.obj < mbr.size()) {
      break;
    }
  }

  CHECK_EQ(0, file->Read(ofs, mbr).obj);
  file->Close();
  LOG(INFO) << "Read " << ofs << " bytes from " << key;
}

void WriteFile(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  size_t pos = FLAGS_write_file.find('/');
  CHECK_NE(string::npos, pos);
  string bucket = FLAGS_write_file.substr(0, pos);

  string domain = absl::StrCat(bucket, ".", kRootDomain);
  string key = FLAGS_write_file.substr(pos + 1);

  http::HttpsClientPool pool{domain, ssl_cntx, io_context};
  pool.set_connect_timeout(2000);

  StatusObject<file::WriteFile*> res = OpenS3WriteFile(key, *aws, &pool);
  CHECK_STATUS(res.status);
  unique_ptr<file::WriteFile> file(res.obj);

  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  memset(buf.get(), 'a', kBufSize);
  CHECK_STATUS(file->Write(absl::string_view(reinterpret_cast<char*>(buf.get()), kBufSize)));
  file->Close();
}

void ListBuckets(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  http::HttpsClientPool pool{kRootDomain, ssl_cntx, io_context};
  pool.set_connect_timeout(2000);

  ListS3BucketResult list_res = ListS3Buckets(*aws, &pool);
  CHECK_STATUS(list_res.status);

  for (const auto& b : list_res.obj) {
    cout << b << endl;
  }
};

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  asio::ssl::context ssl_cntx = AWS::CheckedSslContext();

  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  AWS aws{FLAGS_region, "s3"};

  CHECK_STATUS(aws.Init());

  if (!FLAGS_write_file.empty()) {
    io_context.AwaitSafe([&] { WriteFile(&ssl_cntx, &aws, &io_context); });
  } else if (FLAGS_prefix.empty()) {
    io_context.AwaitSafe([&] { ListBuckets(&ssl_cntx, &aws, &io_context); });
  } else {
    if (FLAGS_get) {
      io_context.AwaitSafe([&] { Get(&ssl_cntx, &aws, &io_context); });
    } else {
      io_context.AwaitSafe([&] { ListObjects(&ssl_cntx, &aws, &io_context); });
    }
  }
  return 0;
}
