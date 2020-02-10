// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
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

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

DEFINE_string(bucket, "", "");
DEFINE_string(read_path, "", "");
DEFINE_string(prefix, "", "");

using FileQ = fibers::buffered_channel<string>;

http::SslContextResult CreateSslContext() {
  system::error_code ec;
  asio::ssl::context cntx{asio::ssl::context::tlsv12_client};
  cntx.set_options(boost::asio::ssl::context::default_workarounds |
                   boost::asio::ssl::context::no_compression | boost::asio::ssl::context::no_sslv2 |
                   boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
                   boost::asio::ssl::context::no_tlsv1_1);
  cntx.set_verify_mode(asio::ssl::verify_peer, ec);
  if (ec) {
    return http::SslContextResult(ec);
  }
  // cntx.add_certificate_authority(asio::buffer(cert_string.data(), cert_string.size()), ec);
  cntx.load_verify_file("/etc/ssl/certs/ca-certificates.crt", ec);
  if (ec) {
    return http::SslContextResult(ec);
  }

#if 0
  SSL_CTX* ssl_cntx = cntx.native_handle();

  long flags = SSL_CTX_get_options(ssl_cntx);
  flags |= SSL_OP_CIPHER_SERVER_PREFERENCE;
  SSL_CTX_set_options(ssl_cntx, flags);

  constexpr char kCiphers[] = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256";
  CHECK_EQ(1, SSL_CTX_set_cipher_list(ssl_cntx, kCiphers));
  CHECK_EQ(1, SSL_CTX_set_ecdh_auto(ssl_cntx, 1));
#endif

  return http::SslContextResult(std::move(cntx));
}

void sha256_string(absl::string_view str, char out[65]) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, str.data(), str.size());
  SHA256_Final(hash, &sha256);

  static constexpr char kHex[] = "0123456789ABCDEF";
  for (unsigned i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    out[i * 2] = kHex[(hash[i] >> 4) & 0xF];
    out[i * 2 + 1] = kHex[hash[i] & 0xF];
  }
  out[64] = 0;
}

void Run(asio::ssl::context* ssl_cntx, IoContext* io_context) {
  const char kDomain[] = "s3.amazonaws.com";
  const char kRegion[] = "us-east-1";
  const char kService[] = "s3";
  const char kAlgo[] = "AWS4-HMAC-SHA256";

  http::HttpsClient https_client(kDomain, io_context, ssl_cntx);
  system::error_code ec = https_client.Connect(2000);
  CHECK(!ec) << ec << "/" << ec.message();

  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  h2::response<h2::string_body> resp;

  req.set(h2::field::host, kDomain);

  const char* const access_key = getenv("AWS_ACCESS_KEY_ID");
  CHECK(access_key);

  // hash of the empty string.
  const char kPayloadHash[] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  string canonical_headers = absl::StrCat("host", ":", kDomain, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-content-sha256", ":", kPayloadHash, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-date", ":", "20200209T232617Z", "\n");

  string method = "GET";
  string canonical_querystring = "";
  string canonical_request = absl::StrCat(method, "\n", "/", "\n", canonical_querystring, "\n");
  string signed_headers = "host;x-amz-content-sha256;x-amz-date";

  absl::StrAppend(&canonical_request, canonical_headers, "\n", signed_headers, "\n", kPayloadHash);

  string datestamp = "20200209";
  string amz_date = "20200209T233000Z";
  string credential_scope =
      absl::StrCat(datestamp, "/", kRegion, "/", kService, "/", "aws4_request");

  char hexdigest[65];
  sha256_string(canonical_request, hexdigest);

  string string_to_sign =
      absl::StrCat(kAlgo, "\n", amz_date, "\n", credential_scope, "\n", hexdigest);
  cout << "String to sign: " << string_to_sign << "---\n";

  ec = https_client.Send(req, &resp);
  CHECK(!ec) << ec << "/" << ec.message();
  cout << resp << endl;
};

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  auto res = CreateSslContext();
  asio::ssl::context* ssl_cntx = absl::get_if<asio::ssl::context>(&res);
  CHECK(ssl_cntx) << absl::get<system::error_code>(res);

  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  io_context.AwaitSafe([&] { Run(ssl_cntx, &io_context); });

  return 0;
}
