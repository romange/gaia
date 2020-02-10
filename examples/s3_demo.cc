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

#include "absl/strings/escaping.h"
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

void Hexify(const char* str, size_t len, char* dest) {
  static constexpr char kHex[] = "0123456789abcdef";

  for (unsigned i = 0; i < len; ++i) {
    *dest++ = kHex[(str[i] >> 4) & 0xF];
    *dest++ = kHex[str[i] & 0xF];
  }
  *dest = '\0';
}

void sha256_string(absl::string_view str, char out[65]) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, str.data(), str.size());
  SHA256_Final(hash, &sha256);

  Hexify(reinterpret_cast<const char *>(hash), SHA256_DIGEST_LENGTH, out);
}

static void HMAC(absl::string_view key, absl::string_view msg, string* dest) {
  HMAC_CTX* hmac = HMAC_CTX_new();

  CHECK_EQ(1, HMAC_CTX_reset(hmac));
  CHECK_EQ(1, HMAC_Init_ex(hmac, reinterpret_cast<const unsigned char*>(key.data()),
                           key.size(), EVP_sha256(), NULL));

  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const unsigned char*>(msg.data()), msg.size()));

  unsigned int len = 32;
  dest->resize(len);

  unsigned char* ptr = reinterpret_cast<unsigned char*>(&dest->front());
  CHECK_EQ(1, HMAC_Final(hmac, ptr, &len));
  HMAC_CTX_free(hmac);
  CHECK_EQ(len, 32);
}

string GetSignatureKey(absl::string_view key, absl::string_view datestamp, absl::string_view region,
                       absl::string_view service) {
  string sign;
  HMAC(absl::StrCat("AWS4", key), datestamp, &sign);
  HMAC(sign, region, &sign);
  HMAC(sign, service, &sign);
  HMAC(sign, "aws4_request", &sign);
  return sign;
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
  const char* const secret_key = getenv("AWS_SECRET_ACCESS_KEY");
  CHECK(secret_key);

  // hash of the empty string.
  const char kPayloadHash[] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  time_t now = time(nullptr);  // Must be recent (upto 900sec skew is allowed vs amazon servers).

  struct tm tm_s;
  CHECK(&tm_s == gmtime_r(&now, &tm_s));

  char datestamp[16], amz_date[32];

  CHECK_GT(strftime(datestamp, arraysize(datestamp), "%Y%m%d", &tm_s), 0);
  CHECK_GT(strftime(amz_date, arraysize(amz_date), "%Y%m%dT%H%M00Z", &tm_s), 0);
  LOG(INFO) << "Time now: " << now;

  string canonical_headers = absl::StrCat("host", ":", kDomain, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-content-sha256", ":", kPayloadHash, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-date", ":", amz_date, "\n");

  string method = "GET";
  string canonical_querystring = "";

  string canonical_request = absl::StrCat(method, "\n", "/", "\n", canonical_querystring, "\n");

  string signed_headers = "host;x-amz-content-sha256;x-amz-date";
  absl::StrAppend(&canonical_request, canonical_headers, "\n", signed_headers, "\n", kPayloadHash);
  VLOG(1) << "CanonicalRequest:\n" << canonical_request << "\n-------------------\n";

  string credential_scope =
      absl::StrCat(datestamp, "/", kRegion, "/", kService, "/", "aws4_request");

  char hexdigest[65];
  sha256_string(canonical_request, hexdigest);

  string string_to_sign =
      absl::StrCat(kAlgo, "\n", amz_date, "\n", credential_scope, "\n", hexdigest);

  // signing_key is not dependent on the request, could be cached between requests for the same
  // service/region.
  string signing_key = GetSignatureKey(secret_key, datestamp, kRegion, kService);
  VLOG(1) << "signing_key: " << absl::Base64Escape(signing_key);

  string signature;
  HMAC(signing_key, string_to_sign, &signature);
  Hexify(signature.data(), signature.size(), hexdigest);

  string authorization_header = absl::StrCat(kAlgo, " Credential=", access_key, "/",
                                             credential_scope, ",SignedHeaders=", signed_headers,
                                             ",Signature=", hexdigest);

  req.set("x-amz-date", amz_date);
  req.set("x-amz-content-sha256", kPayloadHash);
  req.set(h2::field::authorization, authorization_header);

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
