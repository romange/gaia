// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/aws/aws.h"

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "base/logging.h"

namespace util {

using namespace std;
using namespace boost;
namespace h2 = beast::http;

namespace {

void HMAC(absl::string_view key, absl::string_view msg, char dest[SHA256_DIGEST_LENGTH]) {
  HMAC_CTX* hmac = HMAC_CTX_new();

  CHECK_EQ(1, HMAC_CTX_reset(hmac));
  CHECK_EQ(1, HMAC_Init_ex(hmac, reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                           EVP_sha256(), NULL));

  CHECK_EQ(1, HMAC_Update(hmac, reinterpret_cast<const uint8_t*>(msg.data()), msg.size()));

  uint8_t* ptr = reinterpret_cast<uint8_t*>(dest);
  unsigned len = 32;
  CHECK_EQ(1, HMAC_Final(hmac, ptr, &len));
  HMAC_CTX_free(hmac);
  CHECK_EQ(len, 32);
}

string GetSignatureKey(absl::string_view key, absl::string_view datestamp, absl::string_view region,
                       absl::string_view service) {
  char sign[32];
  absl::string_view sign_key{sign, sizeof(sign)};
  HMAC(absl::StrCat("AWS4", key), datestamp, sign);
  HMAC(sign_key, region, sign);
  HMAC(sign_key, service, sign);
  HMAC(sign_key, "aws4_request", sign);

  return string(sign_key);
}

void Hexify(const char* str, size_t len, char* dest) {
  static constexpr char kHex[] = "0123456789abcdef";

  for (unsigned i = 0; i < len; ++i) {
    char c = str[i];
    *dest++ = kHex[(c >> 4) & 0xF];
    *dest++ = kHex[c & 0xF];
  }
  *dest = '\0';
}

// TODO: those are used in gcs_utils as well.
using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

constexpr char kAlgo[] = "AWS4-HMAC-SHA256";

}  // namespace

namespace detail {

void Sha256String(absl::string_view str, char out[65]) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha256;
  SHA256_Init(&sha256);
  SHA256_Update(&sha256, str.data(), str.size());
  SHA256_Final(hash, &sha256);

  Hexify(reinterpret_cast<const char*>(hash), SHA256_DIGEST_LENGTH, out);
}

}  // namespace detail

::boost::asio::ssl::context AWS::CheckedSslContext() {
  system::error_code ec;
  asio::ssl::context cntx{asio::ssl::context::tlsv12_client};
  cntx.set_options(boost::asio::ssl::context::default_workarounds |
                   boost::asio::ssl::context::no_compression | boost::asio::ssl::context::no_sslv2 |
                   boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
                   boost::asio::ssl::context::no_tlsv1_1);
  cntx.set_verify_mode(asio::ssl::verify_peer, ec);
  if (ec) {
    LOG(FATAL) << "Could not set verify mode " << ec;
  }
  // cntx.add_certificate_authority(asio::buffer(cert_string.data(), cert_string.size()), ec);
  cntx.load_verify_file("/etc/ssl/certs/ca-certificates.crt", ec);
  if (ec) {
    LOG(FATAL) << "Could not load certificates file" << ec;
  }

  return cntx;
}

Status AWS::Init() {
  const char* access_key = getenv("AWS_ACCESS_KEY_ID");
  const char* secret_key = getenv("AWS_SECRET_ACCESS_KEY");

  if (!access_key)
    return Status("Can not find AWS_ACCESS_KEY_ID");

  if (!secret_key)
    return Status("Can not find AWS_ACCESS_KEY_ID");

  secret_ = secret_key;
  access_key_ = access_key;

  time_t now = time(nullptr);  // Must be recent (upto 900sec skew is allowed vs amazon servers).

  struct tm tm_s;
  CHECK(&tm_s == gmtime_r(&now, &tm_s));

  CHECK_GT(strftime(date_str_, arraysize(date_str_), "%Y%m%d", &tm_s), 0);
  sign_key_ = GetSignatureKey(secret_, date_str_, region_id_, service_);

  credential_scope_ = absl::StrCat(date_str_, "/", region_id_, "/", service_, "/", "aws4_request");

  return Status::OK;
}

// TODO: to support date refreshes - i.e. to update sign_key_, credential_scope_
// if the current has changed.
void AWS::Sign(absl::string_view domain, absl::string_view body,
               ::boost::beast::http::header<true, ::boost::beast::http::fields>* header) const {
  header->set(h2::field::host, domain);

  time_t now = time(nullptr);  // Must be recent (upto 900sec skew is allowed vs amazon servers).

  struct tm tm_s;
  CHECK(&tm_s == gmtime_r(&now, &tm_s));

  char amz_date[32];

  CHECK_GT(strftime(amz_date, arraysize(amz_date), "%Y%m%dT%H%M00Z", &tm_s), 0);
  VLOG(1) << "Time now: " << now;

  char hexdigest[65];

  detail::Sha256String(body, hexdigest);

  header->set("x-amz-date", amz_date);
  header->set("x-amz-content-sha256", hexdigest);

  string canonical_headers = absl::StrCat("host", ":", domain, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-content-sha256", ":", hexdigest, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-date", ":", amz_date, "\n");

  string auth_header =
      AuthHeader(absl_sv(header->method_string()), canonical_headers, absl_sv(header->target()),
                 absl::string_view{hexdigest, 64}, amz_date);

  header->set(h2::field::authorization, auth_header);
}

string AWS::AuthHeader(absl::string_view method, absl::string_view headers,
                       absl::string_view target, absl::string_view content_sha256,
                       absl::string_view amz_date) const {
  size_t pos = target.find('?');
  absl::string_view url = target.substr(0, pos);
  absl::string_view query_string;
  string canonical_querystring;

  if (pos != string::npos) {
    query_string = target.substr(pos + 1);

    // We must sign query string with params in alphabetical order
    vector<absl::string_view> params = absl::StrSplit(query_string, "&", absl::SkipWhitespace{});
    sort(params.begin(), params.end());
    canonical_querystring = absl::StrJoin(params, "&");
  }

  string canonical_request = absl::StrCat(method, "\n", url, "\n", canonical_querystring, "\n");
  string signed_headers = "host;x-amz-content-sha256;x-amz-date";
  absl::StrAppend(&canonical_request, headers, "\n", signed_headers, "\n", content_sha256);
  VLOG(1) << "CanonicalRequest:\n" << canonical_request << "\n-------------------\n";

  char hexdigest[65];
  detail::Sha256String(canonical_request, hexdigest);

  string string_to_sign =
      absl::StrCat(kAlgo, "\n", amz_date, "\n", credential_scope_, "\n", hexdigest);

  char signature[SHA256_DIGEST_LENGTH];
  HMAC(sign_key_, string_to_sign, signature);
  Hexify(signature, SHA256_DIGEST_LENGTH, hexdigest);

  string authorization_header =
      absl::StrCat(kAlgo, " Credential=", access_key_, "/", credential_scope_,
                   ",SignedHeaders=", signed_headers, ",Signature=", hexdigest);

  return authorization_header;
}

}  // namespace util
