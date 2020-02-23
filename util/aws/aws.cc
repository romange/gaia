// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/aws/aws.h"

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"

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

  Hexify(reinterpret_cast<const char*>(hash), SHA256_DIGEST_LENGTH, out);
}

// TODO: those are used in gcs_utils as well.
using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

constexpr char kAlgo[] = "AWS4-HMAC-SHA256";

}  // namespace

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

  credential_scope_ =
      absl::StrCat(date_str_, "/", region_id_, "/", service_, "/", "aws4_request");

  return Status::OK;
}

// TODO: to support date refreshes - i.e. to update sign_key_, credential_scope_
// if the current has changed.
void AWS::Sign(absl::string_view domain,
               ::boost::beast::http::header<true, ::boost::beast::http::fields>* req) const {
  req->set(h2::field::host, domain);

  // Below is the hexdigest of sha256 of the empty body.
  // TODO: to support non-empty body requests - should be hexdigest of sha256(body).
  const char kPayloadHash[] = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  time_t now = time(nullptr);  // Must be recent (upto 900sec skew is allowed vs amazon servers).

  struct tm tm_s;
  CHECK(&tm_s == gmtime_r(&now, &tm_s));

  char amz_date[32];

  CHECK_GT(strftime(amz_date, arraysize(amz_date), "%Y%m%dT%H%M00Z", &tm_s), 0);
  VLOG(1) << "Time now: " << now;

  string canonical_headers = absl::StrCat("host", ":", domain, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-content-sha256", ":", kPayloadHash, "\n");
  absl::StrAppend(&canonical_headers, "x-amz-date", ":", amz_date, "\n");

  size_t pos = req->target().find('?');
  absl::string_view url = absl_sv(req->target().substr(0, pos));
  absl::string_view canonical_querystring;
  if (pos != string::npos) {
    canonical_querystring = absl_sv(req->target().substr(pos + 1));
  }

  constexpr char kMethod[] = "GET";

  string canonical_request = absl::StrCat(kMethod, "\n", url, "\n", canonical_querystring, "\n");

  string signed_headers = "host;x-amz-content-sha256;x-amz-date";
  absl::StrAppend(&canonical_request, canonical_headers, "\n", signed_headers, "\n", kPayloadHash);
  VLOG(1) << "CanonicalRequest:\n" << canonical_request << "\n-------------------\n";

  char hexdigest[65];
  sha256_string(canonical_request, hexdigest);

  string string_to_sign =
      absl::StrCat(kAlgo, "\n", amz_date, "\n", credential_scope_, "\n", hexdigest);

  char signature[SHA256_DIGEST_LENGTH];
  HMAC(sign_key_, string_to_sign, signature);
  Hexify(signature, SHA256_DIGEST_LENGTH, hexdigest);

  string authorization_header =
      absl::StrCat(kAlgo, " Credential=", access_key_, "/", credential_scope_,
                   ",SignedHeaders=", signed_headers, ",Signature=", hexdigest);

  req->set("x-amz-date", amz_date);
  req->set("x-amz-content-sha256", kPayloadHash);
  req->set(h2::field::authorization, authorization_header);
}

}  // namespace util
