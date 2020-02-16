// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/aws/aws.h"

#include <openssl/hmac.h>

#include "absl/strings/string_view.h"
#include "absl/strings/str_cat.h"
#include "base/logging.h"

namespace util {
using namespace std;
using namespace boost;

namespace {

void HMAC(absl::string_view key, absl::string_view msg, char dest[32]) {
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

  return Status::OK;
}

}  // namespace util
