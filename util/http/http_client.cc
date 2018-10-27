// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/http_client.h"

#include <mutex>
#include <curl/curl.h>

#include "base/logging.h"
#include "base/integral_types.h"
#include "strings/strcat.h"
#include "strings/stringpiece.h"
#include "absl/strings/escaping.h"
#include "util/status.h"
#include "util/http/http_status_code.h"

DEFINE_bool(enable_curl_verbosity, false, "");

namespace http {

using namespace std;
using util::Status;
using util::StatusCode;


namespace {

util::Status from_curl(CURLcode code) {
  return code == CURLE_OK ? Status::OK : Status(StatusCode::IO_ERROR, curl_easy_strerror(code));
}

std::once_flag http_client_init;

}  // namespace

struct HttpClient::Rep {
  CURL* curl = nullptr;
  std::vector<string> headers;
};

HttpClient::HttpClient() : rep_(new Rep) {
  std::call_once(http_client_init, []() { CHECK_EQ(0, curl_global_init(CURL_GLOBAL_NOTHING)); });

  rep_->curl = curl_easy_init();
  CHECK_EQ(CURLE_OK, curl_easy_setopt(rep_->curl, CURLOPT_WRITEFUNCTION, &HttpClient::curl_fun));
  if (FLAGS_enable_curl_verbosity) {
    CHECK_EQ(CURLE_OK, curl_easy_setopt(rep_->curl, CURLOPT_VERBOSE, 1)) ;
  }
}

HttpClient::~HttpClient() {
  curl_easy_cleanup(rep_->curl);
}

std::string HttpClient::EncodeUrl(const Args& args, bool url_encode) {
  std::string res;
  std::string tmp;

  for (const auto& a : args) {
    if (url_encode) {
      absl::WebSafeBase64Escape(a.second, &tmp);
    }
    res.append(a.first).append("=").append(url_encode ? tmp : a.second).append("&");
  }
  if (!res.empty()) {
    res.pop_back();
  }
  return res;
}

#define CURL_OP(x) { auto code = x; \
     if (code != CURLE_OK) return from_curl(code); \
     } while(false);

void HttpClient::set_timeout(uint32 msec) {
  curl_easy_setopt(rep_->curl, CURLOPT_TIMEOUT_MS, msec);
}

util::Status HttpClient::Fetch(StringPiece url, CallbackFun fun) {
  CURL_OP(curl_easy_setopt(rep_->curl, CURLOPT_URL, url.data()));
  CURL_OP(curl_easy_setopt(rep_->curl, CURLOPT_WRITEDATA, &fun));
  struct curl_slist* header_list = nullptr;
  if (!rep_->headers.empty()) {
    for (const string& h : rep_->headers) {
      header_list = curl_slist_append(header_list, h.c_str());
    }
    CHECK_EQ(CURLE_OK, curl_easy_setopt(rep_->curl, CURLOPT_HTTPHEADER, header_list));
  }
  CURL_OP(curl_easy_perform(rep_->curl));
  curl_slist_free_all(header_list);

  long reply = 0;
  curl_easy_getinfo(rep_->curl, CURLINFO_RESPONSE_CODE , &reply);
  if (reply == HTTP_BAD_REQUEST || reply == HTTP_NOT_FOUND) {
    return Status(StatusCode::IO_ERROR, absl::StrCat(reply, ", ",
      StatusStringFromCode(HttpStatusCode(reply))));
  }
  return Status::OK;
}

util::Status HttpClient::ReadToString(StringPiece url, string* dest) {
  dest->clear();
  return Fetch(url, [dest](StringPiece data)
                     {
                      dest->append(data.data(), data.size());
                      return data.size();
                    });
}

void HttpClient::AddHeader(StringPiece header) {
  rep_->headers.push_back(strings::AsString(header));
}

void HttpClient::ClearHeaders() {
  rep_->headers.clear();
}

size_t HttpClient::curl_fun(char *ptr, size_t size, size_t nmemb, void *userdata) {
  CallbackFun* fun = (CallbackFun*)userdata;
  size_t sz = size * nmemb;
  return (*fun)(StringPiece(ptr, sz));
}

}  // namespace http
