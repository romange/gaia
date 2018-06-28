// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef HTTP_CLIENT_H
#define HTTP_CLIENT_H

#include <memory>
#include "strings/stringpiece.h"
#include "util/status.h"

namespace http {

// This class is not thread-safe!.
class HttpClient {

  struct Rep;
  std::unique_ptr<Rep> rep_;
public:
  typedef std::function<size_t(StringPiece data)> CallbackFun;
  HttpClient();
  ~HttpClient();

  typedef std::vector<std::pair<std::string, std::string>> Args;
  // Encode (key,value) arguments into key1=val1&key2=val2... query. Each val is url encoded
  // as well.
  static std::string EncodeUrl(const Args& args, bool url_encode = true);

  void set_timeout(uint32 msec);

  util::Status Fetch(StringPiece url, CallbackFun fun);
  util::Status ReadToString(StringPiece url, std::string* dest);

  void AddHeader(StringPiece header);  // Adds header to all future requests.
  void ClearHeaders(); // Clear all headers.
private:

  static size_t curl_fun(char *ptr, size_t size, size_t nmemb, void *userdata);
};

}  // namespace http

#endif  // HTTP_CLIENT_H