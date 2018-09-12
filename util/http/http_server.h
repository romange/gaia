// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

#include <functional>
#include <memory>
#include <vector>

#include "base/macros.h"
#include "util/status.h"
#include "strings/stringpiece.h"
#include "util/http/http_status_code.h"

#error Deprecated.

namespace http {

class Request {
  friend class Server;
  struct Rep;

  explicit Request(Rep* rep) : rep_(rep) {}
public:
  typedef std::vector<std::pair<StringPiece, StringPiece>> KeyValueArray;

  const char* method() const; // "GET", "POST", etc
  const char* uri() const;            // URL-decoded URI
  StringPiece query() const;   // URL part after '?', not including '?', or empty stringpiece.

  // returns key,value pairs of the parsed query.
  KeyValueArray ParsedQuery() const;

private:

  Rep* rep_ = nullptr;
  DISALLOW_COPY_AND_ASSIGN(Request);
};


class Response {
  friend class Server;
  struct Rep;

  explicit Response(Rep* rep) : rep_(rep) {}
public:
  static const char kHtmlMime[];
  static const char kTextMime[];
  static const char kJsonMime[];

  // mime_type must exist through the lifetime of Response. Even better
  // to use global constants, for example, the constants above.
  void SetContentType(const char* mime_type);  // text/plain, text/html etc

  util::Status Send(HttpStatusCode code);

  //string& content();

  Response& AppendContent(StringPiece str);

  // example of header arguments: "Access-Control-Allow-Origin",  "*"
  // Both arguments should live through the lifetime of this object.
  // i.e. works usually for hardcoded values.
  Response& AddHeader(const char* header, const char* value);

  // Allocates and copies value (only) into internal buffer why still referencing header directly.
  Response& AddHeaderCopy(const char* header, const char* value);

  void SendFile(const char* local_file, HttpStatusCode code);
private:

  Rep* rep_ = nullptr;
  DISALLOW_COPY_AND_ASSIGN(Response);
};

class Server {
public:
  typedef std::function<void(const Request&, Response*)> HttpHandler;

  explicit Server(int port);
  ~Server();

  // Asynchronously starts server and returns to the calling thread.
  util::Status Start();

  // Blocks the calling thread until a signal to finish was received.
  void Wait();

  // Assumes that url is contant string that is never destroyed.
  void RegisterHandler(StringPiece url, HttpHandler handler);

  int port() const;

private:
  // Closes down the http resources and shuts down the server.
  // Not needed to call if  Wait().
  void Shutdown();

  struct Rep;
  std::unique_ptr<Rep> rep_;
};

} // namespace http

#endif  // HTTP_SERVER_H
