// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/http_conn_handler.h"

#include <boost/beast/core.hpp>  // for flat_buffer.
#include <boost/beast/http.hpp>

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "base/logging.h"
#include "strings/stringpiece.h"
#include "util/asio/yield.h"
#include "util/http/status_page.h"

using namespace std;

namespace util {
namespace http {
using namespace boost;
namespace h2 = beast::http;

using fibers_ext::yield;

namespace {

inline system::error_code to_asio(system::error_code ec) {
  if (ec == h2::error::end_of_stream)
    return asio::error::eof;
  return ec;
}

void FilezHandler(const QueryArgs& args, HttpHandler::SendFunction* send) {
  StringPiece file_name;
  for (const auto& k_v : args) {
    if (k_v.first == "file") {
      file_name = k_v.second;
    }
  }
  if (file_name.empty()) {
    http::StringResponse resp = MakeStringResponse(h2::status::unauthorized);
    return send->Invoke(std::move(resp));
  }
  string fname = strings::AsString(file_name);
  FileResponse fresp;
  auto ec = LoadFileResponse(fname, &fresp);
  if (ec) {
    StringResponse res = MakeStringResponse(h2::status::not_found);
    SetMime(kTextMime, &res);
    if (ec == boost::system::errc::no_such_file_or_directory)
      res.body() = "The resource '" + fname + "' was not found.";
    else
      res.body() = "Error '" + ec.message() + "'.";
    return send->Invoke(std::move(res));
  }

  return send->Invoke(std::move(fresp));
}

}  // namespace

HttpHandler::HttpHandler(const ListenerBase* lb, IoContext* cntx)
    : ConnectionHandler(cntx), registry_(lb) {
  favicon_ = "https://rawcdn.githack.com/romange/gaia/master/util/http/favicon-32x32.png";
  resource_prefix_ = "https://cdn.jsdelivr.net/gh/romange/gaia/util/http";
}

system::error_code HttpHandler::HandleRequest() {
  beast::flat_buffer buffer;
  RequestType request;

  system::error_code ec;

  h2::read(*socket_, buffer, request, ec);
  if (ec) {
    return to_asio(ec);
  }
  VLOG(1) << "Full Url: " << request.target();

  SendFunction send(*socket_);
  HandleRequestInternal(request, &send);

  VLOG(1) << "HandleRequestEnd: " << send.ec;

  return to_asio(send.ec);
}

void HttpHandler::HandleRequestInternal(const RequestType& request, SendFunction* send) {
  StringPiece target = as_absl(request.target());
  if (target == "/favicon.ico") {
    h2::response<h2::string_body> resp = MakeStringResponse(h2::status::moved_permanently);
    resp.set(h2::field::location, favicon_);
    resp.set(h2::field::server, "GAIA");
    resp.keep_alive(request.keep_alive());

    return send->Invoke(std::move(resp));
  }

  StringPiece path, query;
  tie(path, query) = ParseQuery(target);
  auto args = SplitQuery(query);

  if (path == "/") {
    return send->Invoke(BuildStatusPage(args, resource_prefix_));
  }

  if (path == "/flagz") {
    h2::response<h2::string_body> resp(h2::status::ok, request.version());
    if (Authorize(args)) {
      resp = ParseFlagz(args);
    } else {
      resp.result(h2::status::unauthorized);
    }
    return send->Invoke(std::move(resp));
  }

  if (path == "/filez") {
    FilezHandler(args, send);
    return;
  }

  if (path == "/profilez") {
    send->Invoke(ProfilezHandler(args));
    return;
  }

  if (registry_) {
    auto it = registry_->cb_map_.find(path);
    if (it == registry_->cb_map_.end() || (it->second.is_protected && !Authorize(args))) {
      h2::response<h2::string_body> resp(h2::status::unauthorized, request.version());
      return send->Invoke(std::move(resp));
    }
    it->second.cb(args, send);
  }
}

bool ListenerBase::RegisterCb(StringPiece path, bool protect, RequestCb cb) {
  CbInfo info{protect, cb};
  auto res = cb_map_.emplace(path, info);
  return res.second;
}

}  // namespace http
}  // namespace util
