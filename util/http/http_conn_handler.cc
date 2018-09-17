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

const char kHtmlMime[] = "text/html";
const char kJsonMime[] = "application/json";

namespace {

inline absl::string_view as_absl(::boost::string_view s) {
  return absl::string_view(s.data(), s.size());
}

inline system::error_code to_asio(system::error_code ec) {
  if (ec == h2::error::end_of_stream)
    return asio::error::eof;
  return ec;
}

inline std::pair<StringPiece, StringPiece> Parse(StringPiece str) {
  std::pair<StringPiece, StringPiece> res;
  size_t pos = str.find('?');
  res.first = str.substr(0, pos);
  if (pos != StringPiece::npos) {
    res.second = str.substr(pos + 1);
  }
  return res;
}

vector<std::pair<StringPiece, StringPiece>> SplitQuery(StringPiece query) {
  vector<StringPiece> args = absl::StrSplit(query, '&');
  vector<std::pair<StringPiece, StringPiece>> res(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    size_t pos = args[i].find('=');
    res[i].first = args[i].substr(0, pos);
    res[i].second = (pos == StringPiece::npos) ? StringPiece() : args[i].substr(pos + 1);
  }
  return res;
}

void HandleVModule(StringPiece str) {
  vector<StringPiece> parts = absl::StrSplit(str, ",", absl::SkipEmpty());
  for (StringPiece p : parts) {
    size_t sep = p.find('=');
    int32_t level = 0;
    if (sep != StringPiece::npos && absl::SimpleAtoi(p.substr(sep + 1), &level)) {
      string module_expr = strings::AsString(p.substr(0, sep));
      int prev = google::SetVLOGLevel(module_expr.c_str(), level);
      LOG(INFO) << "Setting module " << module_expr << " to loglevel " << level
                << ", prev: " << prev;
    }
  }
}

void ParseFlagz(const QueryArgs& args, h2::response<h2::string_body>* response) {
  StringPiece flag_name;
  StringPiece value;
  for (const auto& k_v : args) {
    if (k_v.first == "flag") {
      flag_name = k_v.second;
    } else if (k_v.first == "value") {
      value = k_v.second;
    }
  }
  if (!flag_name.empty()) {
    google::CommandLineFlagInfo flag_info;
    string fname = strings::AsString(flag_name);
    if (!google::GetCommandLineFlagInfo(fname.c_str(), &flag_info)) {
      response->body() = "Flag not found \n";
    } else {
      response->set(h2::field::content_type, kHtmlMime);
      response->body().append("<p>Current value ").append(flag_info.current_value).append("</p>");
      string new_val = strings::AsString(value);
      string res = google::SetCommandLineOption(fname.c_str(), new_val.c_str());
      response->body().append("Flag ").append(res);

      if (flag_name == "vmodule") {
        HandleVModule(value);
      }
    }
  } else if (args.size() == 1) {
    LOG(INFO) << "Printing all flags";
    std::vector<google::CommandLineFlagInfo> flags;
    google::GetAllFlags(&flags);
    for (const auto& v : flags) {
      response->body()
          .append("--")
          .append(v.name)
          .append(": ")
          .append(v.current_value)
          .append("\n");
    }
  }
}

}  // namespace

HttpHandler::HttpHandler(const CallbackRegistry* registry) : registry_(registry) {
  favicon_ = "https://rawcdn.githack.com/romange/gaia/master/util/http/favicon-32x32.png";
  resource_prefix_ = "https://rawcdn.githack.com/romange/gaia/05a9c02/util/http";
}

system::error_code HttpHandler::HandleRequest() {
  beast::flat_buffer buffer;
  h2::request<h2::dynamic_body> request;

  system::error_code ec;

  h2::async_read(*socket_, buffer, request, yield[ec]);
  if (ec) {
    return to_asio(ec);
  }
  Response response{h2::status::ok, request.version()};
  response.set(h2::field::server, "GAIA");
  response.keep_alive(request.keep_alive());
  VLOG(1) << "Full Url: " << request.target();

  HandleRequestInternal(as_absl(request.target()), &response);

  response.prepare_payload();
  h2::async_write(*socket_, response, yield[ec]);

  return to_asio(ec);
}

void HttpHandler::HandleRequestInternal(StringPiece target, Response* resp) {
  if (target == "/favicon.ico") {
    resp->set(h2::field::location, favicon_);
    resp->result(h2::status::moved_permanently);
    return;
  }
  StringPiece path, query;
  tie(path, query) = Parse(target);
  auto args = SplitQuery(query);

  if (path == "/") {
    BuildStatusPage(args, resource_prefix_, resp);
    return;
  }
  if (path == "/flagz") {
    if (Authorize(args)) {
      ParseFlagz(args, resp);
    } else {
      resp->result(h2::status::unauthorized);
    }
    return;
  }
  if (registry_) {
    auto it = registry_->cb_map_.find(path);
    if (it == registry_->cb_map_.end() || (it->second.is_protected && !Authorize(args))) {
      resp->result(h2::status::unauthorized);
    } else {
      it->second.cb(args, resp);
    }
  }
}

bool HttpHandler::Authorize(const QueryArgs& args) const {
  for (const auto& k_v : args) {
    if (Authorize(k_v.first, k_v.second))
      return true;
  }
  return false;
}

bool CallbackRegistry::RegisterCb(StringPiece path, bool protect, HttpHandler::RequestCb cb) {
  CbInfo info{protect, cb};
  auto res = cb_map_.emplace(path, info);
  return res.second;
}

}  // namespace http
}  // namespace util
