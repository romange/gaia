// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gce.h"

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "util/asio/fiber_socket.h"

namespace util {
using namespace std;
using namespace boost;

namespace h2 = beast::http;
namespace rj = rapidjson;

static util::Status ToStatus(const ::boost::system::error_code& ec) {
  return Status(::util::StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()));
}


Status GCE::ParseDefaultConfig() {
  string config = file_util::ExpandPath("~/.config/gcloud/configurations/config_default");
  if (!file::Exists(config)) {
    return Status("Could not find config_default");
  }
  file::LineReader reader(config);
  string scratch;
  StringPiece line;

  while (reader.Next(&line, &scratch)) {
    vector<StringPiece> vals = absl::StrSplit(line, "=");
    if (vals.size() != 2)
      continue;
    for (auto& v : vals) {
      v = absl::StripAsciiWhitespace(v);
    }
    if (vals[0] == "account") {
      account_id_ = string(vals[1]);
    } else if (vals[0] == "project") {
      project_id_ = string(vals[1]);
    }
  }

  if (account_id_.empty() || project_id_.empty()) {
    return Status("Could not find required fields in config_default");
  }
  return Status::OK;
}

Status GCE::Init() {
  string cert;
  if (!file_util::ReadFileToString("/etc/ssl/certs/ca-certificates.crt", &cert)) {
    return Status("Could not find certificates");
  }
  ssl_ctx_.reset(new SslContext{asio::ssl::context::tlsv12_client});
  ssl_ctx_->set_verify_mode(asio::ssl::verify_peer);
  boost::system::error_code ec;
  ssl_ctx_->add_certificate_authority(asio::buffer(cert), ec);
  if (ec) {
    return Status(ec.message());
  }

  RETURN_IF_ERROR(ParseDefaultConfig());
  LOG(INFO) << "Found account " << account_id_ << "/" << project_id_;

  string adc_file = file_util::ExpandPath(
      absl::StrCat("~/.config/gcloud/legacy_credentials/", account_id_, "/adc.json"));

  string adc;
  if (!file_util::ReadFileToString(adc_file, &adc)) {
    return Status("Could not find " + adc_file);
  }

  rj::Document adc_doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  adc_doc.ParseInsitu<kFlags>(&adc.front());

  if (adc_doc.HasParseError()) {
    return Status(rj::GetParseError_En(adc_doc.GetParseError()));
  }
  for (auto it = adc_doc.MemberBegin(); it != adc_doc.MemberEnd(); ++it) {
    if (it->name == "client_id") {
      client_id_ = it->value.GetString();
    } else if (it->name == "client_secret") {
      client_secret_ = it->value.GetString();
    } else if (it->name == "refresh_token") {
      refresh_token_ = it->value.GetString();
    }
  }
  if (client_id_.empty() || client_secret_.empty() || refresh_token_.empty()) {
    return Status("Did not find secret tokens");
  }
  return Status::OK;
}

StatusObject<std::string> GCE::GetAccessToken(IoContext* context) const {
  const char kDomain[] = "oauth2.googleapis.com";
  const char kService[] = "443";

  SslStream stream(FiberSyncSocket{kDomain, kService, context}, *ssl_ctx_);
  RETURN_IF_ERROR(SslConnect(&stream, 2000));

  h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
  req.set(h2::field::host, kDomain);
  req.set(h2::field::content_type, "application/x-www-form-urlencoded");

  string body{"grant_type=refresh_token"};

  absl::StrAppend(&body, "&client_secret=", client_secret(), "&refresh_token=", refresh_token());
  absl::StrAppend(&body, "&client_id=", client_id());

  req.body().assign(body.begin(), body.end());
  req.prepare_payload();
  VLOG(1) << "Req: " << req;

  ::boost::system::error_code ec;
  h2::write(stream, req, ec);
  if (ec) {
    return Status(absl::StrCat("Error sending access token request: ", ec.message()));
  }

  beast::flat_buffer buffer;

  h2::response<h2::dynamic_body> resp;

  h2::read(stream, buffer, resp, ec);
  if (ec) {
    return Status(absl::StrCat("Error reading access token request: ", ec.message()));
  }
  string str = beast::buffers_to_string(resp.body().data());

  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.ParseInsitu<kFlags>(&str.front());

  if (doc.HasParseError()) {
    return Status(rj::GetParseError_En(doc.GetParseError()));
  }

  string access_token, token_type;
  for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    if (it->name == "access_token") {
      access_token = it->value.GetString();
    } else if (it->name == "token_type") {
      token_type = it->value.GetString();
    }
  }
  if (token_type != "Bearer" || access_token.empty()) {
    return Status(absl::StrCat("Bad json response: ", doc.GetString()));
  }
  return access_token;
}

util::Status SslConnect(SslStream* stream, unsigned ms) {
  auto ec = stream->next_layer().ClientWaitToConnect(ms);
  if (ec) {
    return ToStatus(ec);
  }

  stream->handshake(asio::ssl::stream_base::client, ec);
  if (ec) {
    return ToStatus(ec);
  }

  return Status::OK;
}

}  // namespace util
