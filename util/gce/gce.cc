// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/gce.h"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/empty_body.hpp>
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
using tcp = asio::ip::tcp;

static const char kMetaDataHost[] = "metadata.google.internal";

static Status ToStatus(const system::error_code& ec) {
  return Status(StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()));
}

#define RETURN_ON_ERROR \
  if (ec)               \
  return ToStatus(ec)

static StatusObject<string> GetHttp(const h2::request<h2::empty_body>& req, tcp::socket* sock) {
  system::error_code ec;
  boost::beast::flat_buffer buffer;
  h2::response<h2::string_body> resp;

  h2::write(*sock, req, ec);
  RETURN_ON_ERROR;

  h2::read(*sock, buffer, resp, ec);
  RETURN_ON_ERROR;

  return std::move(resp).body();
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
  string tmp_str;
  if (!file_util::ReadFileToString("/etc/ssl/certs/ca-certificates.crt", &tmp_str)) {
    return Status("Could not find certificates");
  }
  system::error_code ec;
  ssl_ctx_.reset(new SslContext{asio::ssl::context::tlsv12_client});
  ssl_ctx_->set_verify_mode(asio::ssl::verify_peer);
  ssl_ctx_->add_certificate_authority(asio::buffer(tmp_str), ec);
  RETURN_ON_ERROR;

  string root_path = file_util::ExpandPath("~/.config/gcloud/");
  string gce_file = absl::StrCat(root_path, "gce");

  asio::io_context io_context;
  tcp::socket socket(io_context);

  auto connect = [&] {
    tcp::resolver resolver{io_context};
    auto const results = resolver.resolve(kMetaDataHost, "80", ec);
    if (!ec) {
      asio::connect(socket, results.begin(), results.end(), ec);
    }
  };

  if (file::Exists(gce_file)) {
    CHECK(file_util::ReadFileToString(gce_file, &tmp_str));
    is_prod_env_ = (tmp_str == "True");
  } else {
    connect();
    is_prod_env_ = !ec;
  }

  if (is_prod_env_) {
    if (!socket.is_open()) {
      connect();
      RETURN_ON_ERROR;
    }

    h2::request<h2::empty_body> req{
        h2::verb::get, "/computeMetadata/v1/instance/service-accounts/default/email", 11};
    req.set("Metadata-Flavor", "Google");
    req.set(h2::field::host, kMetaDataHost);

    VLOG(1) << "Req: " << req;
    auto str_res = GetHttp(req, &socket);
    if (!str_res.ok())
      return str_res.status;
    account_id_ = std::move(str_res.obj);

    req.target("/computeMetadata/v1/project/project-id");
    str_res = GetHttp(req, &socket);
    if (!str_res.ok())
      return str_res.status;
    project_id_ = std::move(str_res.obj);
  } else {
    return ReadDevCreds(root_path);
  }
  return Status::OK;
}

util::Status GCE::ReadDevCreds(const std::string& root_path) {
  RETURN_IF_ERROR(ParseDefaultConfig());
  LOG(INFO) << "Found account " << account_id_ << "/" << project_id_;

  string adc_file = absl::StrCat(root_path, "legacy_credentials/", account_id_, "/adc.json");

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

StatusObject<std::string> GCE::GetAccessToken(IoContext* context, bool force_refresh) const {
  const char kDomain[] = "oauth2.googleapis.com";
  const char kService[] = "443";

  if (!force_refresh) {
    std::lock_guard<fibers::mutex> lk(mu_);
    if (!access_token_.empty())
      return access_token_;
  }

  h2::response<h2::string_body> resp;
  error_code ec;
  beast::flat_buffer buffer;
  access_token_.clear();
  if (is_prod_env_) {
    h2::request<h2::empty_body> req{
        h2::verb::get, "/computeMetadata/v1/instance/service-accounts/default/token", 11};
    req.set("Metadata-Flavor", "Google");
    req.set(h2::field::host, kMetaDataHost);

    FiberSyncSocket socket{kMetaDataHost, "80", context};
    ec = socket.ClientWaitToConnect(2000);
    RETURN_ON_ERROR;

    h2::write(socket, req, ec);
    RETURN_ON_ERROR;

    h2::read(socket, buffer, resp, ec);
    RETURN_ON_ERROR;

  } else {
    SslStream stream(FiberSyncSocket{kDomain, kService, context}, *ssl_ctx_);
    ec = SslConnect(&stream, 2000);
    RETURN_ON_ERROR;

    h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
    req.set(h2::field::host, kDomain);
    req.set(h2::field::content_type, "application/x-www-form-urlencoded");

    string body{"grant_type=refresh_token"};

    absl::StrAppend(&body, "&client_secret=", client_secret(), "&refresh_token=", refresh_token());
    absl::StrAppend(&body, "&client_id=", client_id());

    req.body().assign(body.begin(), body.end());
    req.prepare_payload();
    VLOG(1) << "Req: " << req;

    h2::write(stream, req, ec);
    if (ec) {
      return Status(absl::StrCat("Error sending access token request: ", ec.message()));
    }

    h2::read(stream, buffer, resp, ec);
    if (ec) {
      return Status(absl::StrCat("Error reading access token request: ", ec.message()));
    }
  }

  if (resp.result() != h2::status::ok) {
    return Status(StatusCode::IO_ERROR,
                  absl::StrCat("Http error ", string(resp.reason()), "Body: ", resp.body()));
  }
  VLOG(1) << "Resp: " << resp;
  string& str = resp.body();

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

  std::lock_guard<fibers::mutex> lk(mu_);
  access_token_ = access_token;
  return access_token;
}

void GCE::Test_InjectAcessToken(std::string access_token) {
  std::lock_guard<fibers::mutex> lk(mu_);
  access_token_.swap(access_token);
}

::boost::system::error_code SslConnect(SslStream* stream, unsigned ms) {
  auto ec = stream->next_layer().ClientWaitToConnect(ms);
  if (ec) {
    VLOG(1) << "Error " << ec << "/" << ec.message() << " for socket "
            << stream->next_layer().native_handle();

    return ec;
  }

  stream->handshake(asio::ssl::stream_base::client, ec);
  if (ec) {
    return ec;
  }

  return ::boost::system::error_code{};
}

}  // namespace util
