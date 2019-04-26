// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;
namespace rj = rapidjson;
using asio::ip::tcp;

const char kDomain[] = "oauth2.googleapis.com";
const char kService[] = "443";

class GCE {
 public:
  GCE() = default;

  util::Status Init();

  const std::string& project_id() const { return project_id_; }
  const std::string& client_id() const { return client_id_; }
  const std::string& client_secret() const { return client_secret_; }
  const std::string& account_id() const { return account_id_; }
  const std::string& refresh_token() const { return refresh_token_; }

 private:
  util::Status ParseDefaultConfig();

  std::string project_id_, client_id_, client_secret_, account_id_, refresh_token_;
  unique_ptr<asio::ssl::context> ssl_ctx_;
};

util::Status GCE::ParseDefaultConfig() {
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

util::Status GCE::Init() {
  string cert;
  if (!file_util::ReadFileToString("/etc/ssl/certs/ca-certificates.crt", &cert)) {
    return Status("Could not find certificates");
  }
  ssl_ctx_.reset(new asio::ssl::context{asio::ssl::context::tlsv12_client});
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

void Run(IoContext& io_context, GCE& gce) {
  string cert;
  file_util::ReadFileToStringOrDie("/etc/ssl/certs/ca-certificates.crt", &cert);
  asio::ssl::context ctx(asio::ssl::context::tlsv12_client);
  ctx.set_verify_mode(asio::ssl::verify_peer);

  system::error_code error_code;
  ctx.add_certificate_authority(asio::buffer(cert), error_code);
  CHECK(!error_code) << error_code.message();

  asio::ssl::stream<FiberSyncSocket> stream(FiberSyncSocket{kDomain, kService, &io_context}, ctx);

#if 0
  // Set SNI Hostname (many hosts need this to handshake successfully)
  if (!SSL_set_tlsext_host_name(stream.native_handle(), kDomain)) {
    LOG(FATAL) << "boo";
  }
#endif
  error_code = stream.next_layer().ClientWaitToConnect(2000);
  CHECK(!error_code) << error_code.message();

  stream.handshake(asio::ssl::stream_base::client, error_code);
  CHECK(!error_code) << error_code.message();

  h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
  req.set(h2::field::host, kDomain);
  req.set(h2::field::content_type, "application/x-www-form-urlencoded");

  string body;

  absl::StrAppend(&body, "client_secret=", gce.client_secret(), "&grant_type=refresh_token",
                  "&refresh_token=", gce.refresh_token());
  absl::StrAppend(&body, "&client_id=", gce.client_id());

  /*http::Client client(&io_context);

  auto ec = client.Connect("oauth2.googleapis.com", "80");
  CHECK(!ec) << ec.message();

  client.AddHeader("Content-Type", "application/x-www-form-urlencoded");
  string body("grant_type=refresh_token");
  absl::StrAppend(&body, "&client_secret=", FLAGS_client_secret,
                  "&refresh_token=", FLAGS_gs_oauth2_refresh_token);
  absl::StrAppend(&body, "client_id=", FLAGS_client_id);

  ec = client.Send(http::Client::Verb::post, "/token", body, &resp);
  CHECK(!ec) << ec.message();
*/
  req.body().assign(body.begin(), body.end());
  req.prepare_payload();
  LOG(INFO) << "Req: " << req;

  h2::write(stream, req);

  beast::flat_buffer buffer;

  // http::Client::Response resp;
  h2::response<h2::dynamic_body> resp;

  h2::read(stream, buffer, resp, error_code);
  CHECK(!error_code);

  LOG(INFO) << "Resp: " << resp;
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  GCE gce;
  CHECK_STATUS(gce.Init());
  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();
  io_context.AwaitSafe([&] { Run(io_context, gce); });

  return 0;
}
