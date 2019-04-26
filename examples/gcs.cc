// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

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

using SslStream = asio::ssl::stream<FiberSyncSocket>;

util::Status SslConnect(SslStream* stream, unsigned ms) {
  auto ec = stream->next_layer().ClientWaitToConnect(ms);
  if (ec) {
    return Status(absl::StrCat("Could not connect to google: ", ec.message()));
  }

  stream->handshake(asio::ssl::stream_base::client, ec);
  if (ec) {
    return Status(absl::StrCat("Error with ssl handshake: ", ec.message()));
  }

  return Status::OK;
}

class GCE {
 public:
  GCE() = default;

  util::Status Init();

  const std::string& project_id() const { return project_id_; }
  const std::string& client_id() const { return client_id_; }
  const std::string& client_secret() const { return client_secret_; }
  const std::string& account_id() const { return account_id_; }
  const std::string& refresh_token() const { return refresh_token_; }

  asio::ssl::context& ssl_context() const { return *ssl_ctx_; }

  util::StatusObject<std::string> GetAccessToken(IoContext* context) const;

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

util::StatusObject<std::string> GCE::GetAccessToken(IoContext* context) const {
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

class GCS {
  const GCE& gce_;
  IoContext& io_context_;

 public:
  GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}

  void ListBuckets();

 private:
  std::string access_token_;
};

static ::boost::system::error_code WriteAndRead(SslStream* stream,
                                                h2::request<h2::string_body>* req,
                                                h2::response<h2::dynamic_body>* resp) {
  ::boost::system::error_code ec;
  h2::write(*stream, *req, ec);
  if (ec) {
    return ec;
  }

  beast::flat_buffer buffer;

  h2::read(*stream, buffer, *resp, ec);
  return ec;
}


std::string Stringify(const rj::Value& value) {
  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  value.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
}

void GCS::ListBuckets() {
  const char kDomain[] = "www.googleapis.com";

  if (access_token_.empty()) {
    auto res = gce_.GetAccessToken(&io_context_);
    CHECK_STATUS(res.status);
    access_token_ = res.obj;
    LOG(INFO) << "Access token: " << access_token_;
  }

  SslStream stream(FiberSyncSocket{kDomain, "443", &io_context_}, gce_.ssl_context());
  CHECK_STATUS(SslConnect(&stream, 2000));

  string url = absl::StrCat("/storage/v1/b?project=", gce_.project_id());
  h2::request<h2::string_body> req{h2::verb::get, url, 11};

  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, absl::StrCat("Bearer ", access_token_));

  VLOG(1) << "Req: " << req;
  h2::response<h2::dynamic_body> resp;
  auto ec = WriteAndRead(&stream, &req, &resp);
  if (ec) {
    LOG(FATAL) << "Error communicating with google: " + ec.message();
  }

  string str = beast::buffers_to_string(resp.body().data());

  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.ParseInsitu<kFlags>(&str.front());

  if (doc.HasParseError()) {
    LOG(FATAL) << rj::GetParseError_En(doc.GetParseError());
  }

  auto it = doc.FindMember("items");
  CHECK(it != doc.MemberEnd()) << str;
  const auto& val = it->value;
  CHECK(val.IsArray());
  auto array = val.GetArray();
  for (size_t i = 0; i < array.Size(); ++i) {
    LOG(INFO) << i << ": " << Stringify(array[i]);
  }
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  GCE gce;
  CHECK_STATUS(gce.Init());
  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  GCS gcs(gce, &io_context);

  io_context.AwaitSafe([&] { gcs.ListBuckets(); });

  return 0;
}
