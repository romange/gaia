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

static util::Status ToStatus(const ::boost::system::error_code& ec) {
  return Status(::util::StatusCode::IO_ERROR, absl::StrCat(ec.value(), ": ", ec.message()));
}

static util::Status SslConnect(SslStream* stream, unsigned ms) {
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

static std::string Stringify(const rj::Value& value) {
  rj::StringBuffer buffer;
  rj::Writer<rj::StringBuffer> writer(buffer);
  value.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
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
  using ListBucketResult = util::StatusObject<std::vector<std::string>>;
  using ReadObjectResult = util::StatusObject<size_t>;

  GCS(const GCE& gce, IoContext* context) : gce_(gce), io_context_(*context) {}

  util::Status Connect(unsigned msec);

  ListBucketResult ListBuckets();

  ReadObjectResult Read(const std::string& bucket, const std::string& path, size_t ofs,
                        const strings::MutableByteRange& range);

 private:
  util::Status RefreshTokenIfNeeded();

  static constexpr char kDomain[] = "www.googleapis.com";

  std::string access_token_;
  std::unique_ptr<SslStream> client_;
};

constexpr char GCS::kDomain[];

util::Status GCS::Connect(unsigned msec) {
  client_.reset(new SslStream(FiberSyncSocket{kDomain, "443", &io_context_}, gce_.ssl_context()));

  return SslConnect(client_.get(), msec);
}

util::Status GCS::RefreshTokenIfNeeded() {
  if (!access_token_.empty())
    return Status::OK;
  auto res = gce_.GetAccessToken(&io_context_);
  if (!res.ok())
    return res.status;

  access_token_ = res.obj;
  return Status::OK;
}

auto GCS::ListBuckets() -> ListBucketResult {
  CHECK(client_);

  RETURN_IF_ERROR(RefreshTokenIfNeeded());

  string url = absl::StrCat("/storage/v1/b?project=", gce_.project_id());
  absl::StrAppend(&url, "&fields=items,nextPageToken");

  h2::request<h2::string_body> req{h2::verb::get, url, 11};

  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, absl::StrCat("Bearer ", access_token_));

  VLOG(1) << "Req: " << req;
  h2::response<h2::dynamic_body> resp;
  auto ec = WriteAndRead(client_.get(), &req, &resp);
  if (ec) {
    return ToStatus(ec);
  }

  string str = beast::buffers_to_string(resp.body().data());

  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.ParseInsitu<kFlags>(&str.front());

  if (doc.HasParseError()) {
    LOG(ERROR) << rj::GetParseError_En(doc.GetParseError()) << str;
    return Status(StatusCode::PARSE_ERROR, "Could not parse json response");
  }

  auto it = doc.FindMember("items");
  CHECK(it != doc.MemberEnd()) << str;
  const auto& val = it->value;
  CHECK(val.IsArray());
  auto array = val.GetArray();

  vector<string> results;
  it = doc.FindMember("nextPageToken");
  CHECK(it == doc.MemberEnd()) << "TBD - to support pagination";

  for (size_t i = 0; i < array.Size(); ++i) {
    const auto& item = array[i];
    auto it = item.FindMember("id");
    if (it != item.MemberEnd()) {
      results.emplace_back(it->value.GetString(), it->value.GetStringLength());
    }
  }
  return results;
}

auto GCS::Read(const std::string& bucket, const std::string& path, size_t ofs,
               const strings::MutableByteRange& range)
    -> ReadObjectResult {
  CHECK(client_);

  RETURN_IF_ERROR(RefreshTokenIfNeeded());
  string url = absl::StrCat("/storage/v1/b/", bucket, "/", path);
  absl::StrAppend(&url, "&alt=media");

  h2::request<h2::string_body> req{h2::verb::get, url, 11};

  req.set(h2::field::host, kDomain);
  req.set(h2::field::authorization, absl::StrCat("Bearer ", access_token_));

  VLOG(1) << "Req: " << req;
  h2::response<h2::dynamic_body> resp;
  auto ec = WriteAndRead(client_.get(), &req, &resp);
  if (ec) {
    return ToStatus(ec);
  }
  return Status::OK;
}

void Run(const GCE& gce, IoContext* context) {
  GCS gcs(gce, context);
  CHECK_STATUS(gcs.Connect(2000));
  auto res = gcs.ListBuckets();
  CHECK_STATUS(res.status);
  for (const auto& s : res.obj) {
    cout << s << endl;
  }
}

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  GCE gce;
  CHECK_STATUS(gce.Init());
  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  io_context.AwaitSafe([&] { Run(gce, &io_context); });

  return 0;
}
