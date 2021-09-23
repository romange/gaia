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
#include "util/http/https_client.h"

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

const char* GCE::GoogleCert() {
  // GTS Root R1 certificate
  return R"(
-----BEGIN CERTIFICATE-----
MIIFWjCCA0KgAwIBAgIQbkepxUtHDA3sM9CJuRz04TANBgkqhkiG9w0BAQwFADBH
MQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNlcnZpY2VzIExM
QzEUMBIGA1UEAxMLR1RTIFJvb3QgUjEwHhcNMTYwNjIyMDAwMDAwWhcNMzYwNjIy
MDAwMDAwWjBHMQswCQYDVQQGEwJVUzEiMCAGA1UEChMZR29vZ2xlIFRydXN0IFNl
cnZpY2VzIExMQzEUMBIGA1UEAxMLR1RTIFJvb3QgUjEwggIiMA0GCSqGSIb3DQEB
AQUAA4ICDwAwggIKAoICAQC2EQKLHuOhd5s73L+UPreVp0A8of2C+X0yBoJx9vaM
f/vo27xqLpeXo4xL+Sv2sfnOhB2x+cWX3u+58qPpvBKJXqeqUqv4IyfLpLGcY9vX
mX7wCl7raKb0xlpHDU0QM+NOsROjyBhsS+z8CZDfnWQpJSMHobTSPS5g4M/SCYe7
zUjwTcLCeoiKu7rPWRnWr4+wB7CeMfGCwcDfLqZtbBkOtdh+JhpFAz2weaSUKK0P
fyblqAj+lug8aJRT7oM6iCsVlgmy4HqMLnXWnOunVmSPlk9orj2XwoSPwLxAwAtc
vfaHszVsrBhQf4TgTM2S0yDpM7xSma8ytSmzJSq0SPly4cpk9+aCEI3oncKKiPo4
Zor8Y/kB+Xj9e1x3+naH+uzfsQ55lVe0vSbv1gHR6xYKu44LtcXFilWr06zqkUsp
zBmkMiVOKvFlRNACzqrOSbTqn3yDsEB750Orp2yjj32JgfpMpf/VjsPOS+C12LOO
Rc92wO1AK/1TD7Cn1TsNsYqiA94xrcx36m97PtbfkSIS5r762DL8EGMUUXLeXdYW
k70paDPvOmbsB4om3xPXV2V4J95eSRQAogB/mqghtqmxlbCluQ0WEdrHbEg8QOB+
DVrNVjzRlwW5y0vtOUucxD/SVRNuJLDWcfr0wbrM7Rv1/oFB2ACYPTrIrnqYNxgF
lQIDAQABo0IwQDAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUwAwEB/zAdBgNV
HQ4EFgQU5K8rJnEaK0gnhS9SZizv8IkTcT4wDQYJKoZIhvcNAQEMBQADggIBADiW
Cu49tJYeX++dnAsznyvgyv3SjgofQXSlfKqE1OXyHuY3UjKcC9FhHb8owbZEKTV1
d5iyfNm9dKyKaOOpMQkpAWBz40d8U6iQSifvS9efk+eCNs6aaAyC58/UEBZvXw6Z
XPYfcX3v73svfuo21pdwCxXu11xWajOl40k4DLh9+42FpLFZXvRq4d2h9mREruZR
gyFmxhE+885H7pwoHyXa/6xmld01D1zvICxi/ZG6qcz8WpyTgYMpl0p8WnK0OdC3
d8t5/Wk6kjftbjhlRn7pYL15iJdfOBL07q9bgsiG1eGZbYwE8na6SfZu6W0eX6Dv
J4J2QPim01hcDyxC2kLGe4g0x8HYRZvBPsVhHdljUEn2NIVq4BjFbkerQUIpm/Zg
DdIx02OYI5NaAIFItO/Nis3Jz5nu2Z6qNuFoS3FJFDYoOj0dzpqPJeaAcWErtXvM
+SUWgeExX6GjfhaknBZqlxi9dnKlC54dNuYvoS++cJEPqOba+MSSQGwlfnuzCdyy
F62ARPBopY+Udf90WuioAnwMCeKpSwughQtiue+hMZL77/ZRBIls6Kl0obsXs7X9
SQ98POyDGCBDTtWTurQ0sR8WNh8M5mQ5Fkzc4P4dyKliPUDqysU0ArSuiYgzNdws
E3PYJ/HQcu51OyLemGhmW/HGY0dVHLqlCFF1pkgl
-----END CERTIFICATE-----
)";
}

const char* GCE::kApiDomain = "www.googleapis.com";

asio::ssl::context GCE::CheckedSslContext() {
  auto res = http::CreateClientSslContext(GCE::GoogleCert());
  asio::ssl::context* ssl_cntx = absl::get_if<asio::ssl::context>(&res);
  CHECK(ssl_cntx) << absl::get<system::error_code>(res);
  return std::move(*ssl_cntx);
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
  system::error_code ec;
  asio::ssl::context ssl_context = CheckedSslContext();
  ssl_ctx_.reset(new SslContext{std::move(ssl_context)});

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
    string tmp_str;
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

std::string GCE::access_token() const {
  std::lock_guard<fibers::mutex> lk(mu_);
  return access_token_;
}

StatusObject<std::string> GCE::RefreshAccessToken(IoContext* context) const {
  h2::response<h2::string_body> resp;
  error_code ec;

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

    beast::flat_buffer buffer;
    h2::read(socket, buffer, resp, ec);
    RETURN_ON_ERROR;
  } else {
    constexpr char kDomain[] = "oauth2.googleapis.com";

    http::HttpsClient https_client(kDomain, context, ssl_ctx_.get());

    ec = https_client.Connect(2000);
    RETURN_ON_ERROR;

    h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
    req.set(h2::field::host, kDomain);
    req.set(h2::field::content_type, "application/x-www-form-urlencoded");

    string& body = req.body();
    body = absl::StrCat("grant_type=refresh_token&client_secret=", client_secret(),
                        "&refresh_token=", refresh_token());
    absl::StrAppend(&body, "&client_id=", client_id());
    req.prepare_payload();
    VLOG(1) << "Req: " << req;

    ec = https_client.Send(req, &resp);
    if (ec) {
      return Status(absl::StrCat("Error sending access token request: ", ec.message()));
    }
  }

  if (resp.result() != h2::status::ok) {
    return Status(StatusCode::IO_ERROR,
                  absl::StrCat("Http error ", string(resp.reason()), "Body: ", resp.body()));
  }
  VLOG(1) << "Resp: " << resp;

  return ParseTokenResponse(std::move(resp.body()));
}

util::StatusObject<std::string> GCE::ParseTokenResponse(std::string&& response) const {
  rj::Document doc;
  constexpr unsigned kFlags = rj::kParseTrailingCommasFlag | rj::kParseCommentsFlag;
  doc.ParseInsitu<kFlags>(&response.front());

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

}  // namespace util
