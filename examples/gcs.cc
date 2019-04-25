// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>  // for operator<<
#include <boost/beast/websocket/ssl.hpp>

#include "absl/strings/str_join.h"
#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;
using asio::ip::tcp;

DEFINE_string(gs_oauth2_refresh_token, "", "");
DEFINE_string(client_secret, "", "");
DEFINE_string(client_id, "", "");

const char kDomain[] = "oauth2.googleapis.com";
const char kService[] = "443";


void Run(IoContext& io_context) {
  string cert;
  file_util::ReadFileToStringOrDie("/etc/ssl/certs/ca-certificates.crt", &cert);
  asio::ssl::context ctx(asio::ssl::context::tlsv12_client);
  ctx.set_verify_mode(asio::ssl::verify_peer);

  system::error_code error_code;
  ctx.add_certificate_authority(asio::buffer(cert), error_code);
  CHECK(!error_code) << error_code.message();

  tcp::resolver resolver(io_context.raw_context());
  auto endpoints = resolver.resolve(kDomain, kService);

  asio::ssl::stream<tcp::socket> stream(io_context.raw_context(), ctx);

#if 0
  // Set SNI Hostname (many hosts need this to handshake successfully)
  if (!SSL_set_tlsext_host_name(stream.native_handle(), kDomain)) {
    LOG(FATAL) << "boo";
  }
#endif

  asio::connect(stream.next_layer(), endpoints, error_code);
  CHECK(!error_code);

  stream.handshake(asio::ssl::stream_base::client, error_code);
  CHECK(!error_code) << error_code.message();

  h2::request<h2::string_body> req{h2::verb::post, "/token", 11};
  req.set(h2::field::host, kDomain);
  req.set(h2::field::content_type, "application/x-www-form-urlencoded");

  string body;

  absl::StrAppend(&body, "client_secret=", FLAGS_client_secret, "&grant_type=refresh_token",
                  "&refresh_token=", FLAGS_gs_oauth2_refresh_token);
  absl::StrAppend(&body, "&client_id=", FLAGS_client_id);

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

  CHECK(!FLAGS_client_id.empty() && !FLAGS_client_secret.empty() &&
        !FLAGS_gs_oauth2_refresh_token.empty());

  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();
  io_context.AwaitSafe([&] { Run(io_context); });

  return 0;
}
