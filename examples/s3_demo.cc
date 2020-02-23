// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <openssl/hmac.h>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/string_body.hpp>

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/future.hpp>

#include "absl/strings/str_cat.h"
#include "base/init.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "strings/escaping.h"
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/aws/aws.h"
#include "util/http/https_client.h"
#include "util/http/https_client_pool.h"

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

DEFINE_string(prefix, "", "");
DEFINE_bool(get, false, "");

// TODO: those are used in gcs_utils as well. CreateSslContext is used in gce.
using bb_str_view = ::boost::beast::string_view;

inline absl::string_view absl_sv(const bb_str_view s) {
  return absl::string_view{s.data(), s.size()};
}

http::SslContextResult CreateSslContext() {
  system::error_code ec;
  asio::ssl::context cntx{asio::ssl::context::tlsv12_client};
  cntx.set_options(boost::asio::ssl::context::default_workarounds |
                   boost::asio::ssl::context::no_compression | boost::asio::ssl::context::no_sslv2 |
                   boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
                   boost::asio::ssl::context::no_tlsv1_1);
  cntx.set_verify_mode(asio::ssl::verify_peer, ec);
  if (ec) {
    return http::SslContextResult(ec);
  }
  // cntx.add_certificate_authority(asio::buffer(cert_string.data(), cert_string.size()), ec);
  cntx.load_verify_file("/etc/ssl/certs/ca-certificates.crt", ec);
  if (ec) {
    return http::SslContextResult(ec);
  }

#if 0
  SSL_CTX* ssl_cntx = cntx.native_handle();

  long flags = SSL_CTX_get_options(ssl_cntx);
  flags |= SSL_OP_CIPHER_SERVER_PREFERENCE;
  SSL_CTX_set_options(ssl_cntx, flags);

  constexpr char kCiphers[] = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256";
  CHECK_EQ(1, SSL_CTX_set_cipher_list(ssl_cntx, kCiphers));
  CHECK_EQ(1, SSL_CTX_set_ecdh_auto(ssl_cntx, 1));
#endif

  return http::SslContextResult(std::move(cntx));
}


const char kRootDomain[] = "s3.amazonaws.com";

void List(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  size_t pos = FLAGS_prefix.find('/');
  CHECK_NE(string::npos, pos);
  string bucket = FLAGS_prefix.substr(0, pos);

  string domain = absl::StrCat(bucket, ".", kRootDomain);
  http::HttpsClient https_client(domain, io_context, ssl_cntx);
  system::error_code ec = https_client.Connect(2000);
  CHECK(!ec) << ec << "/" << ec.message();


  string url;
  string key = FLAGS_prefix.substr(pos + 1);
  if (FLAGS_get) {
    absl::StrAppend(&url, "/", key);
  } else {
    url = "/?delimeter=";
    strings::AppendEncodedUrl("/", &url);
    url.append("&prefix=");
    strings::AppendEncodedUrl(key, &url);
  }


  h2::request<h2::empty_body> req{h2::verb::get, url, 11};
  h2::response<h2::string_body> resp;

  aws->Sign(domain, &req);

  LOG(INFO) << "Request: " << req;
  ec = https_client.Send(req, &resp);
  CHECK(!ec) << ec << "/" << ec.message();
  cout << resp << endl;
}

void ListBuckets(asio::ssl::context* ssl_cntx, AWS* aws, IoContext* io_context) {
  http::HttpsClient https_client(kRootDomain, io_context, ssl_cntx);
  system::error_code ec = https_client.Connect(2000);
  CHECK(!ec) << ec << "/" << ec.message();

  h2::request<h2::empty_body> req{h2::verb::get, "/", 11};
  h2::response<h2::string_body> resp;

  aws->Sign(kRootDomain, &req);

  ec = https_client.Send(req, &resp);
  CHECK(!ec) << ec << "/" << ec.message();
  cout << resp.body() << endl;
};

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  auto res = CreateSslContext();
  asio::ssl::context* ssl_cntx = absl::get_if<asio::ssl::context>(&res);
  CHECK(ssl_cntx) << absl::get<system::error_code>(res);

  IoContextPool pool;
  pool.Run();

  IoContext& io_context = pool.GetNextContext();

  AWS aws{"us-east-1", "s3"};

  CHECK_STATUS(aws.Init());

  if (FLAGS_prefix.empty()) {
    io_context.AwaitSafe([&] { ListBuckets(ssl_cntx, &aws, &io_context); });
  } else {
    io_context.AwaitSafe([&] { List(ssl_cntx, &aws, &io_context); });
  }
  return 0;
}
