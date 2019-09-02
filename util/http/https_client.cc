// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/https_client.h"

#include "base/logging.h"
#include "util/asio/io_context.h"

#ifdef BOOST_ASIO_SEPARATE_COMPILATION
  #include <boost/asio/ssl/impl/src.hpp>
#endif

namespace util {
namespace http {

using namespace boost;
namespace h2 = beast::http;

HttpsClient::HttpsClient(absl::string_view host, IoContext* context,
                         ::boost::asio::ssl::context* ssl_ctx)
    : io_context_(*context), ssl_cntx_(*ssl_ctx), host_name_(host) {}

auto HttpsClient::Connect(unsigned msec) -> error_code {
  CHECK(io_context_.InContextThread());

  reconnect_msec_ = msec;

  return InitSslClient();
}

auto HttpsClient::InitSslClient() -> error_code {
  VLOG(2) << "Https::InitSslClient " << reconnect_needed_;

  error_code ec;
  if (!reconnect_needed_)
    return ec;
  client_.reset(new SslStream(FiberSyncSocket{host_name_, "443", &io_context_}, ssl_cntx_));
  client_->next_layer().set_keep_alive(true);

  ec = SslConnect(client_.get(), reconnect_msec_);
  if (!ec) {
    reconnect_needed_ = false;
  } else {
    VLOG(1) << "Error connecting " << ec << ", socket " << client_->next_layer().native_handle();
  }
  return ec;
}

auto HttpsClient::DrainResponse(h2::response_parser<h2::buffer_body>* parser) -> error_code {
  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  auto& body = parser->get().body();
  error_code ec;
  size_t sz = 0;

  // Drain pending response completely to allow reusing the current connection.
  VLOG(1) << "parser: " << parser->got_some();
  while (!parser->is_done()) {
    body.data = buf.get();
    body.size = kBufSize;
    size_t raw_bytes = h2::read(*client_, tmp_buffer_, *parser, ec);
    if (ec && ec != h2::error::need_buffer) {
      VLOG(1) << "Error " << ec << "/" << ec.message();
      return ec;
    }
    sz += raw_bytes;

    VLOG(1) << "DrainResp: " << raw_bytes << "/" << body.size;
  }
  VLOG_IF(1, sz > 0) << "Drained " << sz << " bytes";

  return error_code{};
}

::boost::system::error_code SslConnect(SslStream* stream, unsigned ms) {
  system::error_code ec;
  for (unsigned i = 0; i < 2; ++i) {
    ec = stream->next_layer().ClientWaitToConnect(ms);
    if (ec) {
      VLOG(1) << "Error " << i << ": " << ec << "/" << ec.message() << " for socket "
              << stream->next_layer().native_handle();

      continue;
    }

    stream->handshake(asio::ssl::stream_base::client, ec);
    return ec;
  }

  return ec;
}

}  // namespace http
}  // namespace util
