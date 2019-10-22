// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/buffer_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>

#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "util/asio/fiber_socket.h"

namespace util {
class IoContext;
class FiberSyncSocket;

namespace http {

class SslStream {
  using Impl = ::boost::asio::ssl::stream<FiberSyncSocket>;

 public:
  using next_layer_type = Impl::next_layer_type;
  using lowest_layer_type = Impl::lowest_layer_type;
  using error_code = boost::system::error_code;

  SslStream(FiberSyncSocket&& arg, ::boost::asio::ssl::context& ctx)
      : wrapped_(std::move(arg), ctx) {}

  // To support socket requirements.
  next_layer_type& next_layer() { return wrapped_.next_layer(); }
  lowest_layer_type& lowest_layer() { return wrapped_.lowest_layer(); }

  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec) {
    size_t res = wrapped_.read_some(bufs, ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncReadStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  //! SyncWrite interface:
  //! https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec) {
    size_t res = wrapped_.write_some(bufs, ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  void handshake(Impl::handshake_type type, error_code& ec) {
    wrapped_.handshake(type, ec);
  }

  const error_code& last_error() const { return last_err_;}

 private:
  Impl wrapped_;
  error_code last_err_;
};

// Waiting for std::expected to arrive. Meanwhile we use this interface.
using SslContextResult = absl::variant<::boost::system::error_code, ::boost::asio::ssl::context>;
SslContextResult CreateClientSslContext(absl::string_view cert_string);

class HttpsClient {
 public:
  using error_code = ::boost::system::error_code;

  /**
   * @brief Construct a new Https Client object
   *
   * @param host - a domain of the service like "www.googleapis.com"
   * @param io_context - IoContext thread in which the connection is running.
   *                     HttpsClient should be called only from this thread.
   * @param ssl_ctx    - SSL context for this connection.
   */
  HttpsClient(absl::string_view host, IoContext* io_context, ::boost::asio::ssl::context* ssl_ctx);
  HttpsClient(const HttpsClient&) = delete;
  HttpsClient(HttpsClient&&) = delete;

  error_code Connect(unsigned msec);

  /*! @brief Sends http request but does not read response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method.
   */
  template <typename Req> error_code Send(const Req& req);

  /*! @brief Sends http request and reads response back.
   *
   *  Possibly retries and reconnects if there are problems with connection.
   *  See set_retry_count(uint32_t) method for details.
   */
  template <typename Req, typename Resp> error_code Send(const Req& req, Resp* resp);

  error_code ReadHeader(::boost::beast::http::basic_parser<false>* parser);
  template <typename Parser> error_code Read(Parser* parser);

  error_code DrainResponse(
      ::boost::beast::http::response_parser<::boost::beast::http::buffer_body>* parser);

  SslStream* client() { return client_.get(); }

  void schedule_reconnect() { reconnect_needed_ = true; }

  int32_t native_handle() { return client_->next_layer().native_handle(); }

  uint32_t retry_count() const { return retry_cnt_; }

  //! Sets number of retries for Send(...) methods.
  void set_retry_count(uint32_t cnt) { retry_cnt_ = cnt; }

  ::boost::asio::ssl::context& ssl_context() { return ssl_cntx_; }

  error_code status() const {
    namespace err = ::boost::asio::error;

    return reconnect_needed_ ? err::not_connected : client_->next_layer().status();
  }

 private:
  error_code HandleError(const error_code& ec);

  bool IsError(const error_code& ec) const {
    using err = ::boost::beast::http::error;
    return ec && ec != err::need_buffer;
  }

  error_code ReconnectIfNeeded() {
    if (reconnect_needed_)
      return InitSslClient();
    return error_code{};
  }

  error_code InitSslClient();

  IoContext& io_context_;
  ::boost::asio::ssl::context& ssl_cntx_;

  ::boost::beast::flat_buffer tmp_buffer_;

  std::string host_name_;
  std::unique_ptr<SslStream> client_;

  uint32_t reconnect_msec_ = 1000;
  bool reconnect_needed_ = true;
  uint32_t retry_cnt_ = 1;
};

// ::boost::system::error_code SslConnect(SslStream* stream, unsigned msec);

template <typename Req, typename Resp>
auto HttpsClient::Send(const Req& req, Resp* resp) -> error_code {
  namespace h2 = ::boost::beast::http;
  error_code ec;

  for (uint32_t i = 0; i < retry_cnt_; ++i) {
    ec = Send(req);
    if (IsError(ec))  // Send already retries.
      break;

    h2::read(*client_, tmp_buffer_, *resp, ec);
    if (!IsError(ec)) {
      return ec;
    }
    *resp = Resp{};
  }
  return HandleError(ec);
}

template <typename Req> auto HttpsClient::Send(const Req& req) -> error_code {
  error_code ec;
  for (uint32_t i = 0; i < retry_cnt_; ++i) {
    ec = ReconnectIfNeeded();
    if (IsError(ec))
      continue;
    ::boost::beast::http::write(*client_, req, ec);
    if (IsError(ec)) {
      reconnect_needed_ = true;
    } else {
      return ec;
    }
  }
  return HandleError(ec);
}

// Read methods should not reconnect since they assume some state (i.e. reading http request).
inline auto HttpsClient::ReadHeader(::boost::beast::http::basic_parser<false>* parser)
    -> error_code {
  error_code ec;

  ::boost::beast::http::read_header(*client_, tmp_buffer_, *parser, ec);
  return HandleError(ec);
}

template <typename Parser> auto HttpsClient::Read(Parser* parser) -> error_code {
  error_code ec;

  // Note that read returns number of raw bytes read from stream before parsing which
  // does not correlate to the final data stored inside the parser.
  ::boost::beast::http::read(*client_, tmp_buffer_, *parser, ec);
  return HandleError(ec);
}

inline auto HttpsClient::HandleError(const error_code& ec) -> error_code {
  if (IsError(ec))
    reconnect_needed_ = true;
  return ec;
}

}  // namespace http
}  // namespace util
