// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/ssl/stream.hpp>
#include "util/asio/fiber_socket.h"

namespace util {
namespace http {

namespace detail {
class Engine {
 public:
  using verify_mode = ::boost::asio::ssl::verify_mode;
  using want = ::boost::asio::ssl::detail::engine::want;

  // Construct a new engine for the specified context.
  explicit Engine(SSL_CTX* context);

  // Destructor.
  ~Engine();

  // Get the underlying implementation in the native type.
  SSL* native_handle();

  // Set the peer verification mode.
  boost::system::error_code set_verify_mode(verify_mode v, boost::system::error_code& ec);

  // Set the peer verification depth.
  boost::system::error_code set_verify_depth(int depth, boost::system::error_code& ec);

  // Perform an SSL handshake using either SSL_connect (client-side) or
  // SSL_accept (server-side).
  want handshake(::boost::asio::ssl::stream_base::handshake_type type,
                 boost::system::error_code& ec);

  // Perform a graceful shutdown of the SSL session.
  want shutdown(boost::system::error_code& ec);

  // Write bytes to the SSL session.
  want write(const boost::asio::const_buffer& data, boost::system::error_code& ec,
             std::size_t& bytes_transferred);

  // Read bytes from the SSL session.
  want read(const boost::asio::mutable_buffer& data, boost::system::error_code& ec,
            std::size_t& bytes_transferred);

  // Get output data to be written to the transport.
  boost::asio::mutable_buffer get_output(const boost::asio::mutable_buffer& data);

  // Put input data that was read from the transport.
  boost::asio::const_buffer put_input(const boost::asio::const_buffer& data);

  // Map an error::eof code returned by the underlying transport according to
  // the type and state of the SSL session. Returns a const reference to the
  // error code object, suitable for passing to a completion handler.
  const boost::system::error_code& map_error_code(boost::system::error_code& ec) const;

 private:
  // Disallow copying and assignment.
  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  // Perform one operation. Returns >= 0 on success or error, want_read if the
  // operation needs more input, or want_write if it needs to write some output
  // before the operation can complete.
  want perform(int (Engine::*op)(void*, std::size_t), void* data, std::size_t length,
               boost::system::error_code& ec, std::size_t* bytes_transferred);

  // Adapt the SSL_connect function to the signature needed for perform().
  int do_connect(void*, std::size_t);

  // Adapt the SSL_shutdown function to the signature needed for perform().
  int do_shutdown(void*, std::size_t);

  // Adapt the SSL_read function to the signature needed for perform().
  int do_read(void* data, std::size_t length);

  // Adapt the SSL_write function to the signature needed for perform().
  int do_write(void* data, std::size_t length);

  SSL* ssl_;
  BIO* ext_bio_;
};

}  // namespace detail

class SslStream {
  using Impl = ::boost::asio::ssl::stream<FiberSyncSocket>;

  SslStream(const SslStream&) = delete;
  SslStream& operator=(const SslStream&) = delete;

 public:
  using next_layer_type = Impl::next_layer_type;
  using lowest_layer_type = Impl::lowest_layer_type;
  using error_code = boost::system::error_code;

  SslStream(FiberSyncSocket&& arg, ::boost::asio::ssl::context& ctx);

  // To support socket requirements.
  next_layer_type& next_layer() {
    return next_layer_;
  }

  lowest_layer_type& lowest_layer() {
    return next_layer_.lowest_layer();
  }

  // (fiber) SyncRead interface:
  // https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncReadStream.html
  template <typename MBS> size_t read_some(const MBS& bufs, error_code& ec) {
    namespace detail = ::boost::asio::ssl::detail;

    size_t res = io(next_layer_, detail::read_op<MBS>(bufs), ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncReadStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename MBS> size_t read_some(const MBS& bufs);

  //! SyncWrite interface:
  //! https://www.boost.org/doc/libs/1_69_0/doc/html/boost_asio/reference/SyncWriteStream.html
  template <typename BS> size_t write_some(const BS& bufs, error_code& ec) {
    namespace detail = ::boost::asio::ssl::detail;

    size_t res = io(next_layer_, detail::write_op<BS>(bufs), ec);
    last_err_ = ec;
    return res;
  }

  //! To calm SyncWriteStream compile-checker we provide exception-enabled interface without
  //! implementing it.
  template <typename BS> size_t write_some(const BS& bufs);

  void handshake(Impl::handshake_type type, error_code& ec);

  const error_code& last_error() const {
    return last_err_;
  }

  auto native_handle() {
    return engine_.native_handle();
  }

  // private:
  // The SSL engine.
  ::boost::asio::ssl::detail::engine engine_;

  // Buffer space used to prepare output intended for the transport.
  std::vector<unsigned char> output_buffer_space_;

  // A buffer that may be used to prepare output intended for the transport.
  const boost::asio::mutable_buffer output_buffer_;

  // Buffer space used to read input intended for the engine.
  std::vector<unsigned char> input_buffer_space_;

  // A buffer that may be used to read input intended for the engine.
  const boost::asio::mutable_buffer input_buffer_;

  // The buffer pointing to the engine's unconsumed input.
  boost::asio::const_buffer input_;

 private:
  enum { max_tls_record_size = 17 * 1024 };

  template <typename Stream, typename Operation>
  std::size_t io(Stream& next_layer, const Operation& op, boost::system::error_code& ec);

  FiberSyncSocket next_layer_;

  error_code last_err_;
};

template <typename Stream, typename Operation>
std::size_t SslStream::io(Stream& next_layer, const Operation& op, boost::system::error_code& ec) {
  using engine = ::boost::asio::ssl::detail::engine;

  boost::system::error_code io_ec;
  std::size_t bytes_transferred = 0;
  do
    switch (op(engine_, ec, bytes_transferred)) {
      case engine::want_input_and_retry:

        // If the input buffer is empty then we need to read some more data from
        // the underlying transport.
        if (input_.size() == 0) {
          input_ = boost::asio::buffer(input_buffer_, next_layer.read_some(input_buffer_, io_ec));
          if (!ec)
            ec = io_ec;
        }

        // Pass the new input data to the engine.
        input_ = engine_.put_input(input_);

        // Try the operation again.
        continue;

      case engine::want_output_and_retry:

        // Get output data from the engine and write it to the underlying
        // transport.
        boost::asio::write(next_layer, engine_.get_output(output_buffer_), io_ec);
        if (!ec)
          ec = io_ec;

        // Try the operation again.
        continue;

      case engine::want_output:

        // Get output data from the engine and write it to the underlying
        // transport.
        boost::asio::write(next_layer, engine_.get_output(output_buffer_), io_ec);
        if (!ec)
          ec = io_ec;

        // Operation is complete. Return result to caller.
        engine_.map_error_code(ec);
        return bytes_transferred;

      default:

        // Operation is complete. Return result to caller.
        engine_.map_error_code(ec);
        return bytes_transferred;
    }
  while (!ec);

  // Operation failed. Return result to caller.
  engine_.map_error_code(ec);
  return 0;
}

}  // namespace http
}  // namespace util
