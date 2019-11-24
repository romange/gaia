// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/ssl_stream.h"
#include "base/logging.h"

namespace util {

namespace http {

using namespace boost;
using asio::ssl::detail::stream_core;

namespace {

template <typename Stream, typename Operation>
std::size_t io_fun(Stream& next_layer, const Operation& op, SslStream* core,
                   system::error_code& ec) {
  system::error_code io_ec;
  std::size_t bytes_transferred = 0;
  using asio::ssl::detail::engine;
  DVLOG(1) << "io_fun::start";

  do
    switch (op(core->engine_, ec, bytes_transferred)) {
      case engine::want_input_and_retry:
        DVLOG(2) << "want_input_and_retry";

        // If the input buffer is empty then we need to read some more data from
        // the underlying transport.
        if (core->input_.size() == 0) {
          core->input_ =
              asio::buffer(core->input_buffer_, next_layer.read_some(core->input_buffer_, io_ec));
          if (!ec)
            ec = io_ec;
        }

        // Pass the new input data to the engine.
        core->input_ = core->engine_.put_input(core->input_);

        // Try the operation again.
        continue;

      case engine::want_output_and_retry:
        DVLOG(2) << "engine::want_output_and_retry";

        // Get output data from the engine and write it to the underlying
        // transport.
        asio::write(next_layer, core->engine_.get_output(core->output_buffer_), io_ec);
        if (!ec)
          ec = io_ec;

        // Try the operation again.
        continue;

      case engine::want_output:
        DVLOG(2) << "engine::want_output";

        // Get output data from the engine and write it to the underlying
        // transport.
        asio::write(next_layer, core->engine_.get_output(core->output_buffer_), io_ec);
        if (!ec)
          ec = io_ec;

        // Operation is complete. Return result to caller.
        core->engine_.map_error_code(ec);
        return bytes_transferred;

      default:
        DVLOG(2) << "core->engine_.map_error_code";

        // Operation is complete. Return result to caller.
        core->engine_.map_error_code(ec);
        return bytes_transferred;
    }
  while (!ec);

  // Operation failed. Return result to caller.
  core->engine_.map_error_code(ec);
  return 0;
}

}  // namespace

SslStream::SslStream(FiberSyncSocket&& arg, asio::ssl::context& ctx)
    : next_layer_(std::move(arg)), engine_(ctx.native_handle()),
      output_buffer_space_(max_tls_record_size),
      output_buffer_(boost::asio::buffer(output_buffer_space_)),
      input_buffer_space_(max_tls_record_size),
      input_buffer_(boost::asio::buffer(input_buffer_space_)) {
}

void SslStream::handshake(Impl::handshake_type type, error_code& ec) {
  namespace detail = asio::ssl::detail;

  io_fun(next_layer_, detail::handshake_op(type), this, ec);
}

}  // namespace http
}  // namespace util
