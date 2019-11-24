// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/ssl_stream.h"

namespace util {

namespace http {

SslStream::SslStream(FiberSyncSocket&& arg, ::boost::asio::ssl::context& ctx)
    : next_layer_(std::move(arg)),
      core_(ctx.native_handle(), next_layer_.next_layer().get_executor()) {
}

}  // namespace http
}  // namespace util
