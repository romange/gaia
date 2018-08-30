// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/fiber/future.hpp>

#include "util/asio/asio_utils.h"
#include "util/rpc/async_client.h"
#include "util/rpc/frame_format.h"

namespace util {
namespace rpc {

using namespace boost;

namespace {

template<typename R> fibers::future<std::decay_t<R>> make_ready(R&& r) {
  fibers::promise<std::decay_t<R>> p;
  fibers::future<std::decay_t<R>> res = p.get_future();
  p.set_value(std::forward<R>(r));

  return res;
}

}  // namespace


auto AsyncClient::SendEnvelope(base::PODArray<uint8_t>* header, base::PODArray<uint8_t>* letter)
  -> future_code_t {
  Frame frame(rpc_id_, header->size(), letter->size());
  uint8_t buf[Frame::kMaxByteSize];
  size_t bsz = frame.Write(buf);

  fibers::promise<error_code> p;
  fibers::future<error_code> res = p.get_future();

  error_code ec = channel_.Write(make_buffer_seq(asio::buffer(buf, bsz), *header, *letter));
  if (ec) {
    p.set_value(ec);
    return res;
  }
  p.set_value(ec);
  return res;
}

void AsyncClient::SetupReadFiber() {
}


}  // namespace rpc
}  // namespace util
