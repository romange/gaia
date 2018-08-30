// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <memory>

#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/rpc/async_client.h"
#include "util/rpc/frame_format.h"
#include "util/rpc/rpc_test_utils.h"

namespace util {
namespace rpc {

using namespace std;
using namespace boost;
using asio::ip::tcp;


TEST_F(ServerTest, BadHeader) {
  AsyncClient client(std::move(*channel_));
  base::PODArray<uint8_t> header, letter;
  header.resize_fill(14, 1);
  letter.resize_fill(42, 2);
  AsyncClient::future_code_t fc = client.SendEnvelope(&header, &letter);
  EXPECT_FALSE(fc.get());
}


}  // namespace rpc
}  // namespace util
