// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sentry/sentry.h"

#include <boost/fiber/operations.hpp>
#include <cstring>

#include "util/asio/glog_asio_sink.h"
#include "util/http/http_client.h"

namespace util {
using namespace ::boost;
using namespace ::std;

namespace {

class SentrySink : public GlogAsioSink {
 public:
  explicit SentrySink(IoContext* io_context) : client_(io_context) {
  }

 protected:
  void HandleItem(const Item &item) final;

 private:
  http::Client client_;
};

void SentrySink::HandleItem(const Item &item) {

}

}  // namespace

void EnableSentry(IoContext* context) {
  auto ptr = std::make_unique<SentrySink>(context);
  context->AttachCancellable(ptr.release());
}

}  // namespace util
