// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sentry/sentry.h"

#include <boost/fiber/operations.hpp>
#include <cstring>

#include "util/asio/glog_asio_sink.h"
#include "util/http/http_client.h"

DEFINE_string(sentry_dsn, "", "Sentry DSN in the format "
                              "<pivatekey>@hostname/<project_id>");

namespace util {
using namespace ::boost;
using namespace ::std;

namespace {

struct Dsn {
  string key;
  string hostname;
  string project_id;
};


class SentrySink : public GlogAsioSink {
 public:
  explicit SentrySink(Dsn dsn, IoContext* io_context);

 protected:
  void HandleItem(const Item &item) final;

  bool ShouldIgnore(google::LogSeverity severity, const char *full_filename, int line) final {
    return severity < google::GLOG_ERROR;
  }

 private:
  http::Client client_;
  Dsn dsn_;
  string port_;
};

SentrySink::SentrySink(Dsn dsn, IoContext* io_context) : client_(io_context), dsn_(std::move(dsn)) {
  size_t pos = dsn_.hostname.find(':');
  if (pos != string::npos) {
    port_ = dsn_.hostname.substr(pos + 1);
    dsn_.hostname.resize(pos);
  } else {
    port_ = "80";
  }
}

void SentrySink::HandleItem(const Item& item) {
  auto ec = client_.Connect(dsn_.hostname, port_);
  if (ec) {
    ++lost_messages_;
    return;
  }

  http::Client::Response resp;
  ec = client_.Get(dsn_.project_id, &resp);
  if (ec) {
    ++lost_messages_;
  } else {
    LOG(INFO) << "Success";
  }
}

bool ParseDsn(const string& dsn, Dsn* res) {
  CHECK(!dsn.empty());
  size_t kpos = dsn.find('@');
  if (kpos == string::npos)
    return false;
  res->key = dsn.substr(0, kpos);
  ++kpos;
  size_t pos = dsn.find('/', kpos);
  if (kpos == string::npos)
    return false;
  res->hostname = dsn.substr(kpos, pos - kpos);
  res->project_id = dsn.substr(pos);

  VLOG(1) << "Dsn is: " << res->key << "|" << res->hostname << "|" << res->project_id;
  return true;
}

}  // namespace

void EnableSentry(IoContext* context) {
  Dsn dsn;
  CHECK(ParseDsn(FLAGS_sentry_dsn, &dsn)) << "Could not parse " << FLAGS_sentry_dsn;

  auto ptr = std::make_unique<SentrySink>(std::move(dsn), context);
  context->AttachCancellable(ptr.release());
}

}  // namespace util
