// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sentry/sentry.h"

#include <boost/beast/http/write.hpp>  // For serializing req/resp to ostream
#include <cstring>

#include "base/logging.h"
#include <glog/raw_logging.h>

#include "strings/strcat.h"
#include "util/asio/glog_asio_sink.h"
#include "util/http/http_client.h"

DEFINE_string(sentry_dsn, "", "Sentry DSN in the format <pivatekey>@hostname/<project_id>");

namespace util {
using namespace ::boost;
using namespace ::std;

namespace {

struct Dsn {
  string key;
  string hostname;
  string url;
};

class SentrySink : public GlogAsioSink {
 public:
  explicit SentrySink(Dsn dsn, IoContext* io_context);

 protected:
  void HandleItem(const Item& item) final;

  bool ShouldIgnore(google::LogSeverity severity, const char* full_filename, int line) final {
    return severity < google::GLOG_ERROR;
  }

  void Cancel() final {
    GlogAsioSink::Cancel();
    client_.Shutdown();
    VLOG(1) << "Sentry::Cancel End";
  }

 private:
  string GenSentryBody(const Item& item);

  http::Client client_;
  Dsn dsn_;
  string port_;
};

/* The structure is as follows:

curl  -H 'X-Sentry-Auth: Sentry sentry_version=6, sentry_key=<private-key>' -i
-d '{"event_id": "<event_id>","culprit": "myfile.cc:12", "server_name": "someserver",
"message": "Error message", "level": "error", "platform": "c",
"timestamp": "2018-12-01T21:00:36"}' http://sentry.url/api/project_id/store/

*/

SentrySink::SentrySink(Dsn dsn, IoContext* io_context) : client_(io_context), dsn_(std::move(dsn)) {
  size_t pos = dsn_.hostname.find(':');
  if (pos != string::npos) {
    port_ = dsn_.hostname.substr(pos + 1);
    dsn_.hostname.resize(pos);
  } else {
    port_ = "80";
  }
  client_.AddHeader("X-Sentry-Auth",
                    absl::StrCat("Sentry sentry_version=6, sentry_key=", dsn_.key));
  client_.AddHeader("Content-Type", "application/json");
  client_.AddHeader("Host", dsn_.hostname);
  client_.AddHeader("User-Agent", "gaia-cpp/0.1");
  dsn_.url = absl::StrCat("/api", dsn_.url, "/store/");
}

void SentrySink::HandleItem(const Item& item) {
  auto ec = client_.Connect(dsn_.hostname, port_);
  if (ec) {
    auto msg = ec.message();
    RAW_VLOG(1, "Could not connect %s", msg.c_str());
    ++lost_messages_;
    return;
  }

  http::Client::Response resp;
  string body = GenSentryBody(item);
  ec = client_.Send(http::Client::Verb::post, dsn_.url, body, &resp);

  if (ec) {
    RAW_VLOG(1, "Could not send ");
    ++lost_messages_;
  }
}

string SentrySink::GenSentryBody(const Item& item) {
  string res = absl::StrCat(R"({"culprit":")", item.base_filename, ":", item.line,
                            R"(", "server_name":"TBD")");

  absl::StrAppend(&res,
                  ",\n"
                  R"( "message":")",
                  item.message,
                  R"(", "level":"error", "platform": "c++", "sdk": {"name": "sentry-cpp",
                  "version": "1.0.0"}, "timestamp":")");
  absl::StrAppend(&res, 1900 + item.tm_time.tm_year, "-", item.tm_time.tm_mon + 1, "-",
                  item.tm_time.tm_mday, "T", item.tm_time.tm_hour, ":", item.tm_time.tm_min, ":",
                  item.tm_time.tm_sec);
  absl::StrAppend(&res, R"("})");

  return res;
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
  res->url = dsn.substr(pos);

  VLOG(1) << "Dsn is: " << res->key << "|" << res->hostname << "|" << res->url;
  return true;
}

}  // namespace

void EnableSentry(IoContext* context) {
  if (FLAGS_sentry_dsn.empty()) {
    LOG(INFO) << "--sentry_dsn is not defined, sentry is disabled";
    return;
  }

  Dsn dsn;
  CHECK(ParseDsn(FLAGS_sentry_dsn, &dsn)) << "Could not parse " << FLAGS_sentry_dsn;

  auto ptr = std::make_unique<SentrySink>(std::move(dsn), context);
  context->AttachCancellable(ptr.release());
}

}  // namespace util
