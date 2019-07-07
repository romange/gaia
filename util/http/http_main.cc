// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"

#include "util/http/http_conn_handler.h"
#include "util/html/sorted_table.h"

#include "absl/strings/str_join.h"
#include "base/init.h"
#include "strings/stringpiece.h"

DEFINE_int32(port, 8080, "Port number.");

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  IoContextPool pool;
  AcceptServer server(&pool);
  pool.Run();
  http::Listener<> listener;
  auto cb = [](const http::QueryArgs& args, http::HttpHandler::SendFunction* send) {
    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = "Bar";

    http::SetMime(http::kTextMime, &resp);
    resp.set(beast::http::field::server, "GAIA");

    return send->Invoke(std::move(resp));
  };

  listener.RegisterCb("/foo", false, cb);

  auto table_cb = [](const http::QueryArgs& args, http::HttpHandler::SendFunction* send) {
    using html::SortedTable;

    auto cell = [](auto i, auto j) {
      return absl::StrCat("Val", i, "_", j);
    };


    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = SortedTable::HtmlStart();
    SortedTable::StartTable({"Col1", "Col2", "Col3", "Col4"}, &resp.body());
    for (size_t i = 0; i < 300; ++i) {
      SortedTable::Row({cell(1, i), cell(2, i), cell(3, i), cell(4, i)}, &resp.body());
    }
    SortedTable::EndTable(&resp.body());
    return send->Invoke(std::move(resp));
  };
  listener.RegisterCb("/table", false, table_cb);


  uint16_t port = server.AddListener(FLAGS_port, &listener);
  LOG(INFO) << "Listening on port " << port;

  server.Run();
  server.Wait();

  LOG(INFO) << "Exiting server...";
  return 0;
}
