// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/gce/detail/gcs_utils.h"

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "util/http/https_client.h"

namespace util {
namespace detail {

using namespace boost;
using namespace http;
using namespace ::std;

std::ostream& operator<<(std::ostream& os, const h2::response<h2::buffer_body>& msg) {
  os << msg.reason() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  os << "-------------------------";

  return os;
}

h2::request<h2::dynamic_body> PrepareGenericRequest(h2::verb req_verb, const bb_str_view url,
                                                    const bb_str_view token) {
  h2::request<h2::dynamic_body> req(req_verb, url, 11);
  req.set(h2::field::host, GCE::kApiDomain);

  AddBearer(absl_sv(token), &req);
  return req;
}

ApiSenderBase::~ApiSenderBase() {}

StatusObject<HttpsClientPool::ClientHandle> ApiSenderBase::SendGeneric(unsigned num_iterations,
                                                                       Request req) {
  system::error_code ec;
  HttpsClientPool::ClientHandle handle;

  for (unsigned iters = 0; iters < num_iterations; ++iters) {
    if (!handle) {
      handle = pool_->GetHandle();
      ec = handle->status();
      if (ec) {
        return ToStatus(ec);
      }
    }
    const Request::header_type& header = req;

    VLOG(1) << "ReqIter " << iters << ": socket " << handle->native_handle() << " " << header;

    ec = SendRequestIterative(req, handle.get());

    if (!ec) {  // Success.
      return handle;
    }

    if (ec == asio::error::no_permission) {
      auto token_res = gce_.RefreshAccessToken(&pool_->io_context());
      if (!token_res.ok())
        return token_res.status;

      AddBearer(token_res.obj, &req);
    } else if (ec == asio::error::try_again) {
      ++num_iterations;
      LOG(INFO) << "RespIter " << iters << ": socket " << handle->native_handle()
                << " retrying";
    } else {
      LOG(INFO) << "RespIter " << iters << ": socket " << handle->native_handle()
                << " failed with error " << ec << "/" << ec.message();
      handle.reset();
    }
  }

  return Status(StatusCode::IO_ERROR, "Maximum iterations reached");
}

auto ApiSenderBufferBody::SendRequestIterative(const Request& req, HttpsClient* client)
    -> error_code {
  system::error_code ec = client->Send(req);
  if (ec)
    return ec;

  parser_.emplace().body_limit(kuint64max);
  ec = client->ReadHeader(&parser_.value());

  if (ec) {
    return ec;
  }

  if (!parser_->keep_alive()) {
    client->schedule_reconnect();
    LOG(INFO) << "Scheduling reconnect due to conn-close header";
  }

  const auto& msg = parser_->get();
  VLOG(1) << "HeaderResp(" << client->native_handle() << "): " << msg;

  // Partial content can appear because of the previous reconnect.
  if (msg.result() == h2::status::ok || msg.result() == h2::status::partial_content) {
    return error_code{};  // all is good.
  }

  // Parse & drain whatever comes after problematic status.
  // We must do it as long as we plan to use this connection for more requests.
  ec = client->DrainResponse(&parser_.value());
  if (ec) {
    return ec;
  }

  if (DoesServerPushback(msg.result())) {
    LOG(INFO) << "Retrying(" << client->native_handle() << ") with " << msg;

    this_fiber::sleep_for(1s);
    return asio::error::try_again;  // retry
  }

  if (IsUnauthorized(msg)) {
    return asio::error::no_permission;
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return h2::error::bad_status;
}

}  // namespace detail
}  // namespace util
