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


GcsFileBase::~GcsFileBase() {}

Status GcsFileBase::OpenGeneric(unsigned num_iterations) {
  HttpsClientPool::ClientHandle handle;

  auto token_res = gce_.GetAccessToken(&pool_->io_context());

  if (!token_res.ok())
    return token_res.status;
  string& token = token_res.obj;

  auto req = PrepareRequest(token);

  system::error_code ec;

  for (unsigned iters = 0; iters < num_iterations; ++iters) {
    if (!handle) {
      handle = pool_->GetHandle();
      ec = handle->status();
      if (ec) {
        return detail::ToStatus(ec);
      }
    }
    VLOG(1) << "OpenIter" << iters << ": socket " << handle->native_handle();

    parser_.emplace().body_limit(kuint64max);

    ec = SendRequestIterative(req, handle.get());

    if (!ec) {  // Success.
      https_handle_ = std::move(handle);
      return OnSuccess();
    }

    if (ec == asio::error::no_permission) {
      token_res = gce_.GetAccessToken(&pool_->io_context(), true);
      if (!token_res.ok())
        return token_res.status;

      req = PrepareRequest(token);
    } else if (ec != asio::error::try_again) {
      LOG(INFO) << "socket " << handle->native_handle() << " failed with error " << ec;
      handle.reset();
    }
  }

  return Status(StatusCode::IO_ERROR, "Maximum iterations reached");
}

auto GcsFileBase::SendRequestIterative(const EmptyRequest& req, HttpsClient* client) -> error_code {
  VLOG(1) << "Req: " << req;

  system::error_code ec = client->Send(req);
  if (ec)
    return ec;

  ec = client->ReadHeader(&parser_.value());

  if (ec) {
    return ec;
  }

  if (!parser_->keep_alive()) {
    client->schedule_reconnect();
    LOG(INFO) << "Scheduling reconnect due to conn-close header";
  }

  const auto& msg = parser_->get();
  VLOG(1) << "HeaderResp: " << msg;

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

  if (detail::DoesServerPushback(msg.result())) {
    this_fiber::sleep_for(1s);
    return asio::error::try_again;  // retry
  }

  if (detail::IsUnauthorized(msg)) {
    return asio::error::no_permission;
  }

  LOG(ERROR) << "Unexpected status " << msg;

  return h2::error::bad_status;
}

}  // namespace detail
}  // namespace util
