// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/rpc/impl/rpc_conn_handler.h"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "base/flags.h"
#include "base/logging.h"

#include "util/asio/asio_utils.h"
#include "util/asio/io_context.h"

namespace util {
namespace rpc {

using namespace std::chrono_literals;

namespace {

using RpcConnList = detail::slist<RpcConnectionHandler, RpcConnectionHandler::rpc_hook_t,
                                  detail::constant_time_size<false>, detail::cache_last<false>>;

class Flusher : public IoContext::Cancellable {
 public:
  Flusher() {}

  void Run() final;
  void Cancel() final;

  RpcConnList flush_conn_list;

 private:
  bool stop_ = false;
  fibers::condition_variable cv_;
  fibers::mutex mu_;
};

void Flusher::Run() {
  VLOG(1) << "Flusher start ";
  std::unique_lock<fibers::mutex> lock(mu_);
  uint64_t num_flushes = 0;

  while (!stop_) {
    cv_.wait_for(lock, 300us);

    for (auto it = flush_conn_list.begin(); it != flush_conn_list.end(); ++it) {
      num_flushes += uint64_t(it->PollAndFlushWrites());
    }
  }
  VLOG(1) << "Flusher exited " << num_flushes;
}

void Flusher::Cancel() {
  VLOG(1) << "Flusher::Cancel";
  {
    std::lock_guard<fibers::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
}

thread_local Flusher* flusher = nullptr;

}  // namespace

using asio::ip::tcp;
using fibers_ext::yield;

constexpr size_t kRpcPoolSize = 32;

RpcConnectionHandler::RpcConnectionHandler(ConnectionBridge* bridge, IoContext* context)
    : ConnectionHandler(context),
      bridge_(bridge), rpc_items_(kRpcPoolSize) {}

RpcConnectionHandler::~RpcConnectionHandler() {
  bridge_->Join();

  outgoing_buf_.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i); });
}

void RpcConnectionHandler::OnOpenSocket() {
  VLOG(1) << "OnOpenSocket: " << socket_->native_handle();

  if (!flusher) {
    flusher = new Flusher;
    io_context_.AttachCancellable(flusher);
  }
  flusher->flush_conn_list.push_front(*this);

  bridge_->InitInThread();
}

void RpcConnectionHandler::OnCloseSocket() {
  VLOG(1) << "OnCloseSocket: Before flush fiber join " << socket_->native_handle();
  DCHECK(io_context_.InContextThread());

  // To make sure  does not run. Since Flusher and RpcConnectionHandler fibers are
  // on the same thread, then either:
  // 1. Flusher is outside this->FlushWrites()- then we can just remove ourselves.
  //    and RpcConnList::iterator will be still valid where it point to.
  // 2. We are inside this->FlushWrites(). In that case we want to wait till it finishes to run.
  std::lock_guard<fibers::mutex> ul(wr_mu_);
  flusher->flush_conn_list.erase(RpcConnList::s_iterator_to(*this));

  VLOG(1) << "After flush fiber join " << req_flushes_;
}

system::error_code RpcConnectionHandler::HandleRequest() {
  VLOG(2) << "HandleRequest " << socket_->is_open() << " / "
          << (socket_->is_open() ? socket_->remote_endpoint(ec_) : tcp::endpoint());

  if (ec_)
    return ec_;

  rpc::Frame frame;
  ec_ = frame.Read(&socket_.value());
  if (ec_) {
    return ec_;
  }

  DCHECK_NE(-1, socket_->native_handle());

  if (rpc_items_.empty() && !outgoing_buf_.empty()) {
    req_flushes_ += FlushWrites();
  }

  // We use item for reading the envelope.
  RpcItem* item = rpc_items_.Get();

  item->envelope.Resize(frame.header_size, frame.letter_size);
  auto rbuf_seq = item->buf_seq();
  asio::read(*socket_, rbuf_seq, ec_);
  if (ec_) {
    VLOG(1) << "async_read " << ec_ << " /" << socket_->native_handle();
    return ec_;
  }
  DCHECK_NE(-1, socket_->native_handle());

  // To support streaming we have this writer that creq_flushes_an write multiple envelopes per
  // single rpc request. We pass captures by value to allow asynchronous invocation
  // of ConnectionBridge::HandleEnvelope. We move writer object into HandleEnvelope,
  // thus it will be responsible to own it until the handler finishes.
  // Please note that writer changes the value of 'item' field (it's mutable),
  // so only for the first outgoing envelope it uses the same RpcItem used for reading the data
  // to reduce allocations.
  auto writer = [rpc_id = frame.rpc_id, item, this](Envelope&& env) mutable {
    RpcItem* next = item ? item : rpc_items_.Get();

    next->envelope = std::move(env);
    next->id = rpc_id;
    outgoing_buf_.push_back(*next);

    // after the first use we can not anymore, because it's sent to the outgoing_buf_
    item = nullptr;
  };

  // Might by asynchronous, depends on the bridge_.
  bridge_->HandleEnvelope(frame.rpc_id, &item->envelope, std::move(writer));

  return ec_;
}

bool RpcConnectionHandler::FlushWrites() {
  // Serves as critical section. We can not allow interleaving writes into the socket.
  // If another fiber flushes - we just exit without blocking.
  std::unique_lock<fibers::mutex> ul(wr_mu_, std::try_to_lock_t{});
  if (!ul || outgoing_buf_.empty() || !socket_->is_open())
    return false;

  VLOG(2) << "FlushWritesGuarded: " << outgoing_buf_.size();
  size_t count = outgoing_buf_.size();
  write_seq_.resize(count * 3);
  frame_buf_.resize(count);

  ItemList tmp;
  size_t item_index = 0;
  for (RpcItem& item : outgoing_buf_) {  // iterate over intrusive list.
    Frame f(item.id, item.envelope.header.size(), item.envelope.letter.size());

    uint8_t* buf = frame_buf_[item_index].data();
    size_t frame_sz = f.Write(buf);
    write_seq_[3 * item_index] = asio::buffer(buf, frame_sz);
    write_seq_[3 * item_index + 1] = asio::buffer(item.envelope.header);
    write_seq_[3 * item_index + 2] = asio::buffer(item.envelope.letter);
    ++item_index;
  }
  tmp.swap(outgoing_buf_);

  size_t write_sz = asio::write(*socket_, write_seq_, ec_);

  // We should use clear_and_dispose to delete items safely while unlinking them from tmp.
  tmp.clear_and_dispose([this](RpcItem* i) { rpc_items_.Release(i); });

  VLOG(2) << "Wrote " << count << " requests with " << write_sz << " bytes";
  return true;
}

}  // namespace rpc
}  // namespace util
