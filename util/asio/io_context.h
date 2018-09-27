// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <thread>
#include <boost/asio/io_context.hpp>

namespace util {

class IoContext {
  friend class IoContextPool;
 public:
  using io_context = ::boost::asio::io_context;

  IoContext() : context_ptr_(std::make_shared<io_context>()) {}

  // We use shared_ptr because of the shared ownership with the fibers scheduler.
  typedef std::shared_ptr<io_context> ptr_t;

  void Stop() { context_ptr_->stop(); }

  io_context& get_context() { return *context_ptr_; }

  template<typename Func> void Post(Func&& f) {
    context_ptr_->post(std::forward<Func>(f));
  }

 private:

  ptr_t ptr() { return context_ptr_;}

  ptr_t context_ptr_;
  std::thread::id thread_id_;
};

}  // namespace util
