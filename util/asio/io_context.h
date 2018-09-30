// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <thread>
#include <boost/asio/io_context.hpp>
#include "util/fibers_ext.h"

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

  bool InContextThread() const { return std::this_thread::get_id() == thread_id_; }

  // Runs 'f' in Io Context thread and waits for it to finish,
  // therefore might block the current fiber.
  // If we run in context thread - runs 'f' directly.
  template<typename Func> void PostSynchronous(Func&& f) {
    if (InContextThread()) {
      return f();
    }
    fibers_ext::Done done;
    Post([f = std::forward<Func>(f), &done] { f(); done.Notify();});
    done.Wait();
  }

 private:

  void StartLoop();

  ptr_t ptr() { return context_ptr_;}

  ptr_t context_ptr_;
  std::thread::id thread_id_;
};

}  // namespace util
