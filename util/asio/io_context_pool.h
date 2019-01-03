// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/fiber/fiber.hpp>
#include <experimental/optional>

#include "util/asio/io_context.h"
#include "util/fibers_ext.h"

namespace util {

/// A pool of io_context objects.
class IoContextPool {
 public:
  using io_context = ::boost::asio::io_context;

  IoContextPool(const IoContextPool&) = delete;
  void operator=(const IoContextPool&) = delete;

  // Construct the io_context pool.
  // pool_size = 0 provides pool size as number of cpus.
  explicit IoContextPool(std::size_t pool_size = 0);
  ~IoContextPool();

  /// Runs all io_context objects in the pool and exits.
  void Run();

  /// Stop all io_context objects in the pool.
  // Waits for all the threads to finish.
  // Requires that Run has been called.
  // Blocks the current thread until all the pool threads exited.
  void Stop();

  /// Get an io_context to use. Thread-safe.
  IoContext& GetNextContext();

  IoContext& operator[](size_t i) { return context_arr_[i]; }
  IoContext& at(size_t i) { return context_arr_[i]; }

  size_t size() const { return context_arr_.size(); }

  // Runs func in all IO threads asynchronously. The task must be CPU-only non IO-blocking code
  // because it runs directly in IO-fiber. MapTask runs asynchronously and will exit before
  // the task finishes. The 'func' must accept context as its argument.
  template <typename Func> void AsyncOnAll(Func&& func) {
    for (unsigned i = 0; i < size(); ++i) {
      IoContext& context = context_arr_[i];
      context.Async([&context, func = std::forward<Func>(func)]() mutable { func(context); });
    }
  }

  template <typename Func> void AwaitOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [&bc, func = std::forward<Func>(func)](IoContext& context) {
      func(context);
      bc.Dec();
    };
    AsyncOnAll(std::move(cb));
    bc.Wait();
  }

  // Runs `func` in a fiber asynchronously. func must accept IoContext&.
  // The callback runs inside a wrapping fiber.
  template <typename Func> void AsyncFiberOnAll(Func&& func) {
    AsyncOnAll([func = std::forward<Func>(func)](IoContext& context) {
      ::boost::fibers::fiber(func, std::ref(context)).detach();
    });
  }

  // Runs func in all IO threads in parallel, but waits for all the callbacks to finish.
  // The callback runs inside a wrapping fiber.
  template <typename Func> void AwaitFiberOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [&bc, func = std::forward<Func>(func)](IoContext& context) {
      func(context);
      bc.Dec();
    };
    AsyncFiberOnAll(std::move(cb));
    bc.Wait();
  }

 private:
  void WrapLoop(size_t index, fibers_ext::BlockingCounter* bc);

  typedef ::boost::asio::executor_work_guard<IoContext::io_context::executor_type> work_guard_t;

  std::vector<IoContext> context_arr_;
  struct TInfo {
    pthread_t tid = 0;
    std::experimental::optional<work_guard_t> work;
  };

  std::vector<TInfo> thread_arr_;

  /// The next io_context to use for a connection.
  std::atomic_uint_fast32_t next_io_context_{0};
  thread_local static size_t context_indx_;
  enum State { STOPPED, RUN } state_ = STOPPED;
};

}  // namespace util
