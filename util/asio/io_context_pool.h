// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <thread>
#include <vector>

#include <absl/types/optional.h>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/fiber/fiber.hpp>

#include "base/type_traits.h"
#include "util/asio/io_context.h"
#include "util/fibers/fibers_ext.h"

namespace util {

/** @brief A pool of IoContext objects, representing and managing CPU resources of the system.
 *  @author Roman Gershman
 *
 *  The main entry for launching asynchronous processes across all IO threads. For single thread
 *  manager see IoContext class.
 */
class IoContextPool {
  template <typename Func, typename... Args>
  using AcceptArgsCheck =
      typename std::enable_if<base::is_invocable<Func, Args...>::value, int>::type;

 public:
  using io_context = ::boost::asio::io_context;

  IoContextPool(const IoContextPool&) = delete;
  void operator=(const IoContextPool&) = delete;

  //! Constructs io_context pool with number of threads equal to 'pool_size'.
  //! pool_size = 0 chooses automatically pool size equal to number of cores in the system.
  explicit IoContextPool(std::size_t pool_size = 0);

  ~IoContextPool();

  //! Starts running all IoContext objects in the pool. Does not block.
  void Run();

  /*! @brief Stops all io_context objects in the pool.
   *
   *  Waits for all the threads to finish. Requires that Run has been called.
   *  Blocks the current thread until all the pool threads exited.
   */
  void Stop();

  //! Get an io_context to use. Thread-safe.
  IoContext& GetNextContext();

  IoContext& operator[](size_t i) { return context_arr_[i]; }
  IoContext& at(size_t i) { return context_arr_[i]; }

  size_t size() const { return context_arr_.size(); }

  /*! @brief Runs func in all IO threads asynchronously.
   *
   * The task must be CPU-only non IO-blocking code because it runs directly in IO-fiber.
   * MapTask runs asynchronously and will exit before  the task finishes.
   * The 'func' must accept IoContext& as its argument.
   */
  template <typename Func, AcceptArgsCheck<Func, IoContext&> = 0> void AsyncOnAll(Func&& func) {
    for (unsigned i = 0; i < size(); ++i) {
      IoContext& context = context_arr_[i];
      // func must be copied, it can not be moved, because we dsitribute it into multiple
      // IoContexts.
      context.Async([&context, func] () mutable { func(context); });
    }
  }

  /*! @brief Runs func in all IO threads asynchronously.
   *
   * The task must be CPU-only non IO-blocking code because it runs directly in IO-fiber.
   * MapTask runs asynchronously and will exit before  the task finishes.
   * The 'func' must accept unsigned int (io context index) and IoContext& as its arguments.
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, IoContext&> = 0>
  void AsyncOnAll(Func&& func) {
    for (unsigned i = 0; i < size(); ++i) {
      IoContext& context = context_arr_[i];
      // Copy func on purpose, see above.
      context.Async([&context, i, func] () mutable { func(i, context); });
    }
  }

  /**
   * @brief Runs func in all IO threads asynchronously. Blocks until all the asynchronous calls to func return.
   *
   * Func must accept IoContext&.
   */
  template <typename Func, AcceptArgsCheck<Func, IoContext&> = 0> void AwaitOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](IoContext& context) mutable {
      func(context);
      bc.Dec();
    };
    AsyncOnAll(std::move(cb));
    bc.Wait();
  }

  /**
   * @brief Blocks until all the asynchronous calls to func return. Func receives both the index
   * and IoContext&. func must not block.
   *
   */
  template <typename Func, AcceptArgsCheck<Func, unsigned, IoContext&> = 0>
  void AwaitOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](unsigned index, IoContext& context) mutable {
      func(index, context);
      bc.Dec();
    };
    AsyncOnAll(std::move(cb));
    bc.Wait();
  }

  /**
   * @brief Runs `func` in a fiber asynchronously. func must accept IoContext&.
   *        func may fiber-block.
   *
   * @param func
   *
   * 'func' callback runs inside a wrapping fiber.
   */
  template <typename Func> void AsyncFiberOnAll(Func&& func) {
    AsyncOnAll([func = std::forward<Func>(func)](IoContext& context) {
      ::boost::fibers::fiber(func, std::ref(context)).detach();
    });
  }

  /**
   * @brief Runs `func` wrapped in fiber on all IO threads in parallel. func must accept IoContext&.
   *        func may fiber-block.
   *
   * @param func
   *
   * Waits for all the callbacks to finish.
   */
  template <typename Func> void AwaitFiberOnAll(Func&& func) {
    fibers_ext::BlockingCounter bc(size());
    auto cb = [func = std::forward<Func>(func), bc](IoContext& context) mutable {
      func(context);
      bc.Dec();
    };
    AsyncFiberOnAll(std::move(cb));
    bc.Wait();
  }

  IoContext* GetThisContext();

 private:
  void WrapLoop(size_t index, fibers_ext::BlockingCounter* bc);

  typedef ::boost::asio::executor_work_guard<IoContext::io_context::executor_type> work_guard_t;

  std::vector<IoContext> context_arr_;
  struct TInfo {
    pthread_t tid = 0;
    absl::optional<work_guard_t> work;
  };

  std::vector<TInfo> thread_arr_;

  /// The next io_context to use for a connection.
  std::atomic_uint_fast32_t next_io_context_{0};
  thread_local static size_t context_indx_;
  enum State { STOPPED, RUN } state_ = STOPPED;
};

}  // namespace util
