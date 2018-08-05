// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <vector>
#include <thread>

#include <boost/asio.hpp>
#include "base/pthread_utils.h"

namespace util {

/// A pool of io_context objects.
class IoContextPool {
public:
  IoContextPool(const IoContextPool&) = delete;
  void operator=(const IoContextPool&) = delete;

  // Construct the io_context pool.
  // pool_size = 0 provides pool size as number of cpus.
  explicit IoContextPool(std::size_t pool_size = 0);

  /// Runs all io_context objects in the pool and exits.
  void Run();

  /// Stop all io_context objects in the pool.
  void Stop();

  // Waits for all the threads to finish.
  // Requires that Run has been called.
  // Blocks the current thread until Stop is called and all the pool threads exited.
  void Join();

  /// Get an io_context to use.
  boost::asio::io_context& GetNextContext();

private:
  // We use shared_ptr because of the shared ownership with the fibers scheduler.
  typedef std::shared_ptr<boost::asio::io_context> io_context_ptr;

  std::vector<io_context_ptr> context_arr_;
  struct TInfo {
    pthread_t tid = 0;
    //bool loop_running = false;
  };

  std::vector<TInfo> thread_arr_;

  /// The next io_context to use for a connection.
  std::size_t next_io_context_ = 0;
  thread_local static size_t context_indx_;
};

}  // namespace util
