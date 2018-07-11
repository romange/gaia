// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <vector>
#include <thread>

#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>

#include "base/pthread_utils.h"

/// A pool of io_context objects.
class IoContextPool {
public:
  IoContextPool(const IoContextPool&) = delete;
  void operator=(const IoContextPool&) = delete;

  /// Construct the io_context pool.
  explicit IoContextPool(std::size_t pool_size);

  /// Runs all io_context objects in the pool and exits.
  void Run();

  /// Stop all io_context objects in the pool.
  void Stop();

  // Waits for all the threads to finish.
  // Requires that Run has been called.
  // Blocks the current thread until Stop is called and all the pool threads exited.
  void Join();

  /// Get an io_context to use.
  boost::asio::io_context& GetIoContext();

private:

  typedef std::shared_ptr<boost::asio::io_context> io_context_ptr;

  std::vector<io_context_ptr> context_arr_;
  struct TInfo {
    pthread_t tid = 0;
    bool loop_running = false;
  };

  std::vector<TInfo> thread_arr_;

  /// The next io_context to use for a connection.
  std::size_t next_io_context_ = 0;
};
