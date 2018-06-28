// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include <memory>
#include <functional>

struct event_base;

namespace util {

class Executor {
  class Rep;
  std::unique_ptr<Rep> rep_;

public:
  // if num_threads is 0, then Executor will choose number of threads automatically
  // based on the number of cpus in the system.
  explicit Executor(const std::string& name, bool use_periodic_handler = true);
  ~Executor();

  event_base* ebase();

  void Shutdown();

  typedef std::function<void()> Callback;
  typedef void* PeriodicHandler;

  // Adds a periodic event to the executor.
  PeriodicHandler AddPeriodicEvent(std::function<void()> f, unsigned msec,
                                   std::string name = std::string());

  void RemoveHandler(PeriodicHandler handler);

  // Blocks the calling thread until the event loop exits.
  // Some other thread should call ShutdownEventLoop for that to happen.
  void WaitForLoopToExit();

  void StopOnTermSignal();

  static Executor& Default();
  bool InEbaseThread() const;
};

}  // namespace util

#endif  // _EXECUTOR_H