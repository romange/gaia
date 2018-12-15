// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <thread>
#include <boost/asio/io_context.hpp>
#include "util/fibers_ext.h"

namespace util {

class IoFiberProperties : public boost::fibers::fiber_properties {
 public:
  constexpr static unsigned MAX_NICE_LEVEL = 5;
  constexpr static unsigned NUM_NICE_LEVELS = MAX_NICE_LEVEL + 1;

  IoFiberProperties(::boost::fibers::context* ctx) : fiber_properties(ctx), nice_(3) {
  }

  unsigned nice_level() const {
    return nice_;
  }

  // Call this method to alter nice, because we must notify
  // nice_scheduler of any change.
  // Currently supported levels are 0-MAX_NICE_LEVEL.
  // 0 - has the highest responsiveness and MAX_NICE_LEVEL has the least.
  // Values higher than MAX_NICE_LEVEL will be set to MAX_NICE_LEVEL.
  void SetNiceLevel(unsigned p);

  void set_name(std::string nm) {
    name_ = std::move(nm);
  }

  const std::string& name() const { return name_;}

 private:
  std::string name_;
  unsigned nice_;
};

class IoContext {
  friend class IoContextPool;
 public:
  using io_context = ::boost::asio::io_context;

  class Cancellable {
   public:
    virtual ~Cancellable() {}

    virtual void Run() = 0;
    virtual void Cancel() = 0;
  };

  IoContext() : context_ptr_(std::make_shared<io_context>()) {}

  // We use shared_ptr because of the shared ownership with the fibers scheduler.
  typedef std::shared_ptr<io_context> ptr_t;

  void Stop() { context_ptr_->stop(); }

  io_context& get_context() { return *context_ptr_; }

  // Runs `f` asynchronously in io-context fiber. `f` should not block or lock on mutexes.
  template<typename Func> void Post(Func&& f) {
    context_ptr_->post(std::forward<Func>(f));
  }

  template<typename Func> void PostFiber(Func&& f) {
    Post([f = std::forward<Func>(f)] {
      ::boost::fibers::fiber(f).detach();
    });
  }

  // Similarly to Post(), runs 'f' in Io Context thread, but waits for it to finish by blocking
  // the current fiber. If we call PostSynchronous from the context thread,
  // runs `f` directly. `f` should not block because since it runs directly in IO loop.
  template<typename Func> void PostSynchronous(Func&& f) {
    if (InContextThread()) {
      return f();
    }

    fibers_ext::Done done;
    Post([f = std::forward<Func>(f), &done] () mutable {
      f();
      done.Notify();
    });

    done.Wait();
  }

  template<typename Func> void PostFiberSync(Func&& f) {
    if (InContextThread()) {
      return f();
    }

    fibers_ext::Done done;
    auto cb = [f = std::forward<Func>(f), &done] () mutable {
      f();
      done.Notify();
    };
    PostFiber(std::move(cb));
    done.Wait();
  }

  auto get_executor() { return context_ptr_->get_executor(); }

  bool InContextThread() const { return std::this_thread::get_id() == thread_id_; }

  // Attaches user processes that should live along IoContext. IoContext will shut them down via
  // Cancel() call right before closing its IO loop.
  // Takes ownership over Cancellable runner. Runs it in a dedicated fiber in IoContext thread.
  // During the shutdown process signals the object to cancel by running Cancellable::Cancel()
  // method.
  void AttachCancellable(Cancellable* obj) {
    PostSynchronous([obj, this] () mutable {
      CancellablePair pair(std::unique_ptr<Cancellable>(obj), [obj] { obj->Run(); });
      cancellable_arr_.emplace_back(std::move(pair));
    });
  }

 private:
  void StartLoop(fibers_ext::BlockingCounter* bc);

  using CancellablePair = std::pair<std::unique_ptr<Cancellable>, ::boost::fibers::fiber>;

  ptr_t context_ptr_;
  std::thread::id thread_id_;
  std::vector<CancellablePair> cancellable_arr_;
};

}  // namespace util
