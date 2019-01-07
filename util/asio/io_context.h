// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <boost/asio/io_context.hpp>
#include <thread>

#include "util/fibers_ext.h"

namespace util {

class IoFiberProperties : public boost::fibers::fiber_properties {
 public:
  constexpr static unsigned MAX_NICE_LEVEL = 5;
  constexpr static unsigned NUM_NICE_LEVELS = MAX_NICE_LEVEL + 1;

  IoFiberProperties(::boost::fibers::context* ctx) : fiber_properties(ctx), nice_(3) {}

  unsigned nice_level() const { return nice_; }

  // Call this method to alter nice, because we must notify
  // nice_scheduler of any change.
  // Currently supported levels are 0-MAX_NICE_LEVEL.
  // 0 - has the highest responsiveness and MAX_NICE_LEVEL has the least.
  // Values higher than MAX_NICE_LEVEL will be set to MAX_NICE_LEVEL.
  void SetNiceLevel(unsigned p);

  void set_name(std::string nm) { name_ = std::move(nm); }

  const std::string& name() const { return name_; }

 private:
  std::string name_;
  unsigned nice_;
};

namespace detail {

template <typename R> class ResultMover {
  R r_;  // todo: to set as optional to support objects without default c'tor.
 public:
  template <typename Func> void Apply(Func&& f) { r_ = f(); }

  // Returning rvalue-reference means returning the same object r_ instead of creating a
  // temporary R{r_}. Please note that when we return function-local object, we do not need to
  // return rvalue because RVO eliminates redundant object creation.
  // But for returning data member r_ it's more efficient.
  // "get() &&" means you can call this function only on rvalue ResultMover&& object.
  R&& get() && { return std::forward<R>(r_); }
};

template <> class ResultMover<void> {
 public:
  template <typename Func> void Apply(Func&& f) { f(); }
  void get() {}
};

}  // namespace detail

namespace asio_ext {
// Runs `f` asynchronously in io-context fiber. `f` should not block, lock on mutexes or Await.
// Spinlocks are ok but might cause performance degradation.
template <typename Func> void Async(::boost::asio::io_context& cntx, Func&& f) {
  cntx.post(std::forward<Func>(f));
}

// Similarly to Async(), runs 'f' in io_context thread, but waits for it to finish by blocking
// the calling fiber. `f` should not block because it runs directly from IO loop.
template <typename Func>
auto Await(::boost::asio::io_context& cntx, Func&& f) -> decltype(f()) {
  fibers_ext::Done done;
  using ResultType = decltype(f());
  detail::ResultMover<ResultType> mover;

  Async(cntx, [&, f = std::forward<Func>(f)]() mutable {
    mover.Apply(f);
    done.Notify();
  });

  done.Wait();
  return std::move(mover).get();
}

};  // namespace asio_ext

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

  void Stop();

  io_context& raw_context() { return *context_ptr_; }

  template <typename Func> void Async(Func&& f) {
    asio_ext::Async(*context_ptr_, std::forward<Func>(f));
  }

  template <typename Func, typename... Args> void AsyncFiber(Func&& f, Args&&... args) {
    // Ideally we want to forward args into lambda but it's too complicated before C++20.
    // So I just copy them into capture.
    // We forward captured variables so we need lambda to be mutable.
    Async([f = std::forward<Func>(f), args...]() mutable {
      ::boost::fibers::fiber(std::forward<Func>(f), std::forward<Args>(args)...).detach();
    });
  }

  // Similar to asio_ext::Await(), but if we call Await from the context thread,
  // runs `f` directly (minor optimization).
  template <typename Func> auto Await(Func&& f) -> decltype(f()) {
    if (InContextThread()) {
      return f();
    }
    return asio_ext::Await(*context_ptr_, std::forward<Func>(f));
  }

  // Please note that this function uses Await, therefore can not be used inside Ring0
  // (i.e. Async callbacks).
  template <typename... Args> boost::fibers::fiber LaunchFiber(Args&&... args) {
    ::boost::fibers::fiber fb;
    // It's safe to use & capture since we await before returning.
    Await([&] { fb = boost::fibers::fiber(std::forward<Args>(args)...); });
    return fb;
  }

  // Runs possibly awating function 'f' safely in ContextThread and waits for it to finish,
  // If we are in the context thread already, runs 'f' directly, otherwise
  // runs it wrapped in a fiber. Should be used instead of 'Await' when 'f' itself
  // awaits on something.
  // To summarize: 'f' should not block its thread, but allowed to block its fiber.
  template <typename Func> auto AwaitSafe(Func&& f) -> decltype(f()) {
    if (InContextThread()) {
      return f();
    }

    using ResultType = decltype(f());
    detail::ResultMover<ResultType> mover;
    auto fb = LaunchFiber([&] { mover.Apply(std::forward<Func>(f)); });
    fb.join();

    return std::move(mover).get();
  }

  auto get_executor() { return context_ptr_->get_executor(); }

  bool InContextThread() const { return std::this_thread::get_id() == thread_id_; }

  // Attaches user processes that should live along IoContext. IoContext will shut them down via
  // Cancel() call right before closing its IO loop.
  // Takes ownership over Cancellable runner. Runs it in a dedicated fiber in IoContext thread.
  // During the shutdown process signals the object to cancel by running Cancellable::Cancel()
  // method.
  void AttachCancellable(Cancellable* obj) {
    auto fb = LaunchFiber([obj] { obj->Run(); });
    cancellable_arr_.emplace_back(
        CancellablePair{std::unique_ptr<Cancellable>(obj), std::move(fb)});
  }

 private:
  void StartLoop(fibers_ext::BlockingCounter* bc);

  using CancellablePair = std::pair<std::unique_ptr<Cancellable>, ::boost::fibers::fiber>;

  ptr_t context_ptr_;
  std::thread::id thread_id_;
  std::vector<CancellablePair> cancellable_arr_;
};

}  // namespace util
