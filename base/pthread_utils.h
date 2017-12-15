// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _BASE_PTHREAD_UTILS_H
#define _BASE_PTHREAD_UTILS_H

#include <pthread.h>
#include <functional>
#include "base/logging.h"

#define PTHREAD_CHECK(x) \
  do { \
    int my_err = pthread_ ## x; \
    CHECK_EQ(0, my_err) << #x << ", error: " << strerror(my_err); \
  } while(false)

namespace base {

constexpr int kThreadStackSize = 65536;

void InitCondVarWithClock(clockid_t clock_id, pthread_cond_t* var);


pthread_t StartThread(const char* name, void *(*start_routine) (void *), void *arg);
pthread_t StartThread(const char* name, std::function<void()> f);

class PMutexGuard;

class Mutex {
  friend class PMutexGuard;
 public:
  Mutex() {}
  ~Mutex() { PTHREAD_CHECK(mutex_destroy(&mu_)); }
  void Lock() { PTHREAD_CHECK(mutex_lock(&mu_)); }
  void Unlock() { PTHREAD_CHECK(mutex_unlock(&mu_)); }
  bool TryLock() { return pthread_mutex_trylock(&mu_) == 0;}

 private:
  pthread_mutex_t mu_ = PTHREAD_MUTEX_INITIALIZER;
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;
};

class PMutexGuard {
  PMutexGuard(const PMutexGuard&) = delete;
  void operator=(const PMutexGuard&) = delete;
  pthread_mutex_t* m_;
public:
  PMutexGuard(Mutex& m) : m_(&m.mu_) {}

  PMutexGuard(pthread_mutex_t& m) : m_(&m) {
    PTHREAD_CHECK(mutex_lock(m_));
  }

  ~PMutexGuard() {
    PTHREAD_CHECK(mutex_unlock(m_));
  }
};


}  // namespace base

#endif  // _BASE_PTHREAD_UTILS_H