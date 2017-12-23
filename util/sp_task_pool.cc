// Copyright 2015, .com .  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/sp_task_pool.h"

#include <thread>

#include "base/logging.h"
#include "base/pthread_utils.h"
#include "base/walltime.h"
// #include "util/proc_stats.h"

namespace util {


namespace internal {

void SingleProducerTaskPoolBase::ThreadInfo::Join() {
  if (d.thread_id) {
    pthread_cancel(d.thread_id);
    PTHREAD_CHECK(join(d.thread_id, nullptr));
    d.thread_id = 0;
  }
}

struct SingleProducerTaskPoolBase::RoutineConfig {
  SingleProducerTaskPoolBase* me;
  unsigned thread_index;
};

SingleProducerTaskPoolBase::ThreadLocalInterface::~ThreadLocalInterface() {}

SingleProducerTaskPoolBase::SingleProducerTaskPoolBase(
  std::string name, unsigned queue_capacity, unsigned int num_threads)
    : base_name_(std::move(name)), per_thread_capacity_(queue_capacity) {
  CHECK_GE(queue_capacity, 2);

  start_cancel_ = false;

  if (num_threads == 0) {
    uint32 num_cpus = std::thread::hardware_concurrency();
    if (num_cpus == 0)
      num_threads = 2;
    else
      num_threads = num_cpus * 2;
  }
  VLOG(1) << "TaskPool " << base_name_ << " with " << num_threads << " threads";
  thread_count_ = num_threads;
}

SingleProducerTaskPoolBase::~SingleProducerTaskPoolBase() {
  JoinThreads();

  for (auto t : thread_interfaces_) delete t;
}

void SingleProducerTaskPoolBase::LaunchThreads() {
  CHECK(threads_.empty());

  threads_.resize(thread_count_);

  char buf[16];
  for (unsigned i = 0; i < threads_.size(); ++i) {
    snprintf(buf, sizeof(buf), "%s%d", base_name_.c_str(), i);

    threads_[i].d.thread_id = base::StartThread(buf, ThreadRoutine, new RoutineConfig{this, i});
  }
}

void SingleProducerTaskPoolBase::JoinThreads() {
  start_cancel_ = true;
  for (auto& t : threads_) {
    t.Wake();
    t.Join();
  }
}


unsigned SingleProducerTaskPoolBase::FindMostFreeThread() const {
  // Give each thread a score according to his queue size and if its runnning task.
  // Rerun thread index with lowest score.
  uint32 min_score = kuint32max;
  unsigned index = 0;
  for (unsigned i = 0; i < threads_.size(); ++i) {
    uint32 score = thread_interfaces_[i]->QueueSize();
    if (threads_[i].d.has_tasks) {
      ++score;
    }
    if (score == 0) {
      return i;  // If found thread with score 0 return this thread index.
    }
    if (score < min_score) {
      index = i;
      min_score = score;
    }
  }
  return index;
}


void SingleProducerTaskPoolBase::WaitForTasksToComplete() {
  // We assuming that producer thread stopped enqueing tasks.
  for (unsigned i = 0; i < threads_.size(); ++i) {
    const ThreadLocalInterface* tli = thread_interfaces_[i];
    ThreadInfo::Data& d = threads_[i].d;

    d.ev_task_finished.await([tli, &d] { return tli->IsQueueEmpty() && !d.has_tasks; });
  }
  VLOG(1) << "WaitForTasksToComplete finished";
}

unsigned SingleProducerTaskPoolBase::QueueSize() const {
  unsigned res = 0;
  for (unsigned i = 0; i < threads_.size(); ++i) {
    res = std::max(res, thread_interfaces_[i]->QueueSize());
  }
  return res;
}

uint64 SingleProducerTaskPoolBase::AverageDelayUsec() const {
  uint64 jiffies = 0;
  uint64 count = 0;
  for (unsigned i = 0; i < threads_.size(); ++i) {
    const ThreadLocalInterface* tli = thread_interfaces_[i];
    jiffies += tli->queue_delay_jiffies;
    count += tli->queue_delay_count;
  }

  return count ? jiffies * 100 / count : 0;
}

void* SingleProducerTaskPoolBase::ThreadRoutine(void* arg) {
  RoutineConfig* config = (RoutineConfig*)arg;
  SingleProducerTaskPoolBase* me = config->me;
  ThreadInfo::Data& ti = me->threads_[config->thread_index].d;

  ThreadLocalInterface* thread_interface = me->thread_interfaces_[config->thread_index];

  delete config;
  config = nullptr;
  auto await_check = [me, thread_interface]() {
    return me->start_cancel_ || !thread_interface->IsQueueEmpty();
  };

  unsigned num_yields = 0;
  while (!me->start_cancel_) {
    ti.has_tasks.store(true, std::memory_order_release);
    while (thread_interface->RunTask()) {
      num_yields = 0;
    }
    ti.has_tasks.store(false, std::memory_order_release);

    if (++num_yields > 100) {
      // We iterate 1000 times before even bother to sleep.

      if (thread_interface->IsQueueEmpty()) {
        VLOG(2) << "ti.empty_q_cv.notify";

        ti.ev_task_finished.notify();
        ti.ev_non_empty.await(await_check);
      }
    }
  }
  char buf[30] = {0};
  pthread_getname_np(pthread_self(), buf, sizeof buf);
  VLOG(1) << "Finishing running SingleProducerTaskPoolBase thread " << buf;

  return NULL;
}

}  // namespace internal

}  // namespace util
