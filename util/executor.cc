// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/executor.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <event2/event.h>
#include <event2/thread.h>
#include <signal.h>

#include "base/init.h"
#include "base/logging.h"
#include "base/pthread_utils.h"
#include "base/walltime.h"
#include "util/sp_task_pool.h"

using std::string;
using base::GetThreadTime;

namespace util {

namespace {

std::once_flag init_module;
static Executor* signal_executor_instance = nullptr;

// This map is a histogram for periodic event skipped_runs variable.
// The keys are the current skipped runs counts in periodic events,
// values are - how many times those keys present in periodic events.
// It allows to pull efficiently the the highest key or to reset/upgrade each key.
static std::map<uint32, uint32> hang_times_values_to_count;
static std::mutex hang_times_mu;

static void UpdateMaxSkipRuns(uint32 old_value, uint32 new_value) {
  if (old_value == 0 && new_value == 0) {
    return;
  }
  std::lock_guard<std::mutex> lg_varz(hang_times_mu);

  // Update new value count in map.
  if (new_value != 0) {
    auto k_v = hang_times_values_to_count.emplace(new_value, 1);
    if (k_v.second == false) {
      ++hang_times_values_to_count.at(new_value);
    }
  }

  // Update old value count in map.
  if (old_value != 0) {
    auto it = hang_times_values_to_count.find(old_value);
    if (it->second == 1) {
      hang_times_values_to_count.erase(it);
    } else {
      --it->second;
    }
  }
}

static void ExecutorSigHandler(int sig, siginfo_t *info, void *secret) {
  LOG(INFO) << "Catched signal " << sig << ": " << strsignal(sig);
  if (signal_executor_instance) {
    signal_executor_instance->Shutdown();
  }
}

void PrintLogMsg(int severity, const char *msg) {
  switch(severity) {
    case EVENT_LOG_DEBUG:
    case EVENT_LOG_MSG:
      LOG(INFO) << "ebase: " << msg;
    break;
    case EVENT_LOG_WARN:
      LOG(WARNING) << "ebase: " << msg;
    break;
    default:
      LOG(ERROR) << "ebase: " << msg;
    break;
  }
}

}  // namespace

class Executor::Rep {
  event_base* base_ = nullptr;

  pthread_t event_loop_thread_;
  std::condition_variable shut_down_cond_;
  std::mutex mutex_;
  bool shut_down_;

  std::atomic_bool start_cancel_;  // signals worker threads that they should stop running.

  struct PeriodicItem {
    Executor::Callback cb;
    Executor::Rep* me;
    event* ev;
    std::string name;
    std::atomic_bool is_running, is_deleted;
    unsigned skipped_runs = 0;
    std::mutex mu;
    std::condition_variable not_running_cv;

    PeriodicItem(Executor::Callback c, Executor::Rep* m, std::string nm)
      : cb(std::move(c)), me(m), ev(nullptr), name(std::move(nm)) {
      is_running = false;
      is_deleted = false;
    }
  };

  std::vector<PeriodicItem*> items_;

  static void* RunEventBase(void* me);

  static void PeriodicCb(evutil_socket_t fd, short what, void *ptr);

  struct RunCbTask {
    // typedef int SharedData;
    void operator()(PeriodicItem* item);
  };

  using TaskPool = SingleProducerTaskPool<RunCbTask, PeriodicItem*>;

  string name_;
  std::unique_ptr<TaskPool> task_pool_;

  struct ThreadTimeBucket {
    MicrosecondsInt64 thread_time, wall_time;

    double load() const { return wall_time > 0 ? 100.0 * double(thread_time) / wall_time : 0; }

    ThreadTimeBucket& operator+=(const ThreadTimeBucket& s) {
      thread_time += s.thread_time;
      wall_time += s.wall_time;
      return *this;
  }
  };
  static constexpr int BUCKET_NUM = 5;
  std::array<ThreadTimeBucket, BUCKET_NUM> thread_time_stats_;

 public:
  explicit Rep(const string& name, bool use_periodic);

  ~Rep();

  event_base* base() { return base_; }
  pthread_t event_loop_thread() const { return event_loop_thread_;}

  void StartCancel();

  bool was_cancelled() const { return start_cancel_; }

  void WaitShutdown() {
    std::unique_lock<std::mutex> lg(mutex_);

    // We do not use pthread_join because it can not be used from multiple threads.
    // Here we allow the flexibility for several threads to wait for the loop to exit.
    while (!shut_down_) {
      shut_down_cond_.wait(lg);
    }
    if (task_pool_) {
      task_pool_->WaitForTasksToComplete();
      task_pool_.reset();
    }
  }

  void RemoveHandler(Executor::PeriodicHandler handler);

  PeriodicHandler AddPeriodicEvent(Executor::Callback cb, unsigned msec, std::string name);
 private:
  static void SetupOnce();

  // Note that this pattern repeated itself in varz_stats.h
  // Need to think on how to refactor abstaction of the intrusive linked list.
  static Executor::Rep* & global_list();

  ThreadTimeBucket GetTotalTime() const;

  Executor::Rep* next_;
  Executor::Rep* prev_;
};
Executor::Rep* & Executor::Rep::global_list() {
  static Executor::Rep* global_list = nullptr;
  return global_list;
}

void* Executor::Rep::RunEventBase(void* arg) {
  Executor::Rep* me = (Executor::Rep*)arg;
  LOG(INFO) << "Starting EventBase thread " << me->name_;

  int res;
  constexpr int kFlags = EVLOOP_ONCE;

  int64 start = base::GetMonotonicMicrosFast();
  MicrosecondsInt64 thread_cpu_time = GetThreadTime();
  unsigned cur_stats_index = 0;
  unsigned idle_cnt = 0;
  while ((res = event_base_loop(me->base_, kFlags)) >= 0) {
    if (me->start_cancel_)
      break;
    VLOG(1) << "event loop: " << res;
    if (res == 1) {  // No events were registered. Lets sleep.
      if (++idle_cnt > 10) {
        base::SleepMicros(idle_cnt < 100 ? idle_cnt*10 : 1000);
  }
    } else {
      idle_cnt = 0;
    }
    int64 end = base::GetMonotonicMicrosFast();

    if (end - start > base::kNumMicrosPerSecond * 500) {
      // At least after 500ms update cpu stats
      MicrosecondsInt64 tend = GetThreadTime();
      ThreadTimeBucket tt{tend - thread_cpu_time, end - start};
      if (tt.thread_time > (end - start)*2) {
        LOG_FIRST_N(WARNING, 20) << "Got thread time: " << tt.thread_time
                                 << ", wall time: " << tt.wall_time;
      }

      me->thread_time_stats_[cur_stats_index] = tt;

      if (++cur_stats_index >= BUCKET_NUM) {
        cur_stats_index = 0;
      }

      thread_cpu_time = tend;
      start = end;
    }
  }

  std::lock_guard<std::mutex> lg(me->mutex_);
  VLOG(1) << "Finished running event_base_dispatch with res: " << res << " " << me->name_;
  me->shut_down_ = true;
  me->shut_down_cond_.notify_all();

  return NULL;
}

void Executor::Rep::RunCbTask::operator()(PeriodicItem* item) {
  if (!item->is_deleted) {
    item->cb();
  }
  std::lock_guard<std::mutex> lg(item->mu);
  item->is_running = false;
  item->not_running_cv.notify_one();
}

void Executor::Rep::PeriodicCb(evutil_socket_t fd, short what, void *ptr) {
  PeriodicItem* item = (PeriodicItem*)ptr;
  {
    std::lock_guard<std::mutex> lg(item->mu);
  if (item->is_deleted) {
    evtimer_del(item->ev);
      UpdateMaxSkipRuns(item->skipped_runs, 0);
    return;
  }
  if (item->is_running) {
    if (++item->skipped_runs > 3) {
      LOG(ERROR) << "Task " << item->name << " hangs " << item->skipped_runs << " times";
    }
      UpdateMaxSkipRuns((item->skipped_runs-1), item->skipped_runs);
    return;
  }

  item->is_running = true;
  }
  Executor::Rep* me = item->me;
  if (me->task_pool_->TryRunTask(item)) {
    UpdateMaxSkipRuns(item->skipped_runs, 0);
    item->skipped_runs = 0;
  } else {
    LOG(ERROR) << "Could not add task " << item->name;
    item->is_running = false;
    ++item->skipped_runs;
    UpdateMaxSkipRuns((item->skipped_runs-1), item->skipped_runs);
  }
}

void Executor::Rep::SetupOnce() {
  event_set_log_callback(PrintLogMsg);
  LOG(INFO) << "Using libevent version: " << event_get_version();
}


static std::mutex rep_list_mutex;


Executor::Rep::Rep(const string& name, bool use_periodic) : name_(name) {
  std::call_once(init_module, &SetupOnce);

  shut_down_ = false;
  start_cancel_ = false;

  base_ = CHECK_NOTNULL(event_base_new());

  string thread_name = name + "Base";
  event_loop_thread_ = base::StartThread(thread_name.c_str(), Executor::Rep::RunEventBase, this);

  if (use_periodic) {
    string task_pool_name = name + "tp";
    task_pool_.reset(new TaskPool(task_pool_name.c_str(), 5));
    task_pool_->Launch();
  }

  std::lock_guard<std::mutex> guard(rep_list_mutex);

  next_ = global_list();
  if (next_) {
    next_->prev_ = this;
  }
  global_list() = this;
}


Executor::Rep::~Rep() {
  StartCancel();
  WaitShutdown();

  for (auto item : items_) event_free(item->ev);
  event_base_free(base_);

  for (auto item : items_)
    delete item;

  std::lock_guard<std::mutex> guard(rep_list_mutex);
  if (global_list() == this) {
    global_list() = next_;
  } else {
    if (next_) {
      next_->prev_ = prev_;
    }
    if (prev_) {
      prev_->next_ = next_;
    }
  }
}

void Executor::Rep::StartCancel() {
  for (auto item : items_)
    evtimer_del(item->ev);

  start_cancel_ = true;

  event_base_loopexit(base_, NULL); // signal to exit.
}

auto Executor::Rep::AddPeriodicEvent(std::function<void()> f, unsigned msec,
                                     std::string name) -> PeriodicHandler {
  PeriodicItem* item = new PeriodicItem{std::move(f), this, std::move(name)};

  event* ev = event_new(base(), -1, EV_PERSIST | EV_TIMEOUT, &PeriodicCb, item);
  item->ev = ev;
  timeval tv = {msec / 1000, (msec % 1000) * 1000};

  items_.push_back(item);

  CHECK_EQ(0, event_add(ev, &tv));

  return item;
}

void Executor::Rep::RemoveHandler(PeriodicHandler handler) {
  if (handler == nullptr)
    return;
  PeriodicItem* item = (PeriodicItem*)handler;
  VLOG(1) << "Removing item " << item << " " << item->name;

  std::unique_lock<std::mutex> lg(item->mu);
  item->is_deleted = true;

  // It could be that is_running is not yet set in PeriodicCb function for this item
  // but it's going to do it. In that case when know that the task did not run yet and it will
  // skip the call to item->cb because we just set is_deleted to true.

  // So we do this loop-wait only for the case when the other thread is calling
  // in RunCbTask::operator() item->cb. We want to verify that the callback has finished running
  // before returning.
  while (item->is_running) {
    item->not_running_cv.wait(lg);
  }
  item->cb = nullptr;
  VLOG(1) << "Item " << item->name << " was removed";
}

auto Executor::Rep::GetTotalTime() const -> ThreadTimeBucket {
  ThreadTimeBucket res{0, 0};
  for (const auto& t : thread_time_stats_) {
    res += t;
  }
  return res;
}

Executor::Executor(const std::string& name, bool use_periodic_handler) {
  rep_.reset(new Rep(name, use_periodic_handler));
}

Executor::~Executor() {

}

event_base* Executor::ebase() {
  return rep_->base();
}

auto Executor::AddPeriodicEvent(std::function<void()> f, unsigned msec,
                                std::string name) -> PeriodicHandler {
  return rep_->AddPeriodicEvent(std::move(f), msec, std::move(name));
}

void Executor::RemoveHandler(PeriodicHandler handler) {
  rep_->RemoveHandler(handler);
}

void Executor::Shutdown() {
  rep_->StartCancel();
}

void Executor::WaitForLoopToExit() {
  rep_->WaitShutdown();
}

bool Executor::InEbaseThread() const {
  return rep_->event_loop_thread() == pthread_self();
}
void Executor::StopOnTermSignal() {
  signal_executor_instance = this;

  struct sigaction sa;
  sa.sa_sigaction = ExecutorSigHandler;
  sigemptyset (&sa.sa_mask);
  sa.sa_flags = SA_RESTART | SA_SIGINFO;
  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTERM, &sa, NULL);
}

Executor& Executor::Default() {
  static Executor executor("def");
  return executor;
}
REGISTER_MODULE_INITIALIZER(ExecutorModule,
  evthread_use_pthreads();
);

REGISTER_MODULE_DESTRUCTOR(ExecutorModuleExit,
  Executor::Default().Shutdown();
  Executor::Default().WaitForLoopToExit();
);

}  // namespace util
