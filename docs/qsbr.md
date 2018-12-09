# Quiescent-State-Based Reclamation

Designing high throughput server is hard. One of the problems we often encounter is
how to update internal data-structure that sustain mostly read traffic.
I've wrote in the past a [post about this](http://www.romange.com/2017/09/29/reloading-data-structures-under-high-throughput/).
In short, what I suggested is to use rw-lock with reference-counted object:

```cpp
// Vars section
RWSpinLock index_lock_;
shared_ptr<Index> data_ptr_;

// Query Thread
index_lock_.lock_shared();
shared_ptr<Index> local_copy = data_ptr_;
index_lock_.unlock_shared();

auto res = local_copy->Query(arg);
..................

// Reload thread.
std::shared_ptr<Index> new_index = make_shared<Index>(); // Create another index object.
new_index->UpdateFromFile(file_name);

index_lock_.lock();
data_ptr_ = new_index;
index_lock_.unlock();
```

This approach is much better than blocking schemes but it has its own problems: Reference counting and
read locks have contention on a shared memory and this contention grows with number of threads.
In addition, the write path blocks readers which, in turn, affects our tail latency.

To summarize, lock-less reference counting is considered [as less efficient method](http://www2.rdrop.com/~paulmck/RCU/hart_ipdps06_slides.pdf) in lockless
memory reclamation schemes. How can we improve? PE McKenney generalized the flow above to
Read-modify-write or RCU scheme. There are few overview articles explaining this, I suggest to read
[this one](https://kukuruku.co/post/lock-free-data-structures-the-inside-rcu).

Tom Hard and PE McKenney conducted a thorough comparison of most performant implementations
within this scheme. One of them is a cooperative algorithm called "Quiescent-State-Based Reclamation".

In short, each "read" thread accessing our `data_ptr_` should call a function `quiescent_state();`
which notifies a system that this thread does not hold any references to `data_ptr_`.
This call has almost no performance penalty and can be called when a thread is going to rest or
finished running user callbacks that access `data_ptr_`.

The writer thread blocks itself from deleting old `data_ptr_` until all threads stop accessing it.
I am omitting memory ordering directives for simplicity and assume that the order of execution
as it's written in the code:

```cpp
// My Index pointer. Note that now it's just an atomic to permit multi-threading access.
atomic<Index*> data_ptr_;

// Writer thread.
atomic<Index*> tmp = new Index;
tmp->Load(...);

// Updating the shared pointer to allow accessing new index. Now tmp points to the old index.
// From now on, read threads will query new index.
data_ptr_.swap(tmp);

// tmp can not be deleted until all readers stop accessing it.
// Therefore I am calling magic function that helps me with that.
BlockUntilAllThreadsPassQuiescentState();
delete tmp.load();
```

The function `BlockUntilAllThreadsPassQuiescentState();` allows grace period, during which
the writer is blocked until all readers stopped accessing their current pointers to `Index`.
Please note that `BlockUntilAllThreadsPassQuiescentState();` is not obliged to wait for there readers
that meanwhile start accessing the new value of `data_ptr_`.

In RCU scheme this blocking function is called `synchronize_rcu();` and as I said QSBR is only one
of the implementations for that function. I wrote down the simple pseudo-code
for `BlockUntilAllThreadsPassQuiescentState()` and `quiescent_state()` below to demonstrate the concept.

```cpp
std::atomic<int64_t> global_state = 0;
void BlockUntilAllThreadsPassQuiescentState() {
  ScopedCriticalSection(); // Only one concurrent writer is permitted.

  global_state += 2;
  for (t : registered_threads) {
    while (local_state(t) < global_state) {
      Poll();
    }
  }
}

thread_local int64_t local_state = 0;
void quiescent_state() {
  local_state = global_state + 1;
}

```
Please note that the algorithm assumes that reader threads preempt or call `quiescent_state()`
frequently enough so that the writer won't burn the cpu while polling.

## State of things in Gaia.
Gaia framework assumes a thread-per-core architecture with dedicated IO loop per thread.
It can use optional worker threads to handle non-asynchronous workloads like disk I/O.
Each thread in Gaia can spawn multiple fibers that share CPU for this thread. IO event-loop
is just one of such fibers that coordinates and schedules verious tasks and activation events.
As long as user-fibers do not block their parent thread it's guaranteed that the thread execution
will return to its IO loop. When I thought how to add QSBR to GAIA, my first thought was
that we can just call `quiescent_state()` inside IO loop.

However we can do even better. The important ovesrvation is that any code running directly in IO
loop is running during quiescent state of that thread. In order for IO loop to run, all other fibers
should give up their thread control. Therefore any user-level change to shared data structure during
IO loop fiber execution can not cause data races as long as user fibers do not keep stale data after yielding.

```cpp
thread_local Index* local_index_ = nullptr;

Index* current_index = nullptr;

// Read fiber
auto index = local_index_;
if (index)
  res = index->Query();
...
CHECK(index == local_index_); // local_index_ does not change because this fiber has not yielded.
yield();

// CHECK(index == local_index_);  Bug! Does not necessarily hold.
/////////
.....
// (re)load the index from another thread, possibly outside of the IO Pool.
Index* new_index = new Index();
new_index->Load();

IoContextPool& pool = RefMyPool();

// Blocks until the lambda task has been run in each of the IO threads.
pool.MapTaskSync([new_index] { local_index_ = new_index; });

// Now all thread-local instances are updated.
delete current_index;
current_index = new_index;
```

IO threads never access the global index pointer, instead they access the local-thread cache to
its value. Once we want to reload the index, we issue the update call via MapTaskSync() function.
MapTaskSync schedules the lambda function code to run on each of the threads and block until they finish.
Please note that lambda function runs only when all other fibers relinquished their control, thus do
not access `current_index` anymore via their cached values. Once lambda finishes, fibers will access
the new pointer from now on. The advantage of this approach is its simplicity and the fact that the
writer thread blocks without polling. The disadvantage is that we need to be attentive to every
possible yield inside fibers and design our data access accordingly.

In case we need to reuse pointers between IO calls and those pointers might become stale due to reloads
we will need to implement another algorithm withing RCU scheme.
