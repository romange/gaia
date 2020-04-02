Overview of asynchronous programming
====================================

Let's say we have a program that reads a large file and processes each chunk in the file in some way. A naive implementation would look as following:

    while (chunk = read_chunk()) {
        process_chunk(chunk);
    }

The problem with the naive implementation is that although I/O and CPU are different system resources, it uses only one each time instead of reading and processing at the same time. Asynchronous programming tries to solve this problem by allowing the read operation to run WITHOUT waiting for its result. This can be done either via asynchronous callbacks or by concurrency. A solution based on asynchronous callbacks would look as following:

    // The function "callback" is called when the chunk is read.
    auto callback = [](Chunk&& chunk) {
        if (chunks_remaining_to_read())
            read_chunk_async(callback);
        process_chunk(chunk);
    };
    for (int i = 0; i < CHUNK_LOOK_AHEAD; ++i) {
        // read_chunk_async returns immediately, when it is done,
        // it updates a chunk-callback list which will be called
        // when the event loop allows it.
        read_chunk_async(callback);
    }
    // The event loop runs repeatedly and checks whether there
    // an I/O event finished and added a callback to the list,
    // if there isn't, it tells the OS to idle the process until
    // an event finished.
    while (true) {
        if (has_callbacks()) {
            call_callbacks();
        } else {
            wait_until_there_are_callbacks();
        }
    }

While the example above allows one to express a program that does reading and processing at the same time, the callbacks hide the order of operations (process comes after read) and increase the complexity of the code. A solution using two separate tasks would be easier to read:

    queue = create_queue_with_size(CHUNK_LOOK_AHEAD);
    io_task = start_task([] {
        while (!queue.full()) {
            // read_chunk_async returns immediately, when it is done,
            // it sends the processed chunk to a queue.
            chunk = read_chunk();
            queue.push(chunk);
        }
    });
    main_task = start_task([] {
        while (!processed_all_chunks) {
            chunk = queue.get(); // gets a chunk and initiates an async read
            process_chunk(chunk);
        }
    });

Such tasks can be implemented either via threads or fibers. A *thread* allows a *process* to keep several states of execution and switch between them *preemptively* - whenever the OS wants. A *fiber* allows a *thread* to keep several states of execution and switch between them *cooperatively* - when a fiber willingly yields. The cooperative task switching of fibers set them apart from threads. While threads can run on several processors and their preemptitive switching allows for simple computational parallelism, these same characteristics also introduce race conditions and kernel overhead. In this respect, fibers can be considered the opposite of threads. Fibers belonging to the same thread will always run on the same processor and will only switch when the programmer explicitly calls a yielding function, preventing computational parallelism, but preserving atomicity of operations, and not requiring the use of expensive kernel context switches. Thus, when one wants to implement the example above, one may prefer to use fibers over threads.

IoContext
=========

Usage
-----

Boost provides two different mechanisms for asynchronous programming. The first is the Asio library, which provides a convinient event-loop and callbacks mechanism. The second is the fiber library, which allows easy creation of fibers and switching to them. Gaia's IoContext combines both of these into a single mechanism, allowing one to either run a quick callback in the event loop, or to start a fiber which may stop and resume at later times.

Here is a usage example:

    IoContext worker_io_ctx;
    auto work = asio::make_work_guard(&worker_io_ctx);
    std::thread worker_thread([] {
        fibers_ext::BlockingCounter bc(1); // To have N IoContexts starting, let them
                                           // all share the same BlockingCounter and initialize it
                                           // with N instead of 1.
        worker_io_ctx.StartLoop(&bc);
    });

    // Run c1 DIRECTLY. c1 may not block. Returns immediately.
    worker_io_ctx.Async(c1);
    // Run c2 on fiber. More costly, c2 may block. Joins the fiber.
    worker_io_ctx.AsyncFiber(c2);
    // Run c3 DIRECTLY. c3 may not block. Returns after run.
    worker_io_ctx.Await(c3);
    // Run c4 on fiber. More costly, c4 may block. Return after fiber start with fiber id.
    worker_io_ctx.LaunchFiber(c4);

    // Ofcourse, context can also be passed to I/O running code that will notify it
    // using one of the calls above when an I/O is done.

Internals
---------

Let's discuss how StartLoop is implemented. At its core, `IoContext::StartLoop()` initiates an Asio `io_context` (this is the primitive object that `IoContext` wraps, it allows sending callbacks that will run directly on the Asio event loop, but it doesn't allow starting fibers on it. After initializing the `io_context`, it is connected to an `AsioScheduler`. Afterwards, the `io_context` gets a single callback to run on it: `AsioScheduler::MainLoop()`.

The `AsioScheduler` is a Gaia object responsible for telling the boost fiber manager how to schedule the different fibers. It is designed to give high priotity to the fiber running the event loop, ensuring that it is always ran between other fibers. Besides the event loop running fiber, other fibers are decided using several levels of priority (all lower than the event loop fiber), with round-robin inside each level.

`AsioScheduler::MainLoop()` alternates between executing I/O callbacks from Asio and yielding to fibers. Through this behavior, Asio callbacks and fibers are unified. Furthermore, time allocated to fibers is measured and an error is reported if a fiber took too long to run. However, this is not done for Asio callbacks.

IoContextPool
=============

Usage
-----

`IoContext` objects are not meant to be used directly. Instead, they are used by an `IoContextPool`, which offers a similar functionality, but with a thread pool where each thread is pinned to a specific CPU. `IoContextPool` offers a similar interface to `IoContext`, but also differentiates between sending a request to a single worker thread and broadcasting it to all worker threads.

Here is a usage example:

    IoContextPool pool;
    pool->AwaitOnAll([&](IoContext& ctx) { // Blocks until done, runs directly, runs in parallel.
        initialize_thread_local_global_variables();
    });
    pool->AwaitOnAll([&](unsigned worker_index, IoContext& ctx) {
        initialize_global_indexed_variables(worker_index);
    });
    pool->GetNextContext().Async(c1); // GetNextContext returns an IoContext from one of the workers.
    pool[i].Async(c1); // operator[] allows to select which IoContext to use.
    pool->AsyncOnAll(c2); // Like AwaitOnAll, but returns immediately.
    pool->AsyncFiberOnAll(c3); // Like AsyncOnAll, but instead of running directly starts fibers.
    pool->AwaitFiberOnAll(c4); // Like AwaitOnAll, but instead of running directly starts fibers.
    pool->AwaitFiberOnAllSerially(c5); // Like AwaitFiberOnAll but runs serially, good for a reduce
                                       // or join operation in which workers updates the same global.

Internals
---------

`IoContextPool` is implemented via an array of threads and `IoContext`s. The Async functions are implemented by iterating over every `IoContext` and calling its `Async()` method. The parallel Await functions are implemented by iterating over every `IoContext`, calling its `Async()` method, and waiting on a `fibers_ext::BlockingCounter` which every callback instance updates once it is done. The serial Await function is implemented by calling `AwaitFiberOnAll()` with a mutex that enforces only one fiber running at a time (it could also be implemented by calling `Await()` on every `IoContext`).

File IO
=======

Usage
-----

When a fiber wants to access a file, it has access to two APIs that instead of blocking the entire thread only suspend the current fiber until the I/O is finished. These two APIs are `OpenFiberReadFile` and `OpenFiberWriteFile`. Both of these return interfaces of type `ReadonlyFile` and `WriteFile` which enable standard read/write operations.

Here is a usage example:

    util::StatusObject<WriteFile*> result = OpenFiberWriteFile("ori2785");
    CHECK(result.ok());
    std::unique_ptr<WriteFile> wf(result.obj);
    CHECK_STATUS(wf->Write("Hello\n", 6));
    CHECK_STATUS(wf->Close());

    util::Statusobject<ReadonlyFile*> result2 = OpenFiberReadFile("ori2785");
    CHECK(result2.ok());
    std::unique_ptr<ReadonlyFile> rf(result.obj);
    unsigned char buf[6];
    CHECK_STATUS(rf->Read(0, MutableByteRange(buf, buf + 6)));
    CHECK_STATUS(rf->Close());

Other Gaia APIs (for example, reading line formatted files, CSV, LST) are built on top of these primitives.

Internals
---------

TODO: Explain how it is impossible to really do async I/O in Linux in a non-XFS filesystem so instead Gaia uses a thread pool of I/O threads. Also need to wonder why we need a pool and can't have a single I/O thread since all it does anyway is just call system calls.

TODO: Network IO
TODO: Maybe an example of how Gaia-MR uses all of these.
TODO: Some of the fiber sync primitives, like blocking counter and friends.