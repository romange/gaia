# Gaia - rapid backend development framework in C++

[![Build Status](https://travis-ci.org/romange/gaia.svg?branch=master)](https://travis-ci.org/romange/gaia)
=====

Gaia is a set of libraries and c++ environment that allows you efficient and rapid development
in c++14 on linux systems. The focus is mostly for backend development, data processing etc.


1. Dependency on [abseil-cpp](https://github.com/abseil/abseil-cpp/)
2. Dependency on [Boost 1.69](https://www.boost.org/doc/libs/1_69_0/doc/html/)
3. Uses ninja-build on top of cmake
4. Build artifacts are docker-friendly.
5. Generic RPC implementation.
6. HTTP server implementation.
7. Many other features.


I will gradually add explanations for most crucial blocks in this library.


## Setting Up & Building
1. abseil is integrated as submodule. To fetch abseil run:

       git submodule update --init --recursive

2. Building using docker.
   Short version - running asio_fibers example.
   There is no need to install dependencies on a host machine.

    ```bash
       > docker build -t asio_fibers -f docker/bin_build.Dockerfile --build-arg TARGET=asio_fibers
       > docker run --network host asio_fibers --logtostderr  # server process, from shell A
       > docker run --network host asio_fibers --connect=localhost --count 10000 --num_connections=4  # client process from shell B
    ```

   For a longer version please see [this document](doc/docker_build.md).

3. Building on host machine directly. Currently requires Ubuntu 18.04.

   ```bash
   > sudo ./install-dependencies.sh
   > ./blaze.sh -ninja -release
   > cd build-opt && ninja -j4 asio_fibers

   ```
   *third_party* folder is checked out under build directories.

   Then, from 2 tabs run:

   ```bash
     server> ./asio_fibers --logtostderr
     client> ./asio_fibers --connect=localhost --count 100000 --num_connections=4
   ```



## Single node Mapreduce
GAIA library provides a very efficient multi-threaded mapreduce framework for batch processing.
It supports out of the box json parsing, compressed formats (gzip, zstd),
local disk I/O and GCS (Google Cloud Storage). Using GAIA MR it's possible to map,
re-shard (partition), join and group multiple sources of data very efficiently.
Fibers in GAIA allowed maximizing pipeline execution and balance IO
with CPU workloads in parallel. The example below shows how to process text files and re-shard them based on an imaginary "year" column for each CSV row. Please check out [this tutorial](doc/mr3.md) to learn more about GAIA MR.

~~~~~~~~~~cpp
#include "absl/strings/str_cat.h"
#include "mr/local_runner.h"
#include "mr/mr_main.h"
#include "strings/split.h"  // For SplitCSVLineWithDelimiter.

using namespace std;

DEFINE_string(dest_dir, "~/mr_output", "Working dir where the pipeline writes its by products");

int main() {
  // sets up IO threads and optional http console interace via port 8080 by default.
  PipelineMain pm(&argc, &argv);
  vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);  // could be a local file or "gs://...." url.
  }
  CHECK(!inputs.empty()) << "Must provide some inputs to run!";

  Pipeline* pipeline = pm.pipeline();

  // Assuming that the first line of each file is csv header.
  StringTable ss = pipeline->ReadText("read", inputs).set_skip_header(1);
  auto reshard = [](string str) {
    vector<char*> cols;
    SplitCSVLineWithDelimiter(&str.front(), ',', &cols);
    return absl::StrCat("year-", cols_[0]);
  };

  // Simplest example: read and repartition by year.
  ss.Write("write_input", pb::WireFormat::TXT)
      .WithCustomSharding(reshard).AndCompress(pb::Output::ZSTD);

  // Environment is abstracted away through mr3::Runner class. LocalRunner is an implementation
  // that comes out of the box.
  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);
  pipeline->Run(runner);

  LOG(INFO) << "Pipeline finished";

  return 0;
}
~~~~~~~~~~

## RPC
In addition to great performance, this RPC supports server streaming API, fully asynchronous
processing, low-latency service. GAIA RPC framework employs Boost.ASIO and Boost.Fibers
as its core libraries for asynchronous processing.

1. [IoContextPool](https://github.com/romange/gaia/blob/master/util/asio/io_context_pool.h)
is used for managing a thread-per-core asynchronous engine based on ASIO.
For periodic tasks, look at `asio/period_task.h`.

2. The listening server (AcceptServer) is protocol agnostic and serves both HTTP and RPC.

3. RPC-service methods run inside a fiber. That fiber belongs to a thread that probably serves
many other fiber-based connections in the server. Using regular locking mechanisms
(`std::mutex`, `pthread_mutex`) or calling 3rd party libraries (libmysqlcpp) will block the whole thread and all its connections will be stalled. We need to be mindful of this, and as a policy prohibit thread blocking in fiber-based server code.

4. Nevertheless, RPC service methods might need to issue RPC calls by themselves or block for some other reason.
To do it correctly, we must use fiber-friendly synchronization routines. But even in this case,
we will still block the calling fiber (not thread). All other connections will continue processing but this one will stall. By default, there is one dedicated fiber per RPC connection that reads rpc requests and delegates them to the RPC application code. We need to remember that if higher level server-code stalls its fiber during its request processing, it effectively limits total QPS per that socket connection. For spinlock use-cases (i.e. RAM access locking with rw-spinlocks with low contention) having single fiber per rpc-connection is usually good enough to sustain high throughput. For more complicated cases, it's advised to implement fiber-pool (currently not exposed in GAIA).

5. Server-side streaming is needed for responses that can be very large. Such responses can easily be represented by
a stream of smaller responses with an identical schema. Think of SQL response for example.
It may consist of many rows returned by `SELECT`. Instead, of returning all of them as one blob, server-side streaming can send back multiple responses in the context of a single request on a wire. Each small response is propagated to RPC client via a callback based interface.
As a result, both systems (client and server) are not required to hold the whole response in RAM at the same time.

While GAIA provides very efficient RPC core library, it does not provide higher level RPC bindings.
It's possible though to build a layer that uses protobuf-based declaration language this RPC library.
For raw RPC demo see asio_fibers above.

## HTTP
HTTP handler is implemented using [Boost.Beast](https://www.boost.org/doc/libs/1_68_0/libs/beast/doc/html/index.html) library.
It's integrated with the IoContextPool similarly to RPC service.
Please see [http_main.cc](https://github.com/romange/gaia/blob/master/util/http/http_main.cc), for example. HTTP also provides support for backend monitoring (Varz status page) and for extensible debugging interface. With monitoring C++ backend returns json object that is formatted inside status page in the browser. To check how it looks, please go to [localhost:8080](http://localhost:8080) while `asio_fibers` are running.


### Self-profiling
Every http-powered backend has integrated CPU profiling capabilities using [gperf-tools](https://github.com/gperftools/gperftools) and [pprof](https://github.com/google/pprof)
Profiling can be trigerred in prod using magic-url commands. Enabled profiling usually has very minimal impact
on cpu performance of the running backend.

### Logging
Logging is based on Google's [glog library](https://github.com/google/glog). The library is very reliable, performant and solid. It has many features that allow resilient backend development.
Unfortunately, Google's version has some bugs, which I fixed (waiting for review...), so I use my own fork. Glog library gives me the ability to control logging levels of a backend at run-time without restarting it.

## Tests
GAIA uses googletest+gmock unit-test environment.

## Conventions
To use abseil code use `#include "absl/..."`.
Third_party packages have `TRDP::` prefix in `CMakeLists.txt`. absl libraries have prefix
`absl_...`.
