[http_main.cc](https://github.com/romange/gaia/blob/master/util/http/http_main.cc)
file demonstrates a small http server app that assigns a user handler
for "/hello" url. Lets deep dive into its code.

```cpp
#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"

#include "util/http/http_conn_handler.h"

#include "absl/strings/str_join.h"
#include "base/init.h"
#include "strings/stringpiece.h"

DEFINE_int32(port, 8080, "Port number.");

using namespace std;
using namespace boost;
using namespace util;
namespace h2 = beast::http;

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);

  IoContextPool pool;
  AcceptServer server(&pool);
  pool.Run();
  http::Listener<> listener;
  auto cb = [](const http::QueryArgs& args, http::HttpHandler::SendFunction* send) {
    http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
    resp.body() = "World!";
    return send->Invoke(std::move(resp));
  };

  listener.RegisterCb("/hello", false, cb);
  uint16_t port = server.AddListener(FLAGS_port, &listener);
  LOG(INFO) << "Listening on port " << port;

  server.Run();
  server.Wait();

  LOG(INFO) << "Exiting server...";
  return 0;
}
```

## Setup
So the first `MainInitGuard` line is responsible for starting auxillary background threads,
parse argument flags and setup logging. It's advised that every GAIA app should start
with this line. The next line sets up an IoContext pool that is responsible to start several IO
even-loops - one per CPU core with dedicated thread. Each IO thread integrates its own fiber scheduler.

The threads start running when we call `pool.Run();`. Please note that this command returns to
our main thread after all IO threads start. `AcceptServer`
is responsible for listening on one or more TCP ports, it can accept a connection request
and manage opened connections. This class is protocol agnostic as long as the underlying
transport layer is TCP socket. `AcceptServer` does not handle itself opened connections,
so in order to customize that part of the flow, GAIA provides abstract `util::ListenerInterface`
that is responsible to provide `util::ConnectionHandler` object per each connection.
Derived classes should provide concrete instances of `util::ConnectionHandler` objects. For example,
here we have `http::Listener<>` that derives from `util::ListenerInterface` and is responsible for
allocating http connection handlers. In order to bind `http::Listener<>` to `AcceptServer` we call
`server.AddListener(FLAGS_port, &listener);`.

Each ConnectionHandler instance runs in a dedicated fiber inside IO thread belonging to the IoContextPool.
It should not block its thread and the fiber should share its CPU time with other fiber-connections running
in the thread. Every new connection will be pinned to the next IO thread based on round-robin scheme.

For http connection handler we are interested to reroute http request to user defined pieces of code,
based on url of the request. Root `"/"` url is reserved for backend status http page.

## Run phase

`server.Run();` start the accepting server and immediately returns.
`server.Wait();` blocks the thread main thread until KILL or INT signals stops the accepting loop.
For every new connection a new `util::http::HttpHandler` is created and it handles knows how to
parse http protocol. It parses the url of the incoming request and dispatches accordingly
the registered callbacks functions. Each such `HttpHandler` runs in a single fiber
and handles the connection it owns. It uses fiber-friendly asynchronous methods to write and
read from a socket. The http related code uses
well documented [beast library](https://www.boost.org/doc/libs/1_69_0/libs/beast/doc/html/index.html).
Once you run the program, you can point your to "localhost:8080/hello" and see "World!" response.

## Monitoring
If you browse with your browser to "localhost:8080/" you will see a status page that provides backend
related stats. In our case you will see a single "connection" metric that track number of live connections
in the server. You can add your own metrics, please see [varz_stats.h](https://github.com/romange/gaia/blob/master/util/stats/varz_stats.h) file for reference.
To see json only metric representation you can browse at "localhost:8080/?o=json".
