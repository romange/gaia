#include <boost/asio.hpp>
#include <memory>
#include <list>
#include <vector>

#include "base/init.h"
#include "base/logging.h"


namespace asio = ::boost::asio;

/// A pool of io_context objects.
class io_context_pool {
public:
  io_context_pool(const io_context_pool&) = delete;
  void operator=(const io_context_pool&) = delete;

  /// Construct the io_context pool.
  explicit io_context_pool(std::size_t pool_size);

  /// Run all io_context objects in the pool.
  void run();

  /// Stop all io_context objects in the pool.
  void stop();

  /// Get an io_context to use.
  asio::io_context& get_io_context();

private:
  typedef std::unique_ptr<asio::io_context> io_context_ptr;
  typedef asio::executor_work_guard<
    asio::io_context::executor_type> io_context_work;

  /// The pool of io_contexts.
  std::vector<io_context_ptr> io_contexts_;

  /// The work that keeps the io_contexts running.
  std::list<io_context_work> work_;

  /// The next io_context to use for a connection.
  std::size_t next_io_context_;
};

io_context_pool::io_context_pool(std::size_t pool_size)
  : next_io_context_(0)
{
  CHECK_GT(pool_size, 0);

  // Give all the io_contexts work to do so that their run() functions will not
  // exit until they are explicitly stopped.
  for (std::size_t i = 0; i < pool_size; ++i) {
    io_context_ptr io_context(new asio::io_context);
    work_.push_back(asio::make_work_guard(*io_context));
    io_contexts_.emplace_back(std::move(io_context));
  }
}

void io_context_pool::run() {
  // Create a pool of threads to run all of the io_contexts.
  std::vector<std::thread> threads;
  for (std::size_t i = 0; i < io_contexts_.size(); ++i) {
    std::thread t([this, i] {io_contexts_[i]->run();});
    threads.push_back(std::move(t));
  }

  // Wait for all threads in the pool to exit.
  for (std::size_t i = 0; i < threads.size(); ++i)
    threads[i].join();
}

void io_context_pool::stop() {
  // Explicitly stop all io_contexts.
  for (std::size_t i = 0; i < io_contexts_.size(); ++i)
    io_contexts_[i]->stop();
}

asio::io_context& io_context_pool::get_io_context() {
  // Use a round-robin scheme to choose the next io_context to use.
  asio::io_context& io_context = *io_contexts_[next_io_context_];
  ++next_io_context_;
  if (next_io_context_ == io_contexts_.size())
    next_io_context_ = 0;
  return io_context;
}


struct reply {};
struct request {};

/// The common handler for all incoming requests.
class request_handler {
 public:
  /// Construct with a directory containing files to be served.
  explicit request_handler(const std::string& doc_root) : doc_root_(doc_root) {}

 /// Handle a request and produce a reply.
 void handle_request(const request& req, reply& rep) {}

 private:
  /// The directory containing the files to be served.
  std::string doc_root_;
};


/// Represents a single connection from a client.
class connection : public std::enable_shared_from_this<connection> {
 public:
  /// Construct a connection with the given io_context.
  explicit connection(boost::asio::io_context& io_context,
      request_handler& handler);

  /// Get the socket associated with the connection.
  boost::asio::ip::tcp::socket& socket();

  /// Start the first asynchronous operation for the connection.
  void start();

 private:
  /// Handle completion of a read operation.
  void handle_read(const boost::system::error_code& e,
      std::size_t bytes_transferred);

  /// Handle completion of a write operation.
  void handle_write(const boost::system::error_code& e);

  /// Socket for the connection.
  asio::ip::tcp::socket socket_;

  /// The handler used to process the incoming request.
  request_handler& request_handler_;

  /// Buffer for incoming data.
  std::array<char, 8192> buffer_;

  /// The incoming request.
  request request_;

  /// The reply to be sent back to the client.
  reply reply_;
};

typedef boost::shared_ptr<connection> connection_ptr;

void handle_accept(const boost::system::error_code& e, boost::asio::ip::tcp::socket s) {
  LOG(INFO) << e;
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  io_context_pool pool(std::thread::hardware_concurrency());

  /// The signal_set is used to register for process termination notifications.
  asio::signal_set signals(pool.get_io_context());

  /// Acceptor used to listen for incoming connections.
  asio::ip::tcp::acceptor acceptor(pool.get_io_context());

  signals.add(SIGINT);
  signals.add(SIGTERM);

  asio::ip::tcp::resolver resolver(acceptor.get_executor().context());

  asio::ip::tcp::endpoint endpoint = *resolver.resolve("0.0.0.0", "8080").begin();
  acceptor.open(endpoint.protocol());
  acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
  acceptor.bind(endpoint);
  acceptor.listen();

  request_handler rh("foo");

  /*connection_ptr new_connection(new connection(pool.get_io_context(), rh));

  acceptor.async_accept(new_connection->socket(),
     [](const boost::system::error_code& ec)
    {

    });
*/
  return 0;
}
