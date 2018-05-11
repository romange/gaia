#include <boost/asio.hpp>
#include <memory>
#include <list>
#include <vector>

#include "base/init.h"
#include "base/logging.h"
#include "absl/strings/escaping.h"

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
  : next_io_context_(0) {
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
  LOG(INFO) << "Creating " << io_contexts_.size() << " threads";

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


/// Represents a single connection from a client.
class connection : public std::enable_shared_from_this<connection> {
 public:
  /// Construct a connection with the given io_context.
  connection(asio::io_context& io_context, asio::ip::tcp::socket socket)
    : io_(io_context), socket_(std::move(socket)) {}

  ~connection() {
    LOG(INFO) << "Closing connection " << socket_.remote_endpoint();
  }

  connection(const connection&) = delete;
  connection& operator=(const connection&) = delete;

  /// Get the socket associated with the connection.
  boost::asio::ip::tcp::socket& socket();

  /// Start the first asynchronous operation for the connection.
  void start() {
    do_read();
  }

 private:
  void do_read() {
    // Holds the object until there are active callbacks.
    auto self(shared_from_this());

    socket_.async_read_some(boost::asio::buffer(buffer_),
        [this, self](boost::system::error_code ec, std::size_t length) {
          handle_read(ec, length);
        });
  }

  /// Handle completion of a read operation.
  void handle_read(const boost::system::error_code& e, std::size_t length) {
    if (!e) {
      LOG(INFO) << "Read " << length << " bytes";
      LOG(INFO) << absl::CEscape(absl::string_view(buffer_.data(), length));
      do_read();
    }
  }

  /// Handle completion of a write operation.
  void handle_write(const boost::system::error_code& e);

  asio::io_context& io_;

  /// Socket for the connection.
  asio::ip::tcp::socket socket_;

  /// Buffer for incoming data.
  std::array<char, 8192> buffer_;
};


void do_accept(io_context_pool* pool, asio::ip::tcp::acceptor* acc) {
  acc->async_accept(
     [acc, pool](const boost::system::error_code& ec, boost::asio::ip::tcp::socket peer) {
      if (!ec) {
        LOG(ERROR) << "Succeeded " << peer.remote_endpoint();
        auto cptr = std::make_shared<connection>(pool->get_io_context(), std::move(peer));
        cptr->start();
      }
      do_accept(pool, acc);
    });
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  io_context_pool pool(std::thread::hardware_concurrency());

  /// The signal_set is used to register for process termination notifications.
  asio::signal_set signals(pool.get_io_context(), SIGINT, SIGTERM);

  /// Acceptor used to listen for incoming connections.
  asio::ip::tcp::acceptor acceptor(pool.get_io_context());

  signals.async_wait(
      [&](boost::system::error_code /*ec*/, int /*signo*/) {
        // The server is stopped by cancelling all outstanding asynchronous
        // operations. Once all operations have finished the io_context::run()
        // call will exit.
        acceptor.close();
        pool.stop();
      });

  asio::ip::tcp::resolver resolver(acceptor.get_io_context());
  asio::ip::tcp::endpoint endpoint = *resolver.resolve("0.0.0.0", "8080").begin();
  acceptor.open(endpoint.protocol());
  acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
  acceptor.bind(endpoint);
  acceptor.listen();

  CHECK(acceptor.is_open());

  do_accept(&pool, &acceptor);

  pool.run();

  return 0;
}
