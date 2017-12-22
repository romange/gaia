#include "base/init.h"
#include "base/logging.h"
#include <boost/fiber/all.hpp>
#include <thread>

using namespace boost;

DEFINE_string(file, "", "File to read asynchronously");
DEFINE_int32(threads, 4, "");
DEFINE_int32(io_len, 4, "");

struct ReadRequest {
  int fd;
  ssize_t offs;
};

using fibers::channel_op_status;
using namespace std;

std::atomic<long> num_requests;
void ReadIO(fibers::buffered_channel<ReadRequest>& ch) {  
  ReadRequest rr;
  char buf[32];

  while (true) {
    channel_op_status st = ch.pop(rr);
    if (st == channel_op_status::closed)
      return;

    --num_requests;
    CHECK(st == channel_op_status::success);
    pread(rr.fd, buf, 16, rr.offs);
  }
}

int main(int argc, char **argv) {
  MainInitGuard guard(&argc, &argv);

  fibers::buffered_channel<ReadRequest> ch(FLAGS_io_len);
  using fibers::channel_op_status;

  CHECK(!FLAGS_file.empty());
  int fd = open(FLAGS_file.c_str(), O_RDONLY, 0644);
  CHECK_GT(fd, 0);
  std::vector<std::thread> io_threads;
  for (int i = 0; i < FLAGS_threads; ++i) {
    io_threads.emplace_back(&ReadIO, std::ref(ch));
  }

  struct stat sbuf;
  CHECK_EQ(0, fstat(fd, &sbuf));
  cout << "File size is " << sbuf.st_size << endl;
  off_t offset = 0;

  num_requests = 0;
  while (offset + 20 < sbuf.st_size) {
    channel_op_status st = ch.push(ReadRequest{fd, offset});
    ++num_requests;
    CHECK(st == channel_op_status::success);
    offset += (1 << 17);
  }
  ch.close();
  
  cout << "Before join\n";
  for (auto& t : io_threads)
    t.join();
  CHECK_EQ(0, num_requests.load());
  
  return 0;
}
