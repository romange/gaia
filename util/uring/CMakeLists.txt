add_library(uring_fiber_lib accept_server.cc fiber_socket.cc prebuilt_asio.cc
            proactor.cc uring_fiber_algo.cc)
cxx_link(uring_fiber_lib base Boost::fiber -luring)

cxx_test(proactor_test uring_fiber_lib LABELS CI)
