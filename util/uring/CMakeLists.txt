add_library(uring_fiber_lib accept_server.cc fiber_socket.cc http_handler.cc
            #we need prebuilt_asio for errrors support, consider using our own #
            prebuilt_asio.cc proactor.cc proactor_pool.cc uring_fiber_algo.cc)
cxx_link(uring_fiber_lib base http_common Boost::fiber -luring)

cxx_test(proactor_test uring_fiber_lib)
cxx_test(accept_server_test uring_fiber_lib http_beast_prebuilt)
