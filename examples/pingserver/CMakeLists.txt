add_executable(ping_server ping_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_server base asio_fiber_lib http_v2)

add_executable(ping_sync_server ping_sync_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_sync_server base asio_fiber_lib http_v2)

add_executable(ping_epoll_server ping_epoll_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_epoll_server base asio_fiber_lib http_v2)

add_executable(ping_iouring_server ping_iouring_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_iouring_server base stats_lib uring_fiber_lib)
