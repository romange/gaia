add_executable(ping_server ping_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_server base asio_fiber_lib)

add_executable(ping_sync_server ping_sync_server.cc resp_parser.cc ping_command.cc)
cxx_link(ping_sync_server base asio_fiber_lib)
