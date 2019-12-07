add_executable(redis_toy_server redis_toy_server.cc redis_command.cc
               resp_parser.cc resp_connection_handler.cc)
cxx_link(redis_toy_server base http_v2 absl_variant)
