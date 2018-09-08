add_library(http_base http_status_code.cc)
cxx_link(http_base strings)

add_library(http http_handlers.cc http_server.cc varz_stats.cc http_server_status.cc)
cxx_link(http http_base util TRDP::evhtp proc_stats stats_lib threads fast_malloc)

add_executable(http_main http_main.cc)
cxx_link(http_main  asio_fiber_lib)

# requires  sudo apt-get install libcurl4-gnutls-dev
add_library(http_client http_client.cc)
cxx_link(http_client http_base strings curl util)
