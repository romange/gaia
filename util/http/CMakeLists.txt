add_library(http_v2 http_conn_handler.cc status_page.cc profilez_handler.cc)
cxx_link(http_v2 asio_fiber_lib proc_stats strings stats_lib fast_malloc)

add_executable(http_main http_main.cc)
cxx_link(http_main http_v2)

# requires  sudo apt-get install libcurl4-gnutls-dev
add_library(http_client http_client.cc)
cxx_link(http_client strings asio_fiber_lib)

cxx_test(http_test http_v2 http_client LABELS CI)
