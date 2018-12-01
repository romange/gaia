add_library(http_v2 http_conn_handler.cc status_page.cc profilez_handler.cc)
cxx_link(http_v2 asio_fiber_lib proc_stats strings stats_lib fast_malloc)

add_executable(http_main http_main.cc)
cxx_link(http_main http_v2)

add_library(http_client_lib http_client.cc)
cxx_link(http_client_lib strings asio_fiber_lib)

cxx_test(http_test http_v2 http_client_lib LABELS CI)
