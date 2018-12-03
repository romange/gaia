add_library(sentry sentry.cc)
cxx_link(sentry http_client_lib)

cxx_test(sentry_test http_v2 sentry http_test_lib LABELS CI)
