add_subdirectory(math)
add_subdirectory(coding)


add_library(task_pool sp_task_pool.cc)
cxx_link(task_pool base TRDP::folly)

cxx_test(sp_task_pool_test base task_pool LABELS CI)

