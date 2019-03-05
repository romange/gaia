add_library(fibers_ext fibers_ext.cc fiberqueue_threadpool.cc)
cxx_link(fibers_ext base Boost::fiber)
