FROM ubuntu:16.04

RUN apt -y update && \
    apt -y install software-properties-common libevent-2.0-5 libevent-pthreads-2.0-5 \
                   libunwind8

WORKDIR /app
ADD boost_1_67_0/stage/lib/*.so /opt/boost_1_67_0/lib/
ADD third_party/libs/gflags/lib/libgflags.so.2.2 third_party/libs/gperf/lib/libtcmalloc_and_profiler.so.4 ./

RUN ldconfig /opt/boost_1_67_0/lib/ /app

ADD build-opt/asio_fibers  ./

CMD ["./asio_fibers", "--alsologtostderr"]
