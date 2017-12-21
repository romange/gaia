#!/bin/bash

apt install -y cmake libevent-dev libunwind8-dev zip flex bison ninja-build autoconf-archive

# for folly & proxygen. gperf is not related to gperftools.
apt install -y libboost-all-dev autoconf-archive gperf

BOOST=boost_1_66_0
wget http://dl.bintray.com/boostorg/release/1.66.0/source/$BOOST.tar.bz2 \
    && tar -xjf $BOOST.tar.bz2 && cd $BOOST \
    && ./bootstrap.sh --prefix=/opt/$BOOST --without-libraries=graph_parallel,graph,program_options,wave,test,mpi,python
./b2 --link=shared cxxflags="-std=c++14 -Wno-deprecated-declarations"  --variant=release --threading=multi \
     --without-test -j4
./b2 install
