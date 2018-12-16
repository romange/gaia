#!/bin/bash

set -e

apt install -y cmake libevent-dev libunwind-dev zip flex bison ninja-build autoconf-archive

# for folly & proxygen. gperf is not related to gperftools.
apt install -y gperf curl

BOOST_VER=boost_1_68_0

install_boost() {
    BOOST=$BOOST_VER
    if ! [ -d $BOOST_VER ]; then
      wget -nv http://dl.bintray.com/boostorg/release/1.68.0/source/$BOOST.tar.bz2 \
        && tar -xjf $BOOST.tar.bz2
    fi

    cd $BOOST && ./bootstrap.sh --prefix=/opt/boost --without-libraries=graph_parallel,graph,wave,test,mpi,python
    ./b2 link=shared variant=release debug-symbols=on threading=multi cxxflags="-std=c++14 -Wno-deprecated-declarations -fPIC -O3"  \
         --without-test --without-math --without-log --without-locale --without-wave --without-regex --without-python -j4
    ./b2 install -d0
}

if ! [ -d /opt/boost/lib ]; then
  install_boost
else
  echo "Skipping installing boost"
fi


