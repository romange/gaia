#!/bin/bash

set -e

apt install -y cmake libunwind-dev zip flex bison ninja-build autoconf-archive
apt install -y curl

BVER=1.69.0
BOOST=boost_${BVER//./_}   # replace all . with _

install_boost() {
    if ! [ -d $BOOST ]; then
      wget -nv http://dl.bintray.com/boostorg/release/${BVER}/source/$BOOST.tar.bz2 \
        && tar -xjf $BOOST.tar.bz2
    fi

    cd $BOOST && ./bootstrap.sh --prefix=/opt/${BOOST} --without-libraries=graph_parallel,graph,wave,test,mpi,python
    b2_args=(define=BOOST_COROUTINES_NO_DEPRECATION_WARNING=1 link=shared variant=release debug-symbols=off
             threading=multi --without-test --without-math --without-log --without-locale --without-wave
             --without-regex --without-python -j4)

    ./b2 "${b2_args[@]}" cxxflags="-std=c++14 -Wno-deprecated-declarations -fPIC -O3"
    ./b2 install "${b2_args[@]}" -d0
}

if ! [ -d /opt/${BOOST}/lib ]; then
  install_boost
  ln -sf /opt/${BOOST} /opt/boost
else
  echo "Skipping installing ${BOOST}"
fi

