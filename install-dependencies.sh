#!/bin/bash

set -e

apt install -y cmake libunwind-dev zip flex bison ninja-build autoconf-archive
apt install -y curl

BVER=1.71.0
BOOST=boost_${BVER//./_}   # replace all . with _

install_boost() {
    mkdir -p /tmp/boost && pushd /tmp/boost
    if ! [ -d $BOOST ]; then
      url="http://dl.bintray.com/boostorg/release/${BVER}/source/$BOOST.tar.bz2"
      echo "Downloading from $url"
      wget -nv ${url} && tar -xjf $BOOST.tar.bz2
      chown ${SUDO_USER}:${SUDO_USER} -R $BOOST.tar.bz2 $BOOST
    fi

    booststap_arg="--prefix=/opt/${BOOST} --without-libraries=graph_parallel,graph,wave,test,mpi,python"
    cd $BOOST
    boostrap_cmd=`readlink -f bootstrap.sh`
    echo "Running ${boostrap_cmd} ${booststap_arg}"
    ${boostrap_cmd} ${booststap_arg} || { cat bootstrap.log; return 1; }
    b2_args=(define=BOOST_COROUTINES_NO_DEPRECATION_WARNING=1 link=shared variant=release debug-symbols=on
             threading=multi --without-test --without-math --without-log --without-locale --without-wave
             --without-regex --without-python -j4)

    echo "Building targets with ${b2_args[@]}"
    ./b2 "${b2_args[@]}" cxxflags='-std=c++14 -Wno-deprecated-declarations'
    ./b2 install "${b2_args[@]}" -d0
    chown ${SUDO_USER}:${SUDO_USER} -R ./
    popd
}

if ! [ -d /opt/${BOOST}/lib ]; then
  install_boost
else
  echo "Skipping installing ${BOOST}"
fi

if ! [ -d /opt/boost ]; then
  ln -sf /opt/${BOOST} /opt/boost
fi
