#!/bin/bash

apt install -y cmake libevent-dev libunwind8-dev zip flex bison ninja-build autoconf-archive

# for folly & proxygen. gperf is not related to gperftools.
apt install -y libboost-all-dev autoconf-archive gperf
