#!/bin/bash

apt install cmake libbz2-dev zip flex bison ninja-build autoconf-archive

# for folly & proxygen. gperf is not related to gperftools.
apt install libboost-all-dev autoconf-archive gperf
