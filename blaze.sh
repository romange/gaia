#!/bin/bash

root=`dirname $0`
pushd $root

TARGET_BUILD_TYPE=Debug
BUILD_DIR=build-dbg
COMPILER=`which g++`
GENERATOR=''


for ARG in $*
do
  case "$ARG" in
    -release)
        TARGET_BUILD_TYPE=Release
        BUILD_DIR=build-opt
        shift
        ;;
    -clang)
        COMPILER=`which clang++`
        shift
        ;;
    -ninja)
        GENERATOR='-GNinja'
        shift
        ;;
    *)
     echo bad option "$ARG"
     exit 1
     ;;
  esac
  shift
done

mkdir -p $BUILD_DIR && cd $BUILD_DIR
set -x

cmake -L -DCMAKE_BUILD_TYPE=$TARGET_BUILD_TYPE -DCMAKE_CXX_COMPILER=$COMPILER $GENERATOR ..
popd

