#!/bin/bash

################################################################################
# The following is executed on .travis.yml's script section
################################################################################

set -ex

DEPS_UBUNTU="g++ cmake git libboost-regex-dev libkrb5-dev xfslibs-dev libssl-dev python-dev libfuse-dev default-jdk"
DEPS_CENTOS="gcc-c++ make cmake git boost-devel krb5-devel xfsprogs-devel openssl-devel python-devel fuse-devel java-openjdk java-devel libuuid-devel"

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    make gtest tarball
fi

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
    if [[ "$DISTRO" == "ubuntu" ]]; then
        CMD="sudo apt-get update"
        CMD="$CMD && sudo apt-get install -y $DEPS_UBUNTU"
        CMD="$CMD && CMAKE_OPTIONS=\"-D ENABLE_COVERAGE=yes -D CMAKE_BUILD_TYPE=RelWithDebInfo\" make gtest tarball"
        CMD="$CMD && gcov -o build/debug/src/cc/tests/CMakeFiles/test.t.dir/ integtest_main.cc.o"
    elif [[ "$DISTRO" == "centos" ]]; then
        CMD="yum install -y $DEPS_CENTOS"
        CMD="$CMD && make gtest tarball"
    fi

    docker run --rm -t -v $PWD:$PWD -w $PWD $DISTRO:$VER /bin/bash -c "$CMD"
fi

