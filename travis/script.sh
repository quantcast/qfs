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
    elif [[ "$DISTRO" == "centos" ]]; then
        CMD="yum install -y $DEPS_CENTOS"
    fi

    CMD="$CMD && make gtest tarball"
    docker run --rm -t -v $PWD:$PWD -w $PWD $DISTRO:$VER /bin/bash -c "$CMD"
fi

