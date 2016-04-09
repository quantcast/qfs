#!/bin/bash

################################################################################
# The following is executed on .travis.yml's before_install section
################################################################################

set -ex

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update || true
    brew install git || true
    brew install cmake || true
    brew install maven || true
    brew install boost || true
fi

# use docker to build on linux; pull the corresponding docker image
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
    docker pull $DISTRO:$VER
fi
