#!/bin/bash

################################################################################
# The following is executed on .travis.yml's script section
################################################################################

set -ex

DEPS_UBUNTU="g++ cmake git libboost-regex-dev libkrb5-dev xfslibs-dev libssl-dev python-dev libfuse-dev default-jdk wget unzip maven sudo"
DEPS_CENTOS="gcc-c++ make cmake git boost-devel krb5-devel xfsprogs-devel openssl-devel python-devel fuse-devel java-openjdk java-devel libuuid-devel wget unzip sudo which"

MVN_TAR="apache-maven-3.0.5-bin.tar.gz"
MVN_URL="http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.0.5/binaries/$MVN_TAR"

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    CMAKE_OPTIONS="-D OPENSSL_ROOT_DIR=/usr/local/Cellar/openssl/$(ls -1 /usr/local/Cellar/openssl/ | tail -n 1) -D CMAKE_BUILD_TYPE=RelWithDebInfo" make gtest tarball
fi

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
    if [[ "$DISTRO" == "ubuntu" ]]; then
        CMD="sudo apt-get update"
        CMD="$CMD && sudo apt-get install -y $DEPS_UBUNTU"

        # coverage reports are only generated on ubuntu
        CMD="$CMD && CMAKE_OPTIONS=\"-D ENABLE_COVERAGE=yes -D CMAKE_BUILD_TYPE=RelWithDebInfo\" make gtest tarball"
        CMD="$CMD && gcov -o build/debug/src/cc/tests/CMakeFiles/test.t.dir/ integtest_main.cc.o"
    elif [[ "$DISTRO" == "centos" ]]; then
        CMD="yum install -y $DEPS_CENTOS"

        # CentOS doesn't package maven directly so we have to install it manually
        CMD="$CMD && wget $MVN_URL"
        CMD="$CMD && sudo tar -xf $MVN_TAR -C /usr/local"

        # Set up PATH and links
        CMD="$CMD && pushd /usr/local"
        CMD="$CMD && sudo ln -s ${MVN_TAR%-bin.tar.gz} maven"
        CMD="$CMD && export M2_HOME=/usr/local/maven"
        CMD="$CMD && export PATH=\${M2_HOME}/bin:\${PATH}"
        CMD="$CMD && popd"

	if [[ "$VER" == "7" ]]; then
	    # CentOS7 has the distro information in /etc/redhat-release
	    CMD="$CMD && cut /etc/redhat-release -d' ' --fields=1,3,4 > /etc/issue"
	fi

        # now the actual command
        CMD="$CMD && make gtest tarball"
    fi

    docker run --rm -t -v $PWD:$PWD -w $PWD $DISTRO:$VER /bin/bash -c "$CMD"
fi

# vim: set tw=0:
