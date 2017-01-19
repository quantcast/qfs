#!/bin/bash
#
# $Id$
#
# Copyright 2016-2017 Quantcast Corporation. All rights reserved.
#
# This file is part of Quantcast File System.
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

################################################################################
# The following is executed on .travis.yml's script section
################################################################################

set -ex

DEPS_UBUNTU='g++ cmake git libboost-regex-dev libkrb5-dev libssl-dev python-dev'
DEPS_UBUNTU=$DEPS_UBUNTU' libfuse-dev default-jdk wget unzip maven sudo passwd'
DEPS_UBUNTU=$DEPS_UBUNTU' curl'

DEPS_CENTOS='gcc-c++ make cmake git boost-devel krb5-devel openssl-devel'
DEPS_CENTOS=$DEPS_CENTOS' python-devel fuse-devel java-openjdk java-devel'
DEPS_CENTOS=$DEPS_CENTOS' libuuid-devel wget unzip sudo which'

MVN_TAR="apache-maven-3.0.5-bin.tar.gz"
MVN_URL="http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.0.5/binaries/$MVN_TAR"

MYTMPDIR='.tmp'
MYCODECOV="$MYTMPDIR/codecov.sh"

MYCMAKE_OPTIONS='-D CMAKE_BUILD_TYPE=RelWithDebInfo'
MYQFSTEST_DIR='build/release/qfstest'

set_sudo()
{
    if [ x"$(id -u)" = x0 ]; then
        MYSUDO=
        if [ $# -gt 0 ]; then
            MYUSER=$1
        fi
        if [ x"$MYUSER" = x ]; then
            MYSU=
        else
            MYSU="sudo -u $MYUSER"
        fi
    else
        MYSUDO='sudo'
        MYSU=
        MYUSER=
    fi
}

tail_logs_and_exit()
{
    if [ -d "$MYQFSTEST_DIR" ]; then
        find "$MYQFSTEST_DIR" -type f -name '*.log' -print0 \
        | xargs -0  tail -n 500
    fi
    exit 1
}

do_build()
{
    $MYSU make -j ${1-2} CMAKE_OPTIONS="$MYCMAKE_OPTIONS" test tarball \
    || tail_logs_and_exit
}

do_build_linux()
{
    if [ -r /proc/cpuinfo ]; then
        cat /proc/cpuinfo
    fi
    df -h || true
    MYCMAKE_OPTIONS="$MYCMAKE_OPTIONS -D QFS_EXTRA_CXX_OPTIONS=-Werror"
    $MYSU make rat clean && do_build
}

init_codecov()
{
    # Run code coverage in docker
    # Pass travis env vars to code coverage.
    mkdir -p  "$MYTMPDIR"
    {
        env | grep -E '^(TRAVIS|CI)' | sed \
            -e "s/'/'\\\''/g"  \
            -e "s/=/=\'/" \
            -e 's/$/'"'/" \
            -e 's/^/export /'
        echo 'curl -s https://codecov.io/bash | /bin/bash'
    } > "$MYCODECOV"
}

build_ubuntu()
{
    # Build and test under qfsbuild user.
    set_sudo 'qfsbuild'
    $MYSUDO apt-get update
    $MYSUDO apt-get install -y $DEPS_UBUNTU
    if [ x"$MYUSER" = x ]; then
        true
    else
        # Create regular user to run the build and test under it.
        id -u "$MYUSER" >/dev/null 2>&1 || useradd -m "$MYUSER"
        chown -R "$MYUSER" .
    fi
    # coverage enabled only generated on ubuntu
    MYCMAKE_OPTIONS="$MYCMAKE_OPTIONS -D ENABLE_COVERAGE=ON"
    do_build_linux
    /bin/bash "$MYCODECOV" || true
}

build_centos()
{
    # Build and test under root, if running as root, to make sure that root
    # build succeeds.
    set_sudo ''
    $MYSUDO yum install -y $DEPS_CENTOS
    # CentOS doesn't package maven directly so we have to install it manually
    wget "$MVN_URL"
    $MYSUDO tar -xf "$MVN_TAR" -C '/usr/local'
    # Set up PATH and links
    (
        cd '/usr/local'
        $MYSUDO ln -snf ${MVN_TAR%-bin.tar.gz} maven
    )
    export M2_HOME='/usr/local/maven'
    export PATH=${M2_HOME}/bin:${PATH}
    rm "$MVN_TAR"
    if [ x"$1" == x'7' ]; then
        # CentOS7 has the distro information in /etc/redhat-release
        $MYSUDO /bin/bash -c \
            "cut /etc/redhat-release -d' ' --fields=1,3,4 > /etc/issue"
    fi
    do_build_linux
}

if [ $# -eq 3 -a x"$1" = x'build' ]; then
    "$1_$2" "$3"
    exit
fi

if [ x"$TRAVIS_OS_NAME" = x'linux' ]; then
    if [ x"$DISTRO" == x'ubuntu' ]; then
        init_codecov
    fi
    MYSRCD="$(pwd)"
    docker run --rm -t -v "$MYSRCD:$MYSRCD" -w "$MYSRCD" "$DISTRO:$VER" \
        /bin/bash ./travis/script.sh build "$DISTRO" "$VER"
elif [ x"$TRAVIS_OS_NAME" = x'osx' ]; then
    MYSSLD='/usr/local/Cellar/openssl/'
    if [ -d "$MYSSLD" ]; then
        MYSSLD="${MYSSLD}$(ls -1 "$MYSSLD" | tail -n 1)"
        MYCMAKE_OPTIONS="$MYCMAKE_OPTIONS -D OPENSSL_ROOT_DIR=${MYSSLD}"
    fi
    sysctl machdep.cpu || true
    df -h || true
    do_build
else
    echo "OS: $TRAVIS_OS_NAME not yet supported"
    exit 1
fi
