#!/bin/sh

# Author: Thilee Subramaniam
#
# Copyright 2012-2025 Quantcast Corporation. All rights reserved.
#
# This file is part of Quantcast File System (QFS).
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
#
# Helper script to build Java components of QFS.
#

my_max_try=1
work_dir=''
build_vers_git_path=../cc/common/buildversgit.sh

while [ $# -gt 0 ]; do
    if [ x"$1" = x'-r' -a $# -gt 1 ]; then
        shift
        my_max_try=${1-1}
    elif [ x"$1" = x'-d' -a $# -gt 1 ]; then
        shift
        work_dir=$1
    elif [ x"$1" = x'-v' -a $# -gt 1 ]; then
        shift
        build_vers_git_path=$1
    elif [ x"$1" = x'--' ]; then
        break
    elif [ x"$1" = x'-j' ]; then
        echo "Usage:"
        echo "basename $0 [-r <retry count>] [-d <work-dir>] [-v <build-vers-path>] [<hadoop-version> | clean]"
        echo "Script to build QFA Java access library and optionally the Apache Hadoop QFS plugin."
        echo "Supported hadoop versions: 1.0.*, 1.1.*, 0.23.*, 2.*.*, 3.*.*"
        exit 0
    else
        break
    fi
    shift
done

if mvn --version >/dev/null 2>&1; then
    echo "Using Apache Maven to build QFS jars.."
else
    echo "Skipping Java build of QFS. Please install Apache Maven and try again."
    exit 0
fi

if [ x"$work_dir" = x ]; then
    work_dir=$(dirname "$0")
fi
cd "$work_dir" || exit

hadoop_qfs_profile="none"

if [ $# -eq 1 ]; then
    if [ x"$1" = x'clean' ]; then
        mvn clean
        exit
    fi
    if [ x"$1" != x'--' ]; then
        my_version="$(echo "$1" | cut -d. -f 1-2)"
        my_version_maj="$(echo "$1" | cut -d. -f 1)"
        if [ x"$my_version" = x"1.0" -o x"$my_version" = x"1.1" ]; then
            hadoop_qfs_profile="hadoop_branch1_profile"
        elif [ x"$my_version" = x"0.23" ]; then
            hadoop_qfs_profile="hadoop_trunk_profile"
        elif [ x"$my_version_maj" = x"2" -o x"$my_version_maj" = x"3" ]; then
            hadoop_qfs_profile="hadoop_trunk_profile,hadoop_trunk_profile_2"
        else
            echo "Unsupported Hadoop release version."
            exit 1
        fi
    fi
fi

qfs_release_version=$(sh "$build_vers_git_path" --release) &&
    qfs_source_revision=$(sh "$build_vers_git_path" --head) || exit
if [ x"$qfs_source_revision" = x ]; then
    qfs_source_revision="00000000"
fi

test_build_data=${test_build_data:-"/tmp"}

min_supported_release=6
until javac --release $min_supported_release -version >/dev/null 2>&1; do
    if [ $min_supported_release -ge 30 ]; then
        min_supported_release=6
        break
    fi
    min_supported_release=$(expr $min_supported_release + 1)
done
if [ x"${qfs_access_profile-}" = x ]; then
    if [ $min_supported_release -lt 9 ]; then
        qfs_access_profile="qfs_access_java_pre_9"
    else
        qfs_access_profile="qfs_access_java_9"
    fi
fi
min_supported_release=1.$min_supported_release

echo "qfs_release_version = $qfs_release_version"
echo "qfs_source_revision = $qfs_source_revision"
echo "hadoop_qfs_profile  = $hadoop_qfs_profile"
echo "test_build_data     = $test_build_data"
echo "qfs_access_profile  = $qfs_access_profile"
if [ x"$qfs_access_profile" = x'qfs_access_java_9' ]; then
    qfs_access_project='qfs-access'
else
    qfs_access_project='qfs-access-pre-9'
fi

run_maven_exit_if_success() {
    set -x
    mvn \
        -Dhttps.protocols='TLSv1,TLSv1.1,TLSv1.2' \
        -Dmaven.compiler.source="$min_supported_release" \
        -Dmaven.compiler.target="$min_supported_release" \
        -Dqfs.release.version="$qfs_release_version" \
        -Dqfs.source.revision="$qfs_source_revision" \
        -Dtest.build.data="$test_build_data" \
        ${1+"$@"} &&
        exit
    set +x
}

my_try=0
while true; do
    if [ x"$1" = x'--' ]; then
        shift
        run_maven_exit_if_success -P "$qfs_access_profile" ${1+"$@"}
    elif [ x"$hadoop_qfs_profile" = x'none' ]; then
        run_maven_exit_if_success -P "$qfs_access_profile" \
            --projects "$qfs_access_project" package
    else
        run_maven_exit_if_success \
            -P "$hadoop_qfs_profile","$qfs_access_profile" \
            -Dhadoop.release.version="$1" package
    fi
    my_try=$(expr $my_try + 1)
    [ $my_try -lt $my_max_try ] || break
    echo "Retry: $my_try in 20 * $my_try seconds"
    sleep $(expr 20 \* $my_try)
done

exit 1
