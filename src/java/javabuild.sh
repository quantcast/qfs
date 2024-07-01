#!/bin/sh

# Author: Thilee Subramaniam
#
# Copyright 2012,2016 Quantcast Corporation. All rights reserved.
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

mymaxtry=1
work_dir=''
build_vers_git_path=../cc/common/buildversgit.sh

while [ $# -gt 0 ]; do
    if [ x"$1" = x'-r' -a $# -gt 1 ]; then
        shift
        mymaxtry=${1-1}
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
        myversion="$(echo "$1" | cut -d. -f 1-2)"
        myversionmaj="$(echo "$1" | cut -d. -f 1)"
        if [ x"$myversion" = x"1.0" -o x"$myversion" = x"1.1" ]; then
            hadoop_qfs_profile="hadoop_branch1_profile"
        elif [ x"$myversion" = x"0.23" ]; then
            hadoop_qfs_profile="hadoop_trunk_profile"
        elif [ x"$myversionmaj" = x"2" -o x"$myversionmaj" = x"3" ]; then
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
min_supported_release=1.$min_supported_release

echo "qfs_release_version = $qfs_release_version"
echo "qfs_source_revision = $qfs_source_revision"
echo "hadoop_qfs_profile  = $hadoop_qfs_profile"
echo "test_build_data     = $test_build_data"

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

mytry=0
while true; do
    if [ x"$1" = x'--' ]; then
        shift
        run_maven_exit_if_success ${1+"$@"}
    elif [ x"$hadoop_qfs_profile" = x'none' ]; then
        run_maven_exit_if_success --projects qfs-access package
    else
        run_maven_exit_if_success -P "$hadoop_qfs_profile" \
            -Dhadoop.release.version="$1" package
    fi
    mytry=$(expr $mytry + 1)
    [ $mytry -lt $mymaxtry ] || break
    echo "Retry: $mytry in 20 * $mytry seconds"
    sleep $(expr 20 \* $mytry)
done

exit 1
