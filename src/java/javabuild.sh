#!/bin/sh

# Author: Thilee Subramaniam
#
# Copyright 2012 Quantcast Corp.
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

if [ $# -eq 1 -a x"$1" = x'-h' ]; then
    echo "Usage:"
    echo "basename $0 [<hadoop-version> | clean]"
    echo "Script to build QFA Java access library and optionally the Apache Hadoop QFS plugin."
    echo "Supported hadoop versions: 1.0.*, 1.1.*, 0.23.*, 2.*.*"
    exit 0
fi

cd "`dirname "$0"`"
if [ $? -ne 0 ]; then
    exit 1
fi

if which mvn > /dev/null 2>&1; then
    echo "Using Apache Maven to build QFS jars.."
else
    echo "Skipping Java build of QFS. Please install Apache Maven and try again."
    exit 0
fi

hadoop_qfs_profile="none"

if [ $# -eq 1 ]; then
    if [ x"$1" = x'clean' ]; then
        mvn clean
        exit 0
    fi
    myversion="`echo "$1" | cut -d. -f 1-2`"
    myversionmaj="`echo "$1" | cut -d. -f 1`"
    if [ x"$myversion" = x"1.0"  -o  x"$myversion" = x"1.1" ]; then
        hadoop_qfs_profile="hadoop_branch1_profile"
    elif [ x"$myversion" = x"0.23" ]; then
        hadoop_qfs_profile="hadoop_trunk_profile"
    elif [  x"$myversionmaj" = x"2" ]; then
        hadoop_qfs_profile="hadoop_trunk_profile,hadoop_trunk_profile_2"
    else
        echo "Unsupported Hadoop release version."
        exit 1
    fi
fi

qfs_release_version=`sh ../cc/common/buildversgit.sh -v | head -1`
qfs_source_revision=`sh ../cc/common/buildversgit.sh -v | tail -1`
if [ x"$qfs_source_revision" = x ]; then
    qfs_source_revision="00000000"
fi

test_build_data=${test_build_data:-"/tmp"}

echo "qfs_release_version = $qfs_release_version"
echo "qfs_source_revision = $qfs_source_revision"
echo "hadoop_qfs_profile  = $hadoop_qfs_profile"
echo "test_build_data     = $test_build_data"

if [ x"$hadoop_qfs_profile" = x'none' ]; then
    set -x
    exec mvn -Dqfs.release.version="$qfs_release_version" \
        -Dqfs.source.revision="$qfs_source_revision" \
        -Dtest.build.data="$test_build_data" \
        --projects qfs-access package
else
    set -x
    exec mvn -P "$hadoop_qfs_profile" \
        -Dqfs.release.version="$qfs_release_version" \
        -Dqfs.source.revision="$qfs_source_revision" \
        -Dtest.build.data="$test_build_data" \
        -Dhadoop.release.version="$1" package
fi
