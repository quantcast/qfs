#!/bin/sh
#
# $Id$
#
# Created 2014/09/16
# Author: Mike Ovsiannikov
#
# Copyright 2014 Quantcast Corp.
#
# This file is part of Kosmos File System (KFS).
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
# Hadoop test script wrapper / launcher script intnded to faciliated basic
# hadoop single node testing.
#

mydir="`dirname "$0"`"
mydir="`cd "$mydir" && pwd`"
myqfssrc="`cd "$mydir/../.." && pwd`"

metaserverhost=${metaserverhost-127.0.0.1}
metaserverport=${metaserverport-20000}
qfssrcprefix=${qfssrcprefix-$myqfssrc}
hadoopbin=${hadoopbin-bin/hadoop}

mybuildprefix="$qfssrcprefix/build"
myjbuildprefix="$mybuildprefix/java"
mycbuildprefix="$mybuildprefix/release/lib"

myqfsversion="`sh "$qfssrcprefix/src/cc/common/buildversgit.sh" -v | head -1`"

myhadoop=$hadoopbin
if [ -f "$myhadoop" ]; then
    true
else
    if [ -f bin/hadoop ]; then
        myhadoop="`pwd`/bin/hadoop"
    else
        myhadoop="`which hadoop`"
        if [ x"$myhadoop" = x ]; then
            echo "Error: failed to find hadoop executalbe." 1>&2
            cat << EOF
This script is intended to be executed in hadoops install directory, in place
of bin/hadoop with "jar" and "fs" arguments.
The assumption is that the sample saervers are already succeffully installed and
ruuning, for example using the following:
     ${myqfssrc}/examples/sampleservers/sample_setup.py \
-r ${myqfssrc}/build/release -a install
The following can be used to build required QFS executables and jar files:
    cd ${myqfssrc} && make hadoop-jars
EOF
            exit 1
        fi
    fi
fi
myhadoopvers="`"$myhadoop" version | awk '/^Hadoop /{print $2}'`"

myqfsaccessjar="$myjbuildprefix/qfs-access/qfs-access-${myqfsversion}.jar"
myqfshadoopjar="$myjbuildprefix/hadoop-qfs/hadoop-${myhadoopvers}-qfs-${myqfsversion}.jar"

mystatus=0
if [ -f "${myqfsaccessjar}" ]; then
    true
else
    echo "Error: failed to find: ${myqfsaccessjar}" 1>&2
    mystatus=1
fi
if [ -f "${myqfshadoopjar}" ]; then
    true
else
    echo "Error: failed to find: ${myqfshadoopjar}" 1>&2
fi

if [ $mystatus -ne 0 ]; then
    echo "One of the following can be used to build jar files:"
    echo "cd "$myqfssrc" && make hadoop-jars"
    echo "cd "$myqfssrc" && make ./src/java/javabuild.sh ${myhadoopvers}"
    exit $mystatus
fi

HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${myqfsaccessjar}"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${myqfshadoopjar}"
export HADOOP_CLASSPATH

if [ x"$LD_LIBRARY_PATH" = x ]; then
    JAVA_LIBRARY_PATH="${mycbuildprefix}"
else
    JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:${mycbuildprefix}"
fi
export JAVA_LIBRARY_PATH

if [ x"$LD_LIBRARY_PATH" = x ]; then
    LD_LIBRARY_PATH="${mycbuildprefix}"
else
    LD_LIBRARY_PATH="${mycbuildprefix}:${LD_LIBRARY_PATH}"
fi
export LD_LIBRARY_PATH

mycmd=$1
shift
if [ x"$mycmd" = x"jar" ]; then
    myjarname=$1
    shift
    myprogname=$1
    shift
else
    unset myjarname
    unset myprogname
fi
exec "$myhadoop" \
    "$mycmd" \
    ${myjarname+"$myjarname"} \
    ${myprogname+"$myprogname"} \
    -Dfs.qfs.impl=com.quantcast.qfs.hadoop.QuantcastFileSystem  \
    -Dfs.default.name=qfs://"${metaserverhost}:${metaserverport}" \
    -Dfs.qfs.metaServerHost="${metaserverhost}" \
    -Dfs.qfs.metaServerPort="${metaserverport}" \
    ${1+"$@"}
