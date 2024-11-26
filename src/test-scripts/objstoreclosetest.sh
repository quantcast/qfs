#!/bin/sh
#
# $Id$
#
# Created 2016/04/04
# Author: Mike Ovsiannikov
#
# Copyright 2016 Quantcast Corporation. All rights reserved.
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
# Test object store block close retry correctness, suspending chunk server to
# force lease renew to fail, and the resuming chunk server before issuing
# object block close.
# The test designed to be run after end to end test, "qfstest.h"

ulimit -c unlimited || exit

builddir=`pwd`
toolsdir=${toolsdir-"$builddir"/src/cc/tools}
metadir=${metadir-"$builddir"/src/cc/meta}
chunkdir=${chunkdir-"$builddir"/src/cc/chunk}
devtoolsdir=${devtoolsdir-`dirname "$toolsdir"`/devtools}
qfstestdir=${qfstestdir-"$builddir"/qfstest}
clicfg=${clicfg-"$qfstestdir"/client.prp}
clirootcfg=${clirootcfg-"$qfstestdir"/clientroot.prp}
metaport=${metaport-20200}
metahost=${metahost-127.0.0.1}
csstartport=${csstartport-`expr $metaport + 200`}
csendport=${csendport-`expr $csstartport + 0`}

cptoqfsextraopts=${cptoqfsextraopts-}
logverbose=${logverbose-}
# The following assumes 200 lease re-acquire interval and default 10 sec
# op timeout.
writeflushdelay=5
closedelay=${closedelay-`expr 231 - $writeflushdelay`}

while [ $# -gt 0 ]; do
    if [ x"$1" = x'-close-delay' ]; then
        curoption=$1
        shift
        if [ $# -le 0 ]; then
            echo "$curoption requires numeric argument"
            exit 1
        fi
        closedelay=$1
    elif [ x"$1" = x'-cptoqfs-extra-opts' ]; then
        if [ $# -le 0 ]; then
            echo "$curoption requires numeric argument"
            exit 1
        fi
        cptoqfsextraopts=$1
    elif [ x"$1" = x'-verbose' ]; then
        logverbose='-v'
    else
        echo "Usage: $0 [-close-delay <num>] [cptoqfs-extra-opts <option>] [-verbose]"
        exit 1
    fi
    shift
done

if [ -d "$qfstestdir" ]; then
    true
else
    echo "Directory $qfstestdir does not exist, execute qfstest.sh first."
    exit 1
fi

wait_shutdown_complete()
{
    pid=$0
    maxtry=${1-100}
    k=0
    while kill -0 $pid 2>/dev/null; do
        sleep 1
        k=`expr $k + 1`
        if [ $k -gt $maxtry ]; then
            echo "server $pid shutdown failure" 1>&2
            kill -ABRT $pid
            sleep 3
            kill -KILL $pid 2>/dev/null
            return 1
        fi
    done
    return 0
}

stoptest=1
shutdown()
{
    [ $stoptest -eq 0 ] && return 0
    stoptest=0
    sstatus=0
    cd "$qfstestdir"/meta || return 1
    pid=`cat metaserver.pid`
    kill -QUIT $pid 2>/dev/null
    if wait_shutdown_complete $pid; then
        true;
    else
        sstatus=1
    fi
    cd ../..
    i=$csstartport
    while [ $i -le $csendport ]; do
        cd "$qfstestdir"/chunk/$i || return 1
        pid=`cat chunkserver.pid`
        kill -QUIT `cat chunkserver.pid` 2>/dev/null
        if wait_shutdown_complete $pid; then
            true;
        else
            sstatus=1
        fi
        i=`expr $i + 1`
    done
    return $sstatus
}

trap 'shutdown ; echo "Test failed"' EXIT

shutdown
stoptest=1

cd "$qfstestdir"/meta || exit
kill -KILL `cat metaserver.pid` 2>/dev/null
rm -f kfscp/* kfslog/*
rm -f metaserver.log
cp MetaServer.prp MetaServer-obst.prp
cat >> MetaServer-obst.prp << EOF
    metaServer.recoveryInterval  = 0
    metaServer.csmap.unittest    = 0
    metaServer.clientAuthentication.maxAuthenticationValidTimeSec = 3600
EOF
"$metadir"/metaserver -c MetaServer-obst.prp > metaserver.log 2>&1 || {
    status=$?
    cat metaserver.log
    exit $status
}
"$metadir"/metaserver MetaServer-obst.prp > metaserver.log 2>&1 &
echo $! > metaserver.pid
cd ../.. || exit
i=$csstartport
while [ $i -le $csendport ]; do
    cd "$qfstestdir"/chunk/$i || exit
    kill -KILL `cat chunkserver.pid` 2>/dev/null
    rm -f chunkserver.log
    rm -rf kfschunk*/*
    "$chunkdir"/chunkserver ChunkServer.prp \
        > chunkserver.log 2>&1 &
    echo $! > chunkserver.pid
    cd ../../..  || exit
    i=`expr $i + 1`
done
echo "Waiting for chunk server to connect"
sleep 2
t=0
until "$toolsdir"/qfsadmin -s "$metahost" -p "$metaport" \
            -f "$clirootcfg" upservers 2>/dev/null \
        | awk 'BEGIN{n=0;}{n++;}END{exit(n<1?1:0);}'; do
    t=`expr $t = 1`
    if [ $t -gt 60 ]; then
        echo "wait for chunk servers to connect timed out"
        exit 1
    fi
    sleep 1
done

cat << EOF > "$qfstestdir/exp.test.dat"
    one
    two
EOF

cspidfile=$qfstestdir/chunk/$csendport/chunkserver.pid

echo "Starting test. The test will take about $closedelay seconds."

{
    cat "$qfstestdir/exp.test.dat"
    sleep $writeflushdelay
    kill -STOP `cat "$cspidfile"` || exit
    sleep $closedelay
    kill -CONT `cat "$cspidfile"` || exit
} | "$toolsdir"/cptoqfs -s "$metahost" -p "$metaport" -f "$clicfg" -w 0 -T 8 \
    $cptoqfsextraopts \
    $logverbose \
    -r 0 -k test.dat -d - || exit

"$toolsdir"/cpfromqfs -s "$metahost" -p "$metaport" -f "$clicfg" $logverbose \
    -k test.dat -d "$qfstestdir/test.dat" || exit
cmp "$qfstestdir/exp.test.dat" "$qfstestdir/test.dat" || exit

shutdown || exit
trap '' EXIT
echo "Passed test."
exit 0

