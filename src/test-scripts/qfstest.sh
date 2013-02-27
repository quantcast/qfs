#!/bin/sh
#
# $Id$
#
# Created 2010/07/16
# Author: Mike Ovsiannikov
#
# Copyright 2010-2012 Quantcast Corp.
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
#

if [ $# -ge 1 -a x"$1" = x'-valgrind' ]; then
    shift
    myvalgrind='valgrind -v --log-file=valgrind.log --leak-check=full --leak-resolution=high --show-reachable=yes --db-attach=yes --db-command="kill %p"'
    GLIBCPP_FORCE_NEW=1
    export GLIBCPP_FORCE_NEW
    GLIBCXX_FORCE_NEW=1
    export GLIBCXX_FORCE_NEW
fi
export myvalgrind

exec </dev/null
cd ${1-.} || exit

numchunksrv=${numchunksrv-2}
metasrvport=${metasrvport-20200}
testdir=${testdir-`pwd`/`basename "$0" .sh`}

# kfanout_test.sh parameters
fanouttestsize=${fanouttestsize-1e5}
fanoutpartitions=${fanoutpartitions-3}
kfstestnoshutdownwait=${kfstestnoshutdownwait-}

metasrvchunkport=`expr $metasrvport + 100`
chunksrvport=`expr $metasrvchunkport + 100`
metasrvdir="$testdir/meta"
chunksrvdir="$testdir/chunk"
metasrvprop='MetaServer.prp'
metasrvlog='metaserver.log'
pidsuf='.pid'
metasrvpid="metaserver${pidsuf}"
metasrvout='metaserver.out'
chunksrvprop='ChunkServer.prp'
chunksrvlog='chunkserver.log'
chunksrvpid="chunkserver${pidsuf}"
chunksrvout='chunkserver.out'
metahost='127.0.0.1'
clustername='qfs-test-cluster'
if [ x"$myvalgrind" != x ]; then
    metastartwait='yes' # wait for unit test to finish
fi
[ x"`uname`" = x'Darwin' ] && dotusefuser=yes
export myvalgrind

# cptest.sh parameters
sizes=${sizes-'0 1 2 3 127 511 1024 65535 65536 65537 70300 1e5 10e6 100e6 250e6'}
meta=${meta-"-s $metahost -p $metasrvport"}
export sizes
export meta
if find "$0" -type f -print0 2>/dev/null \
        | xargs -0 echo > /dev/null 2>/dev/null; then
    findprint=-print0
    xargsnull=-0
else
    findprint=-print
    xargsnull=''
fi

getpids()
{
    find . -name \*"${pidsuf}" $findprint | xargs $xargsnull cat
}

myrunprog()
{
    p=`which "$1"`
    shift
    if [ x"$myvalgrind" = x ]; then
        exec "$p" ${1+"$@"}
    else
        eval exec "$myvalgrind" '"$p" ${1+"$@"}'
    fi
}

fodir='src/cc/fanout'
smsdir='src/cc/sortmaster'
if [ x"$sortdir" = x -a \( -d "$smsdir" -o -d "$fodir" \) ]; then
    sortdir="`dirname "$0"`/../../../sort"
fi
if [ -d "$sortdir" ]; then
    sortdir=`cd "$sortdir" >/dev/null 2>&1 && pwd`
else
    sortdir=''
fi

if [ x"$sortdir" = x ]; then
    smtest=''
    fodir=''
    fosdir=$fodir
    fotest=0
else
    smdir="$sortdir/$smsdir"
    smdir=`cd "$smdir" >/dev/null 2>&1 && pwd`
    builddir="`pwd`/src/cc"
    export builddir
    metaport=$metasrvport
    export metaport
    smtest="$smdir/sortmaster_test.sh"
    for name in \
            "$smsdir/ksortmaster" \
            "$smtest" \
            "quantsort/quantsort" \
            "$smdir/../../../glue/ksortcontroller" \
            ; do
        if [ ! -x "$name" ]; then
            echo "$name doesn't exist or not executable, skipping sort master test"
            smtest=''
            break
        fi
    done

    fotest=1
    if [ -d "$fodir" ]; then
        fosdir="$sortdir/$fodir"
    else
        echo "$fodir doesn't exist skipping fanout test"
        fodir=''
        fosdir=$fodir
        fotest=0
    fi
fi

accessdir='src/cc/access'
if [ -e "$accessdir/libqfs_access."* -a -x "`which java 2>/dev/null`" ]; then
    kfsjar="`dirname "$0"`"
    kfsjarvers=`$kfsjar/../cc/common/buildversgit.sh -v | head -1`
    kfsjar="`cd "$kfsjar/../../build/java/qfs-access" >/dev/null 2>&1 && pwd`"
    kfsjar="${kfsjar}/qfs-access-${kfsjarvers}.jar"
    if [ -e "$kfsjar" ]; then
        accessdir="`cd "${accessdir}" >/dev/null 2>&1 && pwd`"
    else
        accessdir=''
    fi
else
    accessdir=''
fi

for dir in  \
        'src/cc/devtools' \
        'src/cc/chunk' \
        'src/cc/meta' \
        'src/cc/tools' \
        'src/cc/libclient' \
        'src/cc/kfsio' \
        'src/cc/qcdio' \
        'src/cc/common' \
        'src/cc/qcrs' \
        "`dirname "$0"`" \
        "$fosdir" \
        "$fodir" \
        ; do
    if [ x"${dir}" = x ]; then
        continue;
    fi
    if [ -d "${dir}" ]; then
        dir=`cd "${dir}" >/dev/null 2>&1 && pwd`
    fi
    if [ ! -d "${dir}" ]; then
        echo "missing directory: ${dir}"
        exit 1
    fi
    PATH="${dir}:${PATH}"
    LD_LIBRARY_PATH="${dir}:${LD_LIBRARY_PATH}"
done
# fuser might be in sbin
PATH="${PATH}:/sbin:/usr/sbin"
export PATH
export LD_LIBRARY_PATH
if [ x"$dotusefuser" != x'yes' ]; then
    [ -x "`which fuser 2>/dev/null`" ] || dotusefuser=yes
fi

rm -rf "$testdir"
mkdir "$testdir" || exit
mkdir "$metasrvdir" || exit
mkdir "$chunksrvdir" || exit

ulimit -c unlimited
# Cleanup handler
if [ x"$dotusefuser" = x'yes' ]; then
    trap 'sleep 1; kill -KILL 0' TERM
    trap 'kill -TERM 0' EXIT INT HUP
else
    trap 'cd "$testdir" && find . -type f $findprint | xargs $xargsnull fuser 2>/dev/null | xargs kill -KILL 2>/dev/null' EXIT INT HUP
fi

echo "Starting meta server $metahost:$metasrvport"

cd "$metasrvdir" || exit
mkdir kfscp || exit
mkdir kfslog || exit
cat > "$metasrvprop" << EOF
metaServer.clientPort = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
metaServer.clusterKey = $clustername
metaServer.cpDir = kfscp
metaServer.logDir = kfslog
metaServer.chunkServer.heartbeatTimeout  = 20
metaServer.chunkServer.heartbeatInterval = 50
metaServer.recoveryInterval = 2
metaServer.loglevel = DEBUG
metaServer.csmap.unittest = 1
metaServer.rebalancingEnabled = 1
metaServer.allocateDebugVerify = 1
metaServer.panicOnInvalidChunk = 1
metaServer.clientSM.auditLogging = 1
metaServer.auditLogWriter.logFilePrefixes = audit.log
metaServer.auditLogWriter.maxLogFileSize = 1e9
metaServer.auditLogWriter.maxLogFiles = 5
metaServer.auditLogWriter.waitMicroSec = 36000e6
metaServer.rootDirUser = `id -u`
metaServer.rootDirGroup = `id -g`
metaServer.rootDirMode = 0777
metaServer.verifyAllOpsPermissions = 1
metaServer.maxSpaceUtilizationThreshold = 0.995
EOF

myrunprog metaserver -c "$metasrvprop" "$metasrvlog" > "${metasrvout}" 2>&1 &
metapid=$!
echo "$metapid" > "$metasrvpid"

cd "$testdir" || exit
sleep 3
kill -0 "$metapid" || exit
if [ x"$metastartwait" = x'yes' ]; then
    remretry=20
    echo "Waiting for the meta server to start, the startup unit tests to finish."
    echo "With valgrind meta server unit tests might take serveral minutes."
    until qfsshell -s "$metahost" -p "$metaport" -q -- stat / 1>/dev/null; do
        kill -0 "$metapid" || exit
        remretry=`expr $remretry - 1`
        [ $remretry -le 0 ] && break
        sleep 3
    done
fi

i=$chunksrvport
e=`expr $i + $numchunksrv`
while [ $i -lt $e ]; do
    dir="$chunksrvdir/$i"
    mkdir "$dir" || exit
    mkdir "$dir/kfschunk" || exit
    cat > "$dir/$chunksrvprop" << EOF
chunkServer.metaServer.hostname = $metahost
chunkServer.metaServer.port = $metasrvchunkport
chunkServer.clientPort = $i
chunkServer.clusterKey = $clustername
chunkServer.rackId = 0
chunkServer.chunkDir = kfschunk
chunkServer.logDir = kfslog
chunkServer.diskIo.crashOnError = 1
chunkServer.abortOnChecksumMismatchFlag = 1
chunkServer.msgLogWriter.logLevel = DEBUG
chunkServer.recAppender.closeEmptyWidStateSec = 5
chunkServer.ioBufferPool.partitionBufferCount = 81920
chunkServer.requireChunkHeaderChecksum = 1
EOF
    cd "$dir" || exit
    echo "Starting chunk server $i"
    myrunprog chunkserver "$chunksrvprop" "$chunksrvlog" > "${chunksrvout}" 2>&1 &
    echo $! > "$chunksrvpid"
    i=`expr $i + 1`
done

sleep 3

cd "$testdir" || exit

# Ensure that chunk and meta servers are running.
for pid in `getpids`; do
    kill -0 "$pid" || exit
done

echo "Starting copy test. Test file sizes: $sizes"
# Run normal test first, then rs test.
# Enable read ahead and set buffer size to an odd value.
# For RS disable read ahead and set odd buffer size.
cppidf="cptest${pidsuf}"
{ \
#    cptokfsopts='-W 2 -b 32767 -w 32767' && \
#    export cptokfsopts && \
    cpfromkfsopts='-r 1e6 -w 65537' && \
    export cpfromkfsopts && \
    cptest.sh && \
    mv cptest.log cptest-0.log && \
    cptokfsopts='-S' && \
    export cptokfsopts && \
    cpfromkfsopts='-r 0 -w 65537' && \
    export cpfromkfsopts && \
    cptest.sh; \
} > cptest.out 2>&1 &
cppid=$!
echo "$cppid" > "$cppidf"

if [ $fotest -ne 0 ]; then
    echo "Starting fanout test. Fanout test data size: $fanouttestsize"
    fopidf="kfanout_test${pidsuf}"
    # Do two runs one with connection pool off and on.
    for p in 0 1; do
        kfanout_test.sh \
            -coalesce 1 \
            -host "$metahost" \
            -port "$metasrvport" \
            -size "$fanouttestsize" \
            -partitions "$fanoutpartitions" \
            -read-retries 1 \
            -kfanout-extra-opts "-U $p" \
        || exit
    done > kfanout_test.out 2>&1 &
    fopid=$!
    echo "$fopid" > "$fopidf"
fi

if [ x"$smtest" != x ]; then
    smpidf="sortmaster_test${pidsuf}"
    echo "Starting sort master test"
    "$smtest" > sortmaster_test.out 2>&1 &
    smpid=$!
    echo "$smpid" > "$smpidf"
fi

if [ x"$accessdir" != x ]; then
    kfsaccesspidf="kfsaccess_test${pidsuf}"
    java \
        -Djava.library.path="$accessdir" \
        -classpath "$kfsjar" \
        -Dkfs.euid="`id -u`" \
        -Dkfs.egid="`id -g`" \
        com.quantcast.qfs.access.KfsTest "$metahost" "$metasrvport" \
        > kfsaccess_test.out 2>&1 &
    kfsaccesspid=$!
    echo "$kfsaccesspid" > "$kfsaccesspidf"
fi

wait $cppid
cpstatus=$?
rm "$cppidf"

cat cptest.out

if [ $fotest -ne 0 ]; then
    wait $fopid
    fostatus=$?
    rm "$fopidf"
    cat kfanout_test.out
else
    fostatus=0
fi

if [ x"$smtest" != x ]; then
    wait $smpid
    smstatus=$?
    rm "$smpidf"
    cat sortmaster_test.out
else
    smstatus=0
fi

if [ x"$accessdir" = x ]; then
    kfsaccessstatus=0
else
    wait $kfsaccesspid
    kfsaccessstatus=$?
    rm "$kfsaccesspidf"
    cat kfsaccess_test.out
fi

status=0
cd "$metasrvdir" || exit
echo "Running meta server log compactor"
logcompactor
status=$?

cd "$testdir" || exit

echo "Shutting down"
pids=`getpids`
for pid in $pids; do
    kill -QUIT "$pid" || exit
done

# Wait 30 sec for shutdown to complete
nsecwait=30
i=0
while true; do
    rpids=
    for pid in $pids; do
        if kill -O "$pid" 2>/dev/null; then
            rpids="$pids $pid"
        elif [ x"$kfstestnoshutdownwait" = x ]; then
            wait "$pid"
            estatus=$?
            if [ $estatus -ne 0 ]; then
                status=$estatus;
            fi
        fi
    done
    pids=$rpids
    [ x"$pids" = x ] && break
    i=`expr $i + 1`
    if [ $i -le $nsecwait ]; then
        sleep 1
    else
        kill -ABRT $rpids
        status=1
        break
    fi
done

find "$testdir" -name core\* || status=1

if [ $status -eq 0 -a $cpstatus -eq 0 \
        -a $fostatus -eq 0 -a $smstatus -eq 0 \
        -a $kfsaccessstatus -eq 0 ]; then
    echo "Passed all tests"
else
    echo "Test failure"
    status=1
fi

if [ x"$dotusefuser" = x'yes' ]; then
    trap '' EXIT
fi
exit $status
