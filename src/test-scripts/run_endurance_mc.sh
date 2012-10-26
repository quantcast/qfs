#!/bin/sh

# $Id$
#
# Author: Mike Ovsiannikov
#
# Copyright 2011-2012 Quantcast Corp.
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
# Endurance test. Start meta server, web ui, and 9 chunk servers configured
# with the failure simulation by default (see usage below).
#
# The logic below expects that 4 directories
# /mnt/data{0-5}/<user-name>
# are available and correspond to 4 physical disks.
# 
# 


bdir=`pwd`
PATH="/sbin:/usr/sbin:$PATH"
export PATH

if [ -f ../../CMakeLists.txt ]; then
    ssrcdir="`cd ../.. >/dev/null 2>&1 && pwd`"
else
    ssrcdir="`cd ../.. >/dev/null 2>&1 && pwd`"
fi

srcdir="`dirname "$0"`"
srcdir="`cd "$srcdir/../.." >/dev/null 2>&1 && pwd`"

chunksdir='./chunks'
metasrvchunkport=20100
chunksrvport=30000
clustername='endurance-test'
numchunksrv=3
chunksrvlog='chunkserver.log'
chunksrvout='chunkserver.out'
chunksrvpid='chunkserver.pid'
chunksrvprop='ChunkServer.prp'

clitestdir="/mnt/data3/$USER/test/cli"

metasrvdir="/mnt/data3/$USER/test/meta"
metasrvprop='MetaServer.prp'
metasrvpid='metaserver.pid'
metasrvlog='metaserver.log'
metasrvout='metaserver.out'
metasrvport=20000
wuiport=`expr $metasrvport + 50`

chunkrundirs="/mnt/data[012]/$USER"
chunkbin="$bdir/src/cc/chunk/chunkserver" 
metabin="$bdir/src/cc/meta/metaserver"
webui="$srcdir/webui"
wuiconf='webui.conf'
wuilog='webui.log'
wuipid='webui.pid'
myhost='127.0.0.1'
metahost=$myhost
errsym='yes'
derrsym='no'
smtest='yes'
testonly='no'
mconly='no'
cponly='no'

kill_all_proc()
{
    { find "$@" -type f | xargs fuser | xargs kill -9 ; } >/dev/null 2>&1
}

if [ x"$1" = x'-h' -o x"$1" = x'-help' -o x"$1" = x'--help' ]; then
    echo \
"Usage: $0 {-stop|-get-logs|-status}"'
 -get-logs       -- get names of all log files
 -stop           -- stop (kill -9) all started processes
 -status [<sec>] -- get current status (tail) from the test logs,
    repeat every <num> sec
 -no-err-sym     -- trun off error sumulator
 -testonly       -- start tests only, do not start / restart meta and chunk servers
 -mc-only        -- only start / restart meta and chunk servers
 -cp-only        -- only run copy test, do not run fanout test
 -no-sm-test     -- do not run sort master endurance test
 -disk-err-sym   -- enable disk error sumulation'
    exit 0
fi

excode=0
while [ $# -gt 0 ]; do
    if [ x"$1" = x'-stop' ]; then
        echo "Shutdown all test processes"
        kill_all_proc "$metasrvdir" $chunkrundirs "$clitestdir"
        shift
        excode=1
    elif [ x"$1" = x'-get-logs' ]; then
        find "$metasrvdir" $chunkrundirs "$clitestdir" -type f -name '*.log*'
        shift
        excode=1
    elif [ x"$1" = x'-status' ]; then
        while true; do
            date
            for n in \
                "$clitestdir/fanout/kfanout_test.log" \
                "$clitestdir/sortmaster/sortmaster_endurance_test.log" \
                "$clitestdir/cp/cptest-"{n,rs,tfs}.log \
                ; do
                [ -f "$n" ] || continue
                echo "============== `basename "$n"` ================="
                tail -n 5 "$n"
            done
            [ x"$2" = x ] && break
            [ $2 -le 0  ] && break
            sleep $2
            echo
        done
        excode=1
        break
    elif [ x"$1" = x'-no-err-sym' ]; then
        shift
        errsym='no'
    elif [ x"$1" = x'-disk-err-sym' ]; then
        shift
        derrsym='yes'
    elif [ x"$1" = x'-no-sm-test' ]; then
        shift
        smtest='no'
    elif [ x"$1" = x'-testonly' ]; then
        shift
        testonly='yes'
    elif [ x"$1" = x'-mc-only' ]; then
        shift
        mconly='yes'
    elif [ x"$1" = x'-cp-only' ]; then
        shift
        cponly='yes'
    else
        echo "invalid option: $1"
        excode=1
        break
    fi
done

if [ $excode -ne 0 ]; then
    exit `expr $excode - 1`
fi

for n in "$chunkbin" "$metabin"; do
    if [ ! -x "$n" ]; then
        echo "$n: does not exist or not executable"
        exit 1
    fi
done

if [ x"$errsym" = x'yes' ]; then
    cstimeout=20
    csretry=140 # make wait longer than chunk replication timeout / 5 sec
else
    csretry=-1 # default
    if [ x"$derrsym" = x'yes' ]; then
        cstimeout=30
    else
        cstimeout=8
    fi
fi


ulimit -c unlimited || exit
ulimit -n 65535 || exit
exec 0</dev/null

if [ x"$testonly" != x'yes' ]; then

kill_all_proc "$metasrvdir" $chunkrundirs "$clitestdir"

echo "Starting meta server $metahost:$metasrvport"

(

mkdir -p "$metasrvdir"
cd "$metasrvdir" || exit
kill_all_proc "$metasrvdir"
mkdir -p kfscp || exit
mkdir -p kfslog || exit
metaserverbin="`basename "$metabin"`"
rm -f "$metaserverbin"
cp "$metabin" . || exit
if [ -d "$webui" ]; then
    wdir=`basename "$webui"`
    rm -rf "$wdir"
    cp -a "$webui" . || exit
fi

cat > "$metasrvprop" << EOF
metaServer.clientPort = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
metaServer.clusterKey = $clustername
metaServer.cpDir = kfscp
metaServer.logDir = kfslog
metaServer.recoveryInterval = 1
metaServer.msgLogWriter.logLevel = DEBUG
metaServer.msgLogWriter.maxLogFileSize = 1e9
metaServer.msgLogWriter.maxLogFiles = 20

metaServer.minChunkservers = 1

metaServer.leaseOwnerDownExpireDelay = 60
metaServer.chunkServer.heartbeatTimeout  = 5
metaServer.chunkServer.heartbeatInterval = 3
metaServer.chunkServer.chunkReallocTimeout = 18
metaServer.chunkServer.chunkAllocTimeout = 18
metaServer.chunkServer.makeStableTimeout = 60

metaServer.clientThreadCount = 4
metaServer.maxDownServersHistorySize = 4096
metaServer.maxCSRestarting = 24
metaServer.useFsTotalSpace = 1
# report all fs free space with "useFsTotalSpace = 1"
chunkServer.totalSpace = 109951162777600

metaServer.maxGoodCandidateLoadRatio = 1.2
metaServer.maxGoodMasterLoadRatio = 1.5
metaServer.maxGoodSlaveLoadRatio = 1.8
metaServer.pastEofRecoveryDelay = 31536000
metaServer.inRackPlacementForAppend = 1
metaServer.sortCandidatesByLoadAvg = 1
metaServer.clientSM.maxPendingOps = 32

chunkServer.recAppender.replicationTimeoutSec = 19
chunkServer.remoteSync.responseTimeoutSec = 19
chunkServer.chunkPlacementPendingReadWeight = 1.3
chunkServer.chunkPlacementPendingWriteWeight = 1.3
chunkServer.bufferManager.waitingAvgInterval = 8
chunkServer.diskIo.maxIoTimeSec = 40
chunkServer.forceDeleteStaleChunks = 1
# chunkServer.msgLogWriter.logLevel = ERROR

metaServer.maxSpaceUtilizationThreshold = 0.89
metaServer.maxSlavePlacementRange = 1.2
metaServer.panicOnInvalidChunk = 1
metaServer.rebalancingEnabled = 1
metaServer.allocateDebugVerify = 1

metaServer.clientSM.auditLogging = 1
metaServer.auditLogWriter.logFilePrefixes = audit.log
metaServer.auditLogWriter.maxLogFileSize = 1e9
metaServer.auditLogWriter.maxLogFiles = 5
metaServer.auditLogWriter.waitMicroSec = 36000e6

metaServer.rootDirUser = `id -u`
metaServer.rootDirGroup = `id -g`
metaServer.rootDirMode = 0777

EOF

rm -f *.log*
./"$metaserverbin" -c "$metasrvprop" "$metasrvlog" > "${metasrvout}" 2>&1 &
echo $! > "$metasrvpid"

if [ -d "$wdir" ]; then
    cd "$wdir" || exit
cat > "$wuiconf" << EOF
[webserver]
webServer.metaserverPort = $metasrvport
webServer.mestaserverHost = $metahost
webServer.port = $wuiport
webServer.docRoot = files
webserver.allmachinesfn = /dev/null
webServer.displayPorts = True
[chunk]
refreshInterval = 5
currentSize = 30
currentSpan = 10
hourlySize = 30
hourlySpan =120
daylySize = 24
daylySpan = 3600 
monthlySize = 30
monthlySpan = 86400
displayPorts = True
predefinedHeaders = D-Timer-overrun-count&D-Timer-overrun-sec&XMeta-server-location&Client-active&Buffer-usec-wait-avg&D-CPU-sys&D-CPU-user&D-Disk-read-bytes&D-Disk-read-count&D-Disk-write-bytes&D-Disk-write-count&Write-appenders&D-Disk-read-errors&D-Disk-write-errors&Num-wr-drives&Num-writable-chunks
predefinedChunkDirHeaders = Chunks&Dev-id&Read-bytes&D-Read-bytes&Read-err&D-Read-err&Read-io&D-Read-io&D-Read-time-microsec&Read-timeout&Space-avail&Space-util-pct&Started-ago&Stopped-ago&Write-bytes&D-Write-bytes&Write-err&D-Write-err&Write-io&D-Write-io&D-Write-time-microsec&Write-timeout&Chunk-server&Chunk-dir
EOF
    rm -f *.log*
    trap '' HUP INT
    ./qfsstatus.py "$wuiconf" > "$wuilog" 2>&1 &
    echo $! > "$wuipid"
fi

)

i=$chunksrvport
rack=1
for n in $chunkrundirs; do
    chunksrvdir="$n/test/chunk"
    mkdir -p "$chunksrvdir"
    kill_all_proc "$chunksrvdir"
    rm -f "$chunksrvdir/`basename "$chunkbin"`"
    cp "$chunkbin" "$chunksrvdir" || exit
    e=`expr $i + $numchunksrv`
    while [ $i -lt $e ]; do
        dir="$chunksrvdir/$i"
        mkdir -p "$dir" || exit
        mkdir -p "$dir/kfschunk" || exit
        cat > "$dir/$chunksrvprop" << EOF
chunkServer.metaServer.hostname = $metahost
chunkServer.metaServer.port = $metasrvchunkport
chunkServer.clientPort = $i
chunkServer.clusterKey = $clustername
chunkServer.rackId = $rack
chunkServer.chunkDir = kfschunk kfschunk1 kfschunk2 kfschunk3
chunkServer.diskIo.crashOnError = 1
chunkServer.abortOnChecksumMismatchFlag = 1
chunkServer.requireChunkHeaderChecksum = 1
chunkServer.recAppender.closeEmptyWidStateSec = 5
chunkServer.ioBufferPool.partitionBufferCount = 131072
chunkServer.msgLogWriter.logLevel = DEBUG
chunkServer.msgLogWriter.maxLogFileSize = 1e9
chunkServer.msgLogWriter.maxLogFiles = 2
EOF
        if [ x"$errsym" = x'yes' ]; then
        cat >> "$dir/$chunksrvprop" << EOF
chunkServer.netErrorSimulator = pn=^[^:]*:$metasrvchunkport\$,a=rand+log,int=128,rsleep=30;
EOF
        elif [ x"$derrsym" = x'yes' ]; then
        cat >> "$dir/$chunksrvprop" << EOF
chunkServer.netErrorSimulator = pn=^[^:]*:$metasrvchunkport\$,a=rand+log+err,int=128;
EOF
        fi

        if [ x"$derrsym" = x'yes' ]; then
        cat >> "$dir/$chunksrvprop" << EOF
chunkServer.diskErrorSimulator.minPeriod = 3000
chunkServer.diskErrorSimulator.maxPeriod = 4000
chunkServer.diskErrorSimulator.minTimeMicroSec = 28000000
chunkServer.diskErrorSimulator.maxTimeMicroSec = 50000000
EOF
        fi
        (
        cd "$dir" || exit
        rm -f *.log*
        echo "Starting chunk server $i"
        trap '' HUP INT
        ../chunkserver "$chunksrvprop" "$chunksrvlog" > "${chunksrvout}" 2>&1 &
        echo $! > "$chunksrvpid"
        )
        i=`expr $i + 1`
    done
    rack=`expr $rack + 1`
done

fi
# -test-only

if [ x"$mconly" = x'yes' ]; then
    exit 0
fi

(
    mydir="$clitestdir/cp"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'tests'
    cp -a "$bdir/src/cc/tests" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    cp "$srcdir/src/test-scripts/cptest.sh" . || exit
    cdirp=`pwd`
    PATH="${cdirp}/tests:${cdirp}/devtools:${PATH}"
    export PATH

    meta="-s $metahost -p $metasrvport"
    export meta
    kfstools="`pwd`/tools"
    export kfstools
    cpfromkfsopts="-T $cstimeout -R $csretry"
    export cpfromkfsopts

    for suf in rs n tfs; do
        cptokfsopts="-T $cstimeout -R $csretry"
        if [ x"$suf" = x'rs' ]; then
            cptokfsopts="$cptokfsopts -S"
        elif [ x"$suf" = x'tfs' ]; then
            cptokfsopts="$cptokfsopts -z 0 -y 128 -u 65536 -r 2 -u 65536 -w 267386880"
        fi
        export cptokfsopts

        echo "Starting cptest.sh $suf"
        trap '' HUP INT
        start=$SECONDS
        while ./cptest.sh "$suf"; do
            echo "$suf test passed. `expr $SECONDS - $start` sec, `date`"
            start=$SECONDS
        done > "cptest-$suf.log" 2>&1 &
        echo $! > "cptest-$suf.pid"
    done
)

if [ x"$cponly" = x'yes' ]; then
    exit 0
fi

(
    mydir="$clitestdir/fanout"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'fanout'
    cp -a "$bdir/src/cc/fanout" . || exit
    rm -rf 'tests'
    cp -a "$bdir/src/cc/tests" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    cp  "$ssrcdir/src/cc/fanout/kfanout_test.sh" . || exit
    if [ x"$csretry" != x -a $csretry -gt 0 ]; then
        foretry="-y $csretry"
    else
        foretry=''
    fi
    cdirp=`pwd`
    PATH="${cdirp}/fanout:${cdirp}/tools:${cdirp}/devtools:${cdirp}/tests:${PATH}"
    export PATH

    echo "Starting kfanout_test.sh"
    trap '' HUP INT
    ./kfanout_test.sh \
        -coalesce 1 \
        -host "$metahost" \
        -port "$metasrvport" \
        -size 1e7 \
        -partitions 64 \
        -read-retries 1 \
        -test-runs 100000 \
        -kfanout-extra-opts "-U 1 -c $cstimeout -q 5 $foretry" \
        > kfanout_test.log 2>&1 &
        echo $! > kfanout_test.pid
)

if [ x"$smtest" = x'no' ]; then
    exit 0
fi

[ -e "$bdir/src/cc/sortmaster/ksortmaster" ] || exit

(
    mydir="$clitestdir/sortmaster"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'tests'
    cp -a "$bdir/src/cc/tests" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    rm -rf 'fanout'
    cp -a "$bdir/src/cc/fanout" . || exit
    rm -rf 'sortmaster'
    cp -a "$bdir/src/cc/sortmaster" . || exit
    rm -rf webui
    cp -a "$srcdir/webui/files" webui || exit
    cp  "$ssrcdir/src/cc/sortmaster/endurance_test.sh" . || exit
    cp  "$ssrcdir/src/cc/sortmaster/sortmaster_test.sh" . || exit
    cp  "$ssrcdir/src/cc/sortmaster/testdata.bin" . || exit
    cp -a "$ssrcdir/glue" . || exit
    cp  "$bdir/quantsort/quantsort" . || exit
    cdirp=`pwd`
    PATH="${cdirp}/sortmaster:${cdirp}/fanout:${cdirp}/tools:${cdirp}/devtools:${cdirp}/tests:${PATH}"
    export PATH
    ksortcontroller="${cdirp}/glue/ksortcontroller"
    export ksortcontroller
    quantsort="${cdirp}/quantsort"
    export quantsort
    webuidir="${cdirp}/webui"
    export webuidir
    if [ x"$csretry" != x -a $csretry -gt 0 ]; then
        chunksrvretry="$csretry"
        export chunksrvretry
    fi
    echo "Starting sortmaster_test.sh"
    trap '' HUP INT
    ./endurance_test.sh > sortmaster_endurance_test.log 2>&1 &
    echo $! > endurance_test.pid
)
