#!/bin/sh

# $Id$
#
# Author: Mike Ovsiannikov
#
# Copyright 2011-2012,2016 Quantcast Corporation. All rights reserved.
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
# /mnt/data{0-3}/<user-name>
# are available and correspond to 4 physical disks.
#
#

bdir=$(pwd)
PATH="/sbin:/usr/sbin:$PATH"
export PATH

if [ -f ../../CMakeLists.txt ]; then
    ssrcdir="$(cd ../.. >/dev/null 2>&1 && pwd)"
else
    ssrcdir="$(cd ../.. >/dev/null 2>&1 && pwd)"
fi

srcdir="$(dirname "$0")"
srcdir="$(cd "$srcdir/../.." >/dev/null 2>&1 && pwd)"

testdirsprefix='/mnt/data'
n=3
while [ $n -ge 0 ]; do
    [ -d "${testdirsprefix}$n" ] || break
    n=$(expr $n - 1)
done
if [ $n -ge 0 ]; then
    testdirsprefix="$(basename "$0" .sh)/data"
fi

chunksdir='./chunks'
metasrvchunkport=20100
metasrvchunkporre="$(expr $metasrvchunkport / 10)[0-9]"
chunksrvport=30000
clustername='endurance-test'
numchunksrv=3
chunksrvlog='chunkserver.log'
chunksrvout='chunkserver.out'
chunksrvpid='chunkserver.pid'
chunksrvprop='ChunkServer.prp'

metasrvprop='MetaServer.prp'
metasrvpid='metaserver.pid'
metasrvlog='metaserver.log'
fscklog='fsck.log'
fsckpid='fsck.pid'
metasrvout='metaserver.out'
metasrvport=20000

wuiconf='webui.conf'
wuilog='webui.log'
wuipid='webui.pid'
myhost='127.0.0.1'

errsim='yes'
derrsim='no'
smtest='yes'
testonly='no'
mconly='no'
cponly='no'
myusevalgrind=''

chunkdirerrsim=0
chunkdirerrsimall=0

mkcerts=$(dirname "$0")
mkcerts="$(cd "$mkcerts" && pwd)/qfsmkcerts.sh"

clientuser=${clientuser-"$(id -un)"}
chunkserverclithreads=${chunkserverclithreads-3}
objectstorebuffersize=${objectstorebuffersize-$(expr 512 \* 1024)}

cabundleurl='https://raw.githubusercontent.com/bagder/ca-bundle/master/ca-bundle.crt'
cabundlefileos='/etc/pki/tls/certs/ca-bundle.crt'
prevlogsdir='prev_logs'
vrcount=0

if openssl version | grep 'SSL 0\.' >/dev/null; then
    auth=${auth-no}
else
    auth=${auth-yes}
fi

if [ x"$SECONDS" = x ]; then
    mystartseconds=$(date -u '+%s')
else
    mystartseconds=''
fi

now_seconds() {
    if [ x"$mystartseconds" = x ]; then
        echo $SECONDS
    else
        echo $(($(date -u '+%s') - $mystartseconds))
    fi
}

update_parameters() {
    clitestdir="${testdirsprefix}3/$USER/test/cli"
    metasrvdir="${testdirsprefix}3/$USER/test/meta"
    wuiport=$(expr $metasrvport + 50)
    metavrport=$(expr $metasrvport + 500)

    chunkrundirs="${testdirsprefix}[012]/$USER"
    chunkbin="$bdir/src/cc/chunk/chunkserver"
    metabin="$bdir/src/cc/meta/metaserver"
    fsckbin="$bdir/src/cc/meta/qfsfsck"
    adminbin="$bdir/src/cc/tools/qfsadmin"
    webui="$srcdir/webui"
    metahost=$myhost
    clientprop="$clitestdir/client.prp"
    clientproprs="${clientprop}.rs"
    certsdir="$(dirname "$clitestdir")/certs"
    objectstoredir="${testdirsprefix}3/$USER/test/object_store"
    cabundlefile="$(dirname "$objectstoredir")/ca-bundle.crt"
}

kill_all_proc() {
    if [ x"$(uname)" = x'Darwin' ]; then
        { find "$@" -type f -print0 | xargs -0 lsof -nt -- | xargs kill -KILL; } \
            >/dev/null 2>&1
    else
        { find "$@" -type f | xargs fuser | xargs kill -KILL; } >/dev/null 2>&1
    fi
}

retry_cmd() {
    local trycnt=$1
    shift
    local interval=$1
    shift
    while true; do
        "$@" && break
        [ $trycnt -le 0 ] && return 1
        sleep $interval
        trycnt=$(expr $trycnt - 1)
    done
    return 0
}

show_help() {
    echo \
        "Usage: $0 {-stop|-get-logs|-status}"'
 -get-logs                -- get names of all log files
 -stop                    -- stop (kill -9) all started processes
 -status [<sec>]          -- get current status (tail) from the test logs,
    repeat every <num> sec
 -no-err-sim              -- trun off error simulator
 -testonly                -- start tests only, do not start / restart meta and
    chunk servers
 -mc-only                 -- only start / restart meta and chunk servers
 -cp-only                 -- only run copy test, do not run fanout test
 -no-sm-test              -- do not run sort master endurance test
 -disk-err-sim            -- enable disk error simulation
 -chunk-dir-err-sim-only  -- only run chunk directory error simulation with
    very short replication / recovery timeout to make all replication recovery
    fail. This option only makes sense if the endurance test was previously run
    and enough chunks exits. Presently the test must be stopped with
 -chunk-dir-err-sim-stop option, and then fack and qfsfileenum must be used to
    detect possible problems.
 -chunk-dir-err-sim-stop  -- stop chunk directories failure simulation test.
 -chunk-dir-err-sim <num> -- enable chunk directory check failure simulator on
    firtst <num> chunk servers
 -valgrind                -- run chunk and meta servers under valgrind
 -auth                    -- turn authentication on or off
 -s3                      -- test with AWS S3
 -vr <num>                -- configure to test VR with <num> nodes
 -test-dirs-prefix prefix -- set test directories prefix, default: /mnt/data
 -clean-test-dirs         -- remove test directories
 '
}

pre_run_cleanup() {
    [ $# -ne 1 ] && return 1
    mkdir -p "$1" || return 1
    rm -f "$1"/* 2>/dev/null
    mv *.log* "$1"/ 2>/dev/null
    rm -f core* 2>/dev/null
    return 0
}

run_with_valgrind() {
    if [ x"$myusevalgrind" = x ]; then
        exec ${1+"$@"}
    else
        GLIBCPP_FORCE_NEW=1 \
            GLIBCXX_FORCE_NEW=1 \
            exec valgrind \
            -v \
            --log-file=valgrind.log \
            --leak-check=full \
            --leak-resolution=high \
            --show-reachable=yes \
            --track-origins=yes \
            --child-silent-after-fork=yes \
            --track-fds=yes \
            --num-callers=24 \
            ${1+"$@"}
    fi
}

update_parameters

s3test='no'
excode=0
removetestdirs=0
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
                "$clitestdir/cp/cptest-"*".log" \
                "$metasrvdir/$fscklog"; do
                [ -f "$n" ] || continue
                echo "============== $(basename "$n") ================="
                tail -n 7 "$n"
            done
            [ x"$2" = x ] && break
            [ $2 -le 0 ] && break
            sleep $2
            echo
        done
        excode=1
        break
    elif [ x"$1" = x'-no-err-sim' ]; then
        shift
        errsim='no'
    elif [ x"$1" = x'-disk-err-sim' ]; then
        shift
        derrsim='yes'
    elif [ x"$1" = x'-no-sm-test' ]; then
        shift
        smtest='no'
    elif [ x"$1" = x'-chunk-dir-err-sim' ]; then
        if [ $# -le 1 ]; then
            echo "$1: missing argument"
            excode=1
            break
        fi
        shift
        chunkdirerrsim=$1
        shift
    elif [ x"$1" = x'-testonly' ]; then
        shift
        testonly='yes'
    elif [ x"$1" = x'-mc-only' ]; then
        shift
        mconly='yes'
    elif [ x"$1" = x'-cp-only' ]; then
        shift
        cponly='yes'
    elif [ x"$1" = x'-valgrind' ]; then
        shift
        myusevalgrind='yes'
    elif [ x"$1" = x'-s3' ]; then
        shift
        s3test='yes'
    elif [ x"$1" = x'-auth' ]; then
        shift
        if [ x"$1" = x'on' -o x"$1" = x'ON' -o \
            x"$1" = x'yes' -o x"$1" = x'YES' ]; then
            auth='yes'
        else
            auth='no'
        fi
        [ $# -gt 0 ] && shift
    elif [ x"$1" = x'-chunk-dir-err-sim-only' ]; then
        shift
        mconly='yes'
        errsim='no'
        derrsim='no'
        chunkdirerrsim=0
        chunkdirerrsimall=1
    elif [ x"$1" = x'-chunk-dir-err-sim-stop' ]; then
        shift
        chunkdirerrsimall=-1
    elif [ x"$1" = x'-vr' ]; then
        if [ $# -le 1 ]; then
            echo "$1: missing argument"
            excode=1
            break
        fi
        shift
        if [ $1 -lt 3 ]; then
            echo "$1: invalid argument, must be equal or greater than 3"
            excode=1
            break
        fi
        vrcount=$1
        shift
    elif [ x"$1" = x'-test-dirs-prefix' ]; then
        if [ $# -le 1 ]; then
            echo "$1: missing argument"
            excode=1
            break
        fi
        shift
        testdirsprefix=$1
        update_parameters
        shift
    elif [ x"$1" = x'-h' -o x"$1" = x'-help' -o x"$1" = x'--help' ]; then
        show_help
        excode=1
        break
    elif [ x"$1" = x'-clean-test-dirs' ]; then
        removetestdirs=1
        shift
    else
        echo "invalid option: $1"
        excode=1
        break
    fi
done

if [ $excode -ne 0 ]; then
    exit $(expr $excode - 1)
fi

if [ $removetestdirs -ne 0 ]; then
    echo 'Shutdown all test processes, and removing test directories'
    kill_all_proc "$metasrvdir" $chunkrundirs "$clitestdir"
    rm -rf "${testdirsprefix}"[0123]"/$USER/test"
fi

if [ x"$s3test" = x'yes' ]; then
    if [ x"$QFS_S3_ACCESS_KEY_ID" = x -o \
        x"$QFS_S3_SECRET_ACCESS_KEY" = x -o \
        x"$QFS_S3_BUCKET_NAME" = x ]; then
        echo "environment variables QFS_S3_ACCESS_KEY_ID," \
            "QFS_S3_SECRET_ACCESS_KEY," \
            "and QFS_S3_BUCKET_NAME must be set accordintly"
        exit 1
    fi
fi

for n in "$chunkbin" "$metabin" "$fsckbin" "$adminbin"; do
    if [ ! -x "$n" ]; then
        echo "$n: does not exist or not executable"
        exit 1
    fi
done

tdir="$(dirname "$testdirsprefix")"
if [ x"$tdir" = x'.' -a x"$(basename "$testdirsprefix")" = x'.' ]; then
    testdirsprefix="$testdirsprefix/data"
fi
mkdir -p "$tdir" || exit
tdir="$(cd "$tdir" >/dev/null && pwd)"
[ x"$tdir" = x ] && exit 1
testdirsprefix="$tdir/$(basename "$testdirsprefix")"
for i in 0 1 2 3; do
    mkdir -p "${testdirsprefix}$i/$USER/test"
done

eval $(env | awk '
    BEGIN { FS="="; }
    /QFS_CLIENT_CONFIG/ {
        print "unexport " $1;
        print "unset " $1;
    }')

update_parameters

mkdir -p "$metasrvdir"
cd "$metasrvdir" || exit

if [ $chunkdirerrsimall -lt 0 ]; then
    cd "$metasrvdir" || exit
    if [ -f "$metasrvpid" ]; then
        cat >>"$metasrvprop" <<EOF
chunkServer.dirCheckFailureSimulatorInterval = -1
metaServer.chunkServer.replicationTimeout    = 510
EOF
        kill -HUP $(cat "$metasrvpid")
        exit
    else
        echo "not found: $metasrvpid"
        exit 1
    fi
fi

mkdir -p "$clitestdir" || exit
cp /dev/null "$clientprop" || exit
if [ x"$auth" = x'yes' ]; then
    echo "Authentication on"
    "$mkcerts" "$certsdir" meta root "$clientuser" || exit
    cat >>"$clientprop" <<EOF
client.auth.X509.X509PemFile = $certsdir/$clientuser.crt
client.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
else
    echo "Authentication off"
fi
QFS_CLIENT_CONFIG="FILE:${clientprop}"
export QFS_CLIENT_CONFIG

if [ x"$errsim" = x'yes' ]; then
    cstimeout=20
    csretry=200 # make wait longer than chunk replication timeout / 5 sec
    if [ $vrcount -gt 2 -o -d "$metasrvdir/vr1" ]; then
        cat >>"$clientprop" <<EOF
client.maxNumRetriesPerOp = 200
EOF
    fi
else
    csretry=-1 # default
    if [ x"$derrsim" = x'yes' ]; then
        cstimeout=30
    else
        cstimeout=8
    fi
fi

if [ -f "$metasrvdir/kfscp/latest" ]; then
    i=1
    while [ -d "$metasrvdir/vr$i" ]; do
        i=$(expr $i + 1)
    done
    if [ $i -ge 3 ]; then
        vrcount=$i
    fi
fi
metaserverlocs=''
metaserverextralocs=''
metachunkserverlocs=''
if [ $vrcount -gt 2 ]; then
    csport=$metasrvchunkport
    port=$metasrvport
    endport=$(expr $metasrvport + $vrcount)
    while [ $port -lt $endport ]; do
        metaserverlocs="$metaserverlocs $metahost $port"
        if [ $csport -gt $metasrvchunkport ]; then
            metachunkserverlocs="$metachunkserverlocs $metahost $csport"
        fi
        if [ x"$metahost" = x'127.0.0.1' ]; then
            # Add host name to test resolver.
            metaserverextralocs="$metaserverextralocs localhost $port"
            metachunkserverlocs="$metachunkserverlocs localhost $csport"
        fi
        port=$(expr $port + 1)
        csport=$(expr $csport + 1)
    done
fi

cat >>"$clientprop" <<EOF
client.metaServerNodes = $metaserverlocs $metaserverextralocs
EOF

cp "$clientprop" "$clientproprs" || exit
cat >>"$clientproprs" <<EOF
client.connectionPool = 1
EOF

ulimit -c unlimited || exit
if [ x"$(ulimit -Hn)" = x'unlimited' ]; then
    # Hack around mac os peculiarity.
    ulimit -Hn 65535 2>/dev/null
fi
ulimit -n $(ulimit -Hn) || exit
mycurlimit=$(ulimit -n) || exit
if [ x"$mycurlimit" != x'unlimited' ]; then
    if [ $(ulimit -n) -le 1024 ]; then
        echo "Insufficient open file descriptor limit: $(ulimit -n)"
        exit 1
    fi
fi
exec 0</dev/null

if [ x"$testonly" = x'yes' ]; then
    kill_all_proc "$clitestdir"
else
    kill_all_proc "$metasrvdir" $chunkrundirs "$clitestdir"

    if [ x"$s3test" = x'yes' ]; then
        if [ -f "$cabundlefileos" ]; then
            echo "Using $cabundlefileos"
            cabundlefile=$cabundlefileos
        else
            if [ -x "$(which curl 2>/dev/null)" ]; then
                curl "$cabundleurl" >"$cabundlefile" || exit
            else
                wget "$cabundleurl" -O "$cabundlefile" || exit
            fi
        fi
    else
        mkdir -p "$objectstoredir" || exit
    fi

    echo "Starting meta server $metahost:$metasrvport"

    (
        trap '' EXIT

        mkdir -p "$metasrvdir"
        cd "$metasrvdir" || exit

        kill_all_proc "$metasrvdir"
        mkdir -p kfscp || exit
        mkdir -p kfslog || exit

        metaserverbin="$(basename "$metabin")"
        rm -f "$metaserverbin"
        cp "$metabin" . || exit

        qfsfsckbin="$(basename "$fsckbin")"
        rm -f "$qfsfsckbin"
        cp "$fsckbin" . || exit

        qfsadminbin="$(basename "$adminbin")"
        rm -f "$qfsadminbin"
        cp "$adminbin" . || exit

        if [ -d "$webui" ]; then
            wdir=$(basename "$webui")
            rm -rf "$wdir"
            cp -a "$webui" . || exit
        fi
        metabinmd5=$(openssl md5 </dev/null 2>/dev/null | awk '{print $2}')
        mymd5=$(openssl md5 <./"$metaserverbin" 2>/dev/null | awk '{print $2}')
        metabinmd5="$metabinmd5 $mymd5"
        mymd5=$(echo test | openssl md5 2>/dev/null | awk '{print $2}')
        metabinmd5="$metabinmd5 $mymd5"

        cat >"$metasrvprop" <<EOF
metaServer.clusterKey = $clustername
metaServer.cpDir = kfscp
metaServer.logDir = kfslog
metaServer.checkpoint.lockFileName = ckpt.lock
metaServer.recoveryInterval = 1
metaServer.msgLogWriter.logLevel = DEBUG
metaServer.msgLogWriter.maxLogFileSize = 1e9
metaServer.msgLogWriter.maxLogFiles = 16

metaServer.minChunkservers = 1

metaServer.leaseOwnerDownExpireDelay = 60
metaServer.chunkServer.heartbeatTimeout  = 5
metaServer.chunkServer.heartbeatInterval = 3
metaServer.chunkServer.chunkReallocTimeout = 600
metaServer.chunkServer.chunkAllocTimeout = 18
metaServer.chunkServer.makeStableTimeout = 600

metaServer.clientThreadCount = 4
metaServer.maxDownServersHistorySize = 4096
metaServer.maxCSRestarting = 24
metaServer.useFsTotalSpace = 1

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
chunkServer.diskIo.maxIoTimeSec = 600
chunkServer.forceDeleteStaleChunks = 1
chunkServer.storageTierPrefixes = kfschunk1 0 kfschunk2 14 kfschunk3 9
# chunkServer.msgLogWriter.logLevel = ERROR

metaServer.maxSpaceUtilizationThreshold = 0.89
metaServer.maxSlavePlacementRange = 1.2
metaServer.panicOnInvalidChunk = 1
metaServer.rebalancingEnabled = 1
metaServer.allocateDebugVerify = 1

metaServer.clientSM.auditLogging = 1
metaServer.auditLogWriter.logFilePrefixes = audit.log
metaServer.auditLogWriter.maxLogFileSize = 100e6
metaServer.auditLogWriter.maxLogFiles = 50
metaServer.auditLogWriter.waitMicroSec = 36000e6

metaServer.rootDirUser = $(id -u)
metaServer.rootDirGroup = $(id -g)
metaServer.rootDirMode = 0777

metaServer.appendPlacementIgnoreMasterSlave = 1
metaServer.startupAbortOnPanic = 1
metaServer.debugSimulateDenyHelloResumeInterval = 10
metaServer.debugPanicOnHelloResumeFailureCount = 0
metaServer.helloResumeFailureTraceFileName = helloresumefail.log

metaServer.log.panicOnIoError = 1
metaServer.log.debugCommitted.fileName = committeddebug.txt

metaServer.dataStore.maxCheckpointsToKeepCount = 120

metaServer.objectStoreEnabled = 1
metaServer.objectStoreReadCanUsePoxoyOnDifferentHost = 1
metaServer.objectStoreWriteCanUsePoxoyOnDifferentHost = 1
metaServer.objectStorePlacementTest = 1
metaServer.maxSpaceUtilizationThreshold = 0.995
metaServer.userAndGroup.updatePeriodSec = 30
metaServer.maxDumpsterCleanupInFlight = 8
metaServer.maxTruncateChunksDeleteCount = 1
metaServer.maxTruncatedChunkDeletesInFlight = 18

metaServer.metaMds = $metabinmd5

chunkServer.objBlockDiscardMinMetaUptime = 8

metaServer.ATimeUpdateResolution = 0
metaServer.dirATimeUpdateResolution = 0

metaServer.allowChunkServerRetire = 1
EOF

        if [ x"$auth" = x'yes' ]; then
            cat >>"$metasrvprop" <<EOF
metaServer.clientAuthentication.X509.X509PemFile = $certsdir/meta.crt
metaServer.clientAuthentication.X509.PKeyPemFile = $certsdir/meta.key
metaServer.clientAuthentication.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
metaServer.CSAuthentication.X509.X509PemFile     = $certsdir/meta.crt
metaServer.CSAuthentication.X509.PKeyPemFile     = $certsdir/meta.key
metaServer.CSAuthentication.X509.CAFile          = $certsdir/qfs_ca/cacert.pem
metaServer.cryptoKeys.keysFileName               = keys.txt

metaServer.clientAuthentication.maxAuthenticationValidTimeSec = 20
metaServer.CSAuthentication.maxAuthenticationValidTimeSec     = 20
EOF
        fi

        if [ $chunkdirerrsimall -gt 0 ]; then
            cat >>"$metasrvprop" <<EOF
chunkServer.chunkDirsCheckIntervalSecs       = 15
chunkServer.dirCheckFailureSimulatorInterval = 10
metaServer.chunkServer.replicationTimeout    = 8
EOF
        fi

        pre_run_cleanup "$prevlogsdir" || exit

        if [ -f "kfscp/latest" ]; then
            true
        else
            cp "$metasrvprop" "$metasrvprop.tmp"
            cat >>"$metasrvprop.tmp" <<EOF
metaServer.clientPort      = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
EOF
            if ./"$metaserverbin" -c "$metasrvprop.tmp" >"${metasrvout}" 2>&1; then
                rm "$metasrvprop.tmp"
            else
                status=$?
                cat "${metasrvout}"
                exit $status
            fi
        fi

        if [ $vrcount -gt 2 ]; then
            echo "Setting up VR with $vrcount nodes"
            filesystemid=$(awk '
            BEGIN{FS="/";}
            {
                if ($1 == "filesysteminfo") {
                    print $3;
                    exit;
                }
            }
        ' kfscp/latest)
            if [ x"$filesystemid" = x ]; then
                echo "Failed to determine files system id in kfscp/latest"
                exit 1
            fi
            cat >>"$metasrvprop" <<EOF
metaServer.metaDataSync.fileSystemId = $filesystemid
metaServer.metaDataSync.servers      = $metaserverlocs $metaserverextralocs
# metaServer.vr.ignoreInvalidVrState = 1
EOF
            if [ x"$auth" = x'yes' ]; then
                cat >>"$metasrvprop" <<EOF
metaServer.metaDataSync.auth.X509.X509PemFile = $certsdir/root.crt
metaServer.metaDataSync.auth.X509.PKeyPemFile = $certsdir/root.key
metaServer.metaDataSync.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem

metaServer.log.transmitter.auth.X509.X509PemFile = $certsdir/root.crt
metaServer.log.transmitter.auth.X509.PKeyPemFile = $certsdir/root.key
metaServer.log.transmitter.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem

metaServer.log.receiver.auth.X509.X509PemFile = $certsdir/meta.crt
metaServer.log.receiver.auth.X509.PKeyPemFile = $certsdir/meta.key
metaServer.log.receiver.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
metaServer.log.receiver.auth.whiteList        = root
metaServer.log.receiver.auth.maxAuthenticationValidTimeSec = 30
metaServer.log.receiver.auth.reAuthTimeout                 = 10
EOF
            fi
            i=$vrcount
            myhostname='' # `hostname`
            while [ $i -gt 0 ]; do
                i=$(expr $i - 1)
                if [ $i -gt 0 ]; then
                    vrdir="vr$i"
                    mkdir -p "$vrdir/kfscp" "$vrdir/kfslog" || exit
                    cp "$metasrvprop" "$vrdir/" || exit
                else
                    vrdir='.'
                fi
                port=$(expr $metavrport + $i)
                cport=$(expr $metasrvport + $i)
                csport=$(expr $metasrvchunkport + $i)
                cat >>"$vrdir/$metasrvprop" <<EOF
metaServer.clientPort            = $cport
metaServer.chunkServerPort       = $csport
metaServer.log.receiver.listenOn = 0.0.0.0 $port
EOF
                if [ x = x"$myhostname" ]; then
                    cat >>"$vrdir/$metasrvprop" <<EOF
metaServer.vr.id = $i
EOF
                else
                    cat >>"$vrdir/$metasrvprop" <<EOF
metaServer.vr.hostnameToId = t.$myhostname 999 $myhostname $i t1.$myhostname 998
EOF
                fi
            done

            i=0
            vrdir='.'
            mbdir="$(pwd)"
            while [ $i -lt $vrcount ]; do
                cd "$vrdir" || exit
                if [ $i -gt 0 ]; then
                    pre_run_cleanup "$prevlogsdir" || exit
                fi
                rm -rf "$metasrvlog"
                (
                    trap '' HUP EXIT
                    run_with_valgrind \
                        "$mbdir/$metaserverbin" "$metasrvprop" "$metasrvlog" \
                        >"${metasrvout}" 2>&1 &
                    mpid=$!
                    echo $mpid >"$metasrvpid"
                    kill -0 $mpid
                ) || exit
                i=$(expr $i + 1)
                vrdir="vr$i"
                cd "$mbdir" || exit
            done
            adminclientprop=qfsadmin.prp
            cat >"$adminclientprop" <<EOF
client.metaServerNodes       = $metaserverlocs $metaserverextralocs
EOF
            if [ x"$auth" = x'yes' ]; then
                cat >>"$adminclientprop" <<EOF
client.auth.X509.X509PemFile = $certsdir/root.crt
client.auth.X509.PKeyPemFile = $certsdir/root.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
            fi
            sleep 2 # allow met servers start
            if [ -f './vrstate' ]; then
                true
            else
                nodeidlist=''
                i=0
                while [ $i -lt $vrcount ]; do
                    vrlocation="$metahost $(expr $metavrport + $i)"
                    echo "Adding node $i $vrlocation"
                    retry_cmd 5 5 ./"$qfsadminbin" \
                        -f "$adminclientprop" \
                        -s "$metahost" \
                        -p "$metasrvport" \
                        -F op-type=add-node \
                        -F arg-count=1 \
                        -F node-id="$i" \
                        -F args="$vrlocation" \
                        vr_reconfiguration ||
                        exit
                    nodeidlist="$nodeidlist $i"
                    i=$(expr $i + 1)
                done
                sleep 3 # allow primary to connect to backups.
                # Active all nodes.
                echo "Activating nodes $nodeidlist"
                retry_cmd 5 5 ./"$qfsadminbin" \
                    -f "$adminclientprop" \
                    -s "$metahost" \
                    -p "$metasrvport" \
                    -F op-type=activate-nodes \
                    -F arg-count="$vrcount" \
                    -F args="$nodeidlist" \
                    vr_reconfiguration ||
                    exit
                # Add duplicate channel for node 1
                sleep 1
                vrlocation="$metahost $(expr $metavrport + 1)"
                echo "Adding node  1 listener $vrlocation"
                retry_cmd 5 5 ./"$qfsadminbin" \
                    -f "$adminclientprop" \
                    -s "$metahost" \
                    -p "$metasrvport" \
                    -F op-type=add-node-listeners \
                    -F arg-count=1 \
                    -F node-id=1 \
                    -F args="$vrlocation" \
                    vr_reconfiguration ||
                    exit
                echo "VR status:"
                ./"$qfsadminbin" \
                    -f "$adminclientprop" \
                    -s "$metahost" \
                    -p "$metasrvport" \
                    vr_get_status ||
                    exit
            fi
            # Enable error simulation after VR is configured, as otherwise
            # configuration might fail or take long time.
            if [ x"$errsim" = x'yes' ]; then
                i=0
                vrdir='.'
                cat >>"$vrdir/$metasrvprop" <<EOF
metaServer.log.netErrorSimulator = a=rand+log,int=4096,rsleep=60;
EOF
                while [ $i -lt $vrcount ]; do
                    cat >>"$vrdir/$metasrvprop" <<EOF
metaServer.log.receiver.netErrorSimulator = a=rand+log,int=8192,rsleep=60;
EOF
                    kill -HUP $(cat "$vrdir/$metasrvpid") || exit 1
                    i=$(expr $i + 1)
                    vrdir="vr$i"
                done
            fi
        else
            if [ x"$errsim" = x'yes' ]; then
                cat >>"$metasrvprop" <<EOF
metaServer.log.failureSimulationInterval = 100
EOF
            fi
            cat >>"$metasrvprop" <<EOF
metaServer.clientPort      = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
EOF
            (
                trap '' HUP EXIT
                run_with_valgrind \
                    "$(pwd)/$metaserverbin" \
                    "$metasrvprop" "$metasrvlog" >"${metasrvout}" 2>&1 &
                echo $! >"$metasrvpid"
            ) || exit
        fi

        fsckfailures=0
        fsckruns=0
        while true; do
            sleep $(awk 'BEGIN{printf("%.0f\n", rand() * 100); exit;}')
            if ./"$qfsfsckbin" -A 1 -c kfscp; then
                fsckstatus="OK"
            else
                fsckstatus="FAILED"
                fsckfailures=$(expr $fsckfailures + 1)
            fi
            fsckruns=$(expr $fsckruns + 1)
            echo "==== RUN: $fsckruns FAILURES: $fsckfailures STATUS: $fsckstatus ==="
        done >"$fscklog" 2>&1 &
        echo $! >"$fsckpid"

        if [ -d "$wdir" ]; then
            cd "$wdir" || exit
            unset headerscs
            for h in \
                D-Timer-overrun-count \
                D-Timer-overrun-sec \
                XMeta-location \
                Client-active \
                Buffer-usec-wait-avg \
                D-CPU-sys \
                D-CPU-user \
                D-Disk-read-bytes \
                D-Disk-read-count \
                D-Disk-write-bytes \
                D-Disk-write-count \
                Write-appenders \
                D-Disk-read-errors \
                D-Disk-write-errors \
                Num-wr-drives \
                Num-writable-chunks; do
                headerscs="${headerscs-}${headerscs+&}${h}"
            done
            unset headerscsdirs
            for h in \
                Chunks \
                Dev-id \
                Read-bytes \
                D-Read-bytes \
                Read-err \
                D-Read-err \
                Read-io \
                D-Read-io \
                D-Read-time-microsec \
                Read-timeout \
                Space-avail \
                Space-util-pct \
                Started-ago \
                Stopped-ago \
                Write-bytes \
                D-Write-bytes \
                Write-err \
                D-Write-err \
                Write-io \
                D-Write-io \
                D-Write-time-microsec \
                Write-timeout \
                Chunk-server \
                Chunk-dir; do
                headerscsdirs="${headerscsdirs-}${headerscsdirs+&}${h}"
            done
            cat >"$wuiconf" <<EOF
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
predefinedHeaders = $headerscs
predefinedChunkDirHeaders = $headerscsdirs
EOF
            rm -f *.log*
            trap '' HUP INT
            ./qfsstatus.py "$wuiconf" >"$wuilog" 2>&1 &
            echo $! >"$wuipid"
            kill -0 $(cat "$wuipid") || {
                echo "Failed to start meta server web UI"
                exit 1
            }
        fi
        exit 0
    ) || {
        kill_all_proc "$metasrvdir"
        echo "Failed to start meta server"
        exit 1
    }

    trap 'kill_all_proc "$metasrvdir" $chunkrundirs' EXIT

    i=$chunksrvport
    rack=1
    for n in $chunkrundirs; do
        chunksrvdir="$n/test/chunk"
        mkdir -p "$chunksrvdir"
        kill_all_proc "$chunksrvdir"
        rm -f "$chunksrvdir/$(basename "$chunkbin")"
        cp "$chunkbin" "$chunksrvdir" || exit
        e=$(expr $i + $numchunksrv)
        while [ $i -lt $e ]; do
            dir="$chunksrvdir/$i"
            mkdir -p "$dir" || exit
            (
                cd "$dir" || exit
                mkdir -p "kfschunk" || exit
                pre_run_cleanup "$prevlogsdir" || exit
            ) || exit
            cat >"$dir/$chunksrvprop" <<EOF
chunkServer.metaServer.hostname = $metahost
chunkServer.metaServer.port = $metasrvchunkport
chunkServer.meta.abortOnRequestParseError = 1
chunkServer.clientPort = $i
chunkServer.clusterKey = $clustername
chunkServer.rackId = $rack
chunkServer.chunkDir = kfschunk kfschunk1 kfschunk2 kfschunk3
chunkServer.diskIo.crashOnError = 1
chunkServer.abortOnChecksumMismatchFlag = 1
chunkServer.requireChunkHeaderChecksum = 1
chunkServer.recAppender.closeEmptyWidStateSec = 5
chunkServer.msgLogWriter.logLevel = DEBUG
chunkServer.msgLogWriter.maxLogFileSize = 1e9
chunkServer.msgLogWriter.maxLogFiles = 30
chunkServer.clientThreadCount = $chunkserverclithreads
chunkServer.minChunkCountForHelloResume = 0
chunkServer.helloResumeFailureTraceFileName = helloresumefail.log
EOF
            if [ $(expr $i - $chunksrvport) -lt $chunkdirerrsim ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.chunkDirsCheckIntervalSecs       = 15
chunkServer.dirCheckFailureSimulatorInterval = 10
EOF
            fi
            if [ x"$errsim" = x'yes' ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.netErrorSimulator = pn=^[^:]*:$metasrvchunkporre\$,a=rand+log,int=128,rsleep=30;
chunkServer.recAppender.cleanupSec            = 900
chunkServer.recAppender.closeEmptyWidStateSec = 300
EOF
            elif [ x"$derrsim" = x'yes' ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.netErrorSimulator = pn=^[^:]*:$metasrvchunkporre\$,a=rand+log+err,int=128;
chunkServer.recAppender.cleanupSec            = 1800
chunkServer.recAppender.closeEmptyWidStateSec = 1200
EOF
            fi

            if [ x"$derrsim" = x'yes' ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.diskErrorSimulator.minPeriod = 3000
chunkServer.diskErrorSimulator.maxPeriod = 4000
chunkServer.diskErrorSimulator.minTimeMicroSec = 28000000
chunkServer.diskErrorSimulator.maxTimeMicroSec = 50000000
EOF
            fi
            if [ x"$auth" = x'yes' ]; then
                "$mkcerts" "$certsdir" chunk$i || exit
                cat >>"$dir/$chunksrvprop" <<EOF
chunkserver.meta.auth.X509.X509PemFile = $certsdir/chunk$i.crt
chunkserver.meta.auth.X509.PKeyPemFile = $certsdir/chunk$i.key
chunkserver.meta.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
            fi
            if [ x"$s3test" = x'yes' ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.objectDir                               = s3://aws.
chunkServer.diskQueue.aws.bucketName                = $QFS_S3_BUCKET_NAME
chunkServer.diskQueue.aws.accessKeyId               = $QFS_S3_ACCESS_KEY_ID
chunkServer.diskQueue.aws.secretAccessKey           = $QFS_S3_SECRET_ACCESS_KEY
chunkServer.diskQueue.aws.debugTrace.requestHeaders = 1
chunkServer.diskQueue.aws.ssl.verifyPeer            = 1
chunkServer.diskQueue.aws.ssl.CAFile                = $cabundlefile

# Give the buffer manager the same as with no S3 131072*0.4, appender
# 131072*(1-0.4)*0.4, and the rest to S3 write buffers: ~18 chunks by 64MB
chunkServer.objStoreBufferDataRatio           = 0.3557
chunkServer.recAppender.bufferLimitRatio      = 0.2413
chunkServer.bufferManager.maxRatio            = 0.4022
chunkServer.ioBufferPool.partitionBufferCount = 130382
EOF
            else
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.ioBufferPool.partitionBufferCount = 131072
chunkServer.objStoreBlockWriteBufferSize      = $objectstorebuffersize
chunkServer.objectDir                         = $objectstoredir
EOF
            fi
            if [ $vrcount -gt 2 ]; then
                cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.meta.nodes = $metachunkserverlocs
EOF
            fi
            (
                trap '' EXIT
                cd "$dir" || exit
                echo "Starting chunk server $i"
                trap '' HUP INT
                run_with_valgrind \
                    ../chunkserver "$chunksrvprop" "$chunksrvlog" \
                    >"${chunksrvout}" 2>&1 &
                echo $! >"$chunksrvpid"
            ) || exit
            i=$(expr $i + 1)
        done
        rack=$(expr $rack + 1)
    done

fi
# -test-only

if [ x"$mconly" = x'yes' ]; then
    trap '' EXIT
    exit 0
fi

trap 'kill_all_proc "$metasrvdir" $chunkrundirs' EXIT

(
    trap '' EXIT
    mydir="$clitestdir/cp"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    cp "$srcdir/src/test-scripts/cptest.sh" . || exit
    cdirp=$(pwd)
    PATH="${cdirp}/devtools:${PATH}"
    export PATH

    meta="-s $metahost -p $metasrvport"
    export meta
    kfstools="$(pwd)/tools"
    export kfstools
    cpfromkfsopts="-T $cstimeout -R $csretry"
    export cpfromkfsopts

    for suf in rs n tfs os; do
        cptokfsopts="-T $cstimeout -R $csretry"
        if [ x"$suf" = x'rs' ]; then
            cptokfsopts="$cptokfsopts -S"
        elif [ x"$suf" = x'tfs' ]; then
            cptokfsopts="$cptokfsopts -z 0 -y 128 -u 65536 -r 2 -u 65536 -w 267386880"
        elif [ x"$suf" = x'os' ]; then
            cptokfsopts="$cptokfsopts -r 0"
        fi
        export cptokfsopts

        echo "Starting cptest.sh $suf"
        trap '' HUP INT
        if [ x"$suf" = x"rs" ]; then
            QFS_CLIENT_CONFIG="FILE:${clientproprs}"
            export QFS_CLIENT_CONFIG
        fi
        start=$(now_seconds)
        while ./cptest.sh "$suf"; do
            echo "$suf test passed. $(($(now_seconds) - $start)) sec, $(date)"
            start=$(now_seconds)
        done >"cptest-$suf.log" 2>&1 &
        echo $! >"cptest-$suf.pid"
    done
    exit 0
) || exit 1

if [ x"$cponly" = x'yes' ]; then
    trap '' EXIT
    exit 0
fi

if [ -e "$bdir/src/cc/fanout" ]; then
    true
else
    trap '' EXIT
    exit 0
fi

(
    trap '' EXIT
    mydir="$clitestdir/fanout"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'fanout'
    cp -a "$bdir/src/cc/fanout" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    cp "$ssrcdir/src/cc/fanout/kfanout_test.sh" . || exit
    if [ x"$csretry" != x -a $csretry -gt 0 ]; then
        foretry="-y $csretry"
    else
        foretry=''
    fi
    cdirp=$(pwd)
    PATH="${cdirp}/fanout:${cdirp}/tools:${cdirp}/devtools:${PATH}"
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
        -kfanout-extra-opts "-U 1 -c $cstimeout -q 5 $foretry -P 3" \
        -cpfromkfs-extra-opts "-R $csretry" \
        >kfanout_test.log 2>&1 &
    echo $! >kfanout_test.pid
    exit 0
) || exit

if [ x"$smtest" = x'no' ]; then
    trap '' EXIT
    exit 0
fi

if [ -e "$bdir/src/cc/sortmaster/ksortmaster" ]; then
    true
else
    trap '' EXIT
    exit 0
fi

if [ x"$auth" = x'yes' ]; then
    smauthconf="$clitestdir/sortmasterauth.prp"
    export smauthconf
    cat >"$smauthconf" <<EOF
sortmaster.auth.X509.X509PemFile = $certsdir/$clientuser.crt
sortmaster.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
sortmaster.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
fi

(
    trap '' EXIT
    mydir="$clitestdir/sortmaster"
    kill_all_proc "$mydir"
    rm -rf "$mydir"
    mkdir -p "$mydir" || exit 1
    cd "$mydir" || exit

    rm -rf 'tools'
    cp -a "$bdir/src/cc/tools" . || exit
    rm -rf 'devtools'
    cp -a "$bdir/src/cc/devtools" . || exit
    rm -rf 'fanout'
    cp -a "$bdir/src/cc/fanout" . || exit
    rm -rf 'sortmaster'
    cp -a "$bdir/src/cc/sortmaster" . || exit
    rm -rf webui
    cp -a "$srcdir/webui/files" webui || exit
    cp "$ssrcdir/src/cc/sortmaster/endurance_test.sh" . || exit
    cp "$ssrcdir/src/cc/sortmaster/sortmaster_test.sh" . || exit
    cp "$ssrcdir/src/cc/sortmaster/testdata.bin" . || exit
    cp -a "$ssrcdir/glue" . || exit
    cp "$bdir/quantsort/quantsort" . || exit
    cdirp=$(pwd)
    PATH="${cdirp}/sortmaster:${cdirp}/fanout:${cdirp}/tools:${cdirp}/devtools:${PATH}"
    export PATH
    ksortcontroller="${cdirp}/glue/ksortcontroller"
    export ksortcontroller
    quantsort="${cdirp}/quantsort"
    export quantsort
    webuidir="${cdirp}/webui"
    export webuidir
    metaservernodes=$metaserverlocs
    export metaservernodes
    if [ x"$csretry" != x -a $csretry -gt 0 ]; then
        chunksrvretry="$csretry"
        export chunksrvretry
    fi
    echo "Starting sortmaster_test.sh"
    trap '' HUP INT
    ./endurance_test.sh >sortmaster_endurance_test.log 2>&1 &
    echo $! >endurance_test.pid || exit
    exit 0
) || exit 1

trap '' EXIT
exit 0
