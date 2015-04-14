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

while [ $# -ge 1 ]; do
    if [ x"$1" = x'-valgrind' ]; then
        myvalgrind='valgrind -v --log-file=valgrind.log --leak-check=full --leak-resolution=high --show-reachable=yes --track-origins=yes'
        GLIBCPP_FORCE_NEW=1
        export GLIBCPP_FORCE_NEW
        GLIBCXX_FORCE_NEW=1
        export GLIBCXX_FORCE_NEW
    elif [ x"$1" = x'-ipv6' ]; then
        testipv6='yes'
    elif [ x"$1" = x'-noauth' ]; then
        auth='no'
    elif [ x"$1" = x'-auth' ]; then
        auth='no'
    else
        echo "unsupported option: $1" 1>&2
        echo "Usage: $0 [-valgrind] [-ipv6] [-noauth] [-auth]"
        exit 1
    fi
    shift
done
export myvalgrind

exec </dev/null
cd ${1-.} || exit

if openssl version | grep 'OpenSSL 1\.' > /dev/null; then
    auth=${auth-yes}
else
    auth=${auth-no}
fi
if [ x"$auth" = x'yes' ]; then
    echo "Authentication on"
fi

if [ x"$testipv6" = x'yes' ]; then
    metahost='::1'
    metahosturl="[$metahost]"
    iptobind='::'
else
    metahost='127.0.0.1'
    metahosturl=$metahost
    iptobind='0.0.0.0'
fi

clientuser=${clientuser-"`id -un`"}

numchunksrv=${numchunksrv-3}
metasrvport=${metasrvport-20200}
testdir=${testdir-`pwd`/`basename "$0" .sh`}

export metahost
export metasrvport
export metahosturl

unset QFS_CLIENT_CONFIG
unset QFS_CLIENT_CONFIG_127_0_0_1_${metasrvport}

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
metaservercreatefsout='metaservercreatefs.out'
metasrvout='metaserver.out'
chunksrvprop='ChunkServer.prp'
chunksrvlog='chunkserver.log'
chunksrvpid="chunkserver${pidsuf}"
chunksrvout='chunkserver.out'
csallowcleartext=${csallowcleartext-1}
clustername='qfs-test-cluster'
clientprop="$testdir/client.prp"
clientrootprop="$testdir/clientroot.prp"
certsdir=${certsdir-"$testdir/certs"}
minrequreddiskspace=${minrequreddiskspace-6.5e9}
minrequreddiskspacefanoutsort=${minrequreddiskspacefanoutsort-11e9}
chunkserverclithreads=${chunkserverclithreads-3}
mkcerts=`dirname "$0"`
mkcerts="`cd "$mkcerts" && pwd`/qfsmkcerts.sh"

if [ x"$myvalgrind" != x ]; then
    metastartwait='yes' # wait for unit test to finish
fi
[ x"`uname`" = x'Darwin' ] && dontusefuser=yes
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
# Use QFS_CLIENT_CONFIG for sort master.
#    if [ x"$auth" = x'yes' ]; then
#        smauthconf="$testdir/sortmasterauth.prp"
#        export smauthconf
#    fi
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
        'src/cc/qfsc' \
        'src/cc/krb' \
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
if [ x"$dontusefuser" != x'yes' ]; then
    [ -x "`which fuser 2>/dev/null`" ] || dontusefuser=yes
fi

rm -rf "$testdir"
mkdir "$testdir" || exit
mkdir "$metasrvdir" || exit
mkdir "$chunksrvdir" || exit

if [ $fotest -ne 0 ]; then
    mindiskspace=$minrequreddiskspacefanoutsort
else
    mindiskspace=$minrequreddiskspace
fi

df -P -k "$testdir" | awk -v msp="${mindiskspace}" '
    { lns = lns $0 "\n" }
    /^\// {
    if ($4 * 1024 < msp) {
        print lns
        printf(\
            "Insufficient host file system available space:" \
            " %5.2e, at least %5.2e required for the test.\n", \
            $4 * 1024., msp)
        exit 1
    }}' || exit

if [ x"$auth" = x'yes' ]; then
    "$mkcerts" "$certsdir" meta root "$clientuser" || exit
cat > "$clientprop" << EOF
client.auth.X509.X509PemFile = $certsdir/$clientuser.crt
client.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
    QFS_CLIENT_CONFIG="FILE:${clientprop}"
    export QFS_CLIENT_CONFIG
fi

ulimit -c unlimited
# Cleanup handler
if [ x"$dontusefuser" = x'yes' ]; then
    trap 'sleep 1; kill -KILL 0' TERM
    trap 'kill -TERM 0' EXIT INT HUP
else
    trap 'cd "$testdir" && find . -type f $findprint | xargs $xargsnull fuser 2>/dev/null | xargs kill -KILL 2>/dev/null' EXIT INT HUP
fi

echo "Starting meta server $metahosturl:$metasrvport"

cd "$metasrvdir" || exit
mkdir kfscp || exit
mkdir kfslog || exit
cat > "$metasrvprop" << EOF
metaServer.clientIp = $iptobind
metaServer.chunkServerIp = $iptobind
metaServer.clientPort = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
metaServer.clusterKey = $clustername
metaServer.cpDir = kfscp
metaServer.logDir = kfslog
metaServer.chunkServer.heartbeatTimeout  = 20
metaServer.chunkServer.heartbeatInterval = 50
metaServer.recoveryInterval = 2
metaServer.loglevel = DEBUG
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
metaServer.maxSpaceUtilizationThreshold = 0.995
metaServer.clientCSAllowClearText = $csallowcleartext
metaServer.appendPlacementIgnoreMasterSlave = 1
metaServer.clientThreadCount = 2
metaServer.startupAbortOnPanic = 1
EOF

if [ x"$auth" = x'yes' ]; then
    cat >> "$metasrvprop" << EOF
metaServer.clientAuthentication.X509.X509PemFile = $certsdir/meta.crt
metaServer.clientAuthentication.X509.PKeyPemFile = $certsdir/meta.key
metaServer.clientAuthentication.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
metaServer.clientAuthentication.whiteList        = $clientuser root

# Set short valid time to test session time enforcement.
metaServer.clientAuthentication.maxAuthenticationValidTimeSec = 5
# Insure that the write lease is valid for at least 10 min to avoid spurious
# write retries with 5 seconds authentication timeous.
metaServer.minWriteLeaseTimeSec = 600

metaServer.CSAuthentication.X509.X509PemFile     = $certsdir/meta.crt
metaServer.CSAuthentication.X509.PKeyPemFile     = $certsdir/meta.key
metaServer.CSAuthentication.X509.CAFile          = $certsdir/qfs_ca/cacert.pem
metaServer.CSAuthentication.blackList            = none

# Set short valid time to test chunk server re-authentication.
metaServer.CSAuthentication.maxAuthenticationValidTimeSec = 5

metaServer.cryptoKeys.keysFileName               = keys.txt
EOF
fi

metaserver -c "$metasrvprop" > "${metaservercreatefsout}" 2>&1 || {
    status=$?
    cat "${metaservercreatefsout}"
    exit $status
}

cat >> "$metasrvprop" << EOF
metaServer.csmap.unittest = 1
EOF

myrunprog metaserver "$metasrvprop" "$metasrvlog" > "${metasrvout}" 2>&1 &
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
    mkdir "$dir/kfschunk-tier0" || exit
    cat > "$dir/$chunksrvprop" << EOF
chunkServer.clientIp = $iptobind
chunkServer.metaServer.hostname = $metahost
chunkServer.metaServer.port = $metasrvchunkport
chunkServer.clientPort = $i
chunkServer.clusterKey = $clustername
chunkServer.rackId = $i
chunkServer.chunkDir = kfschunk kfschunk-tier0
chunkServer.logDir = kfslog
chunkServer.diskIo.crashOnError = 1
chunkServer.abortOnChecksumMismatchFlag = 1
chunkServer.msgLogWriter.logLevel = DEBUG
chunkServer.recAppender.closeEmptyWidStateSec = 5
chunkServer.ioBufferPool.partitionBufferCount = 8192
chunkServer.bufferManager.maxClientQuota = 2097152
chunkServer.requireChunkHeaderChecksum = 1
chunkServer.storageTierPrefixes = kfschunk-tier0 2
chunkServer.exitDebugCheck = 1
chunkServer.rsReader.debugCheckThread = 1
chunkServer.clientThreadCount = $chunkserverclithreads
# chunkServer.forceVerifyDiskReadChecksum = 1
# chunkServer.debugTestWriteSync = 1
EOF
    if [ x"$auth" = x'yes' ]; then
        "$mkcerts" "$certsdir" chunk$i || exit
        cat >> "$dir/$chunksrvprop" << EOF
chunkserver.meta.auth.X509.X509PemFile = $certsdir/chunk$i.crt
chunkserver.meta.auth.X509.PKeyPemFile = $certsdir/chunk$i.key
chunkserver.meta.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem

EOF
    fi
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

if [ x"$auth" = x'yes' ]; then
    clientdelegation=`qfs \
        -fs "qfs://${metahosturl}:${metasrvport}" \
        -cfg "${clientprop}" -delegate | awk '
    { if ($1 == "Token:") t=$2; else if ($1 == "Key:") k=$2; }
    END{printf("client.auth.psk.key=%s client.auth.psk.keyId=%s", k, t); }'`
    clientenvcfg="${clientdelegation} client.auth.allowChunkServerClearText=0"
else
    clientenvcfg=
fi

echo "Starting copy test. Test file sizes: $sizes"
# Run normal test first, then rs test.
# Enable read ahead and set buffer size to an odd value.
# For RS disable read ahead and set odd buffer size.
cppidf="cptest${pidsuf}"
{
#    cptokfsopts='-W 2 -b 32767 -w 32767' && \
    QFS_CLIENT_CONFIG=$clientenvcfg \
    cptokfsopts='-r 3 -m 1 -l 15' \
    cpfromkfsopts='-r 1e6 -w 65537' \
    cptest.sh && \
    mv cptest.log cptest-0.log && \
    cptokfsopts='-S -m 2 -l 2' \
    cpfromkfsopts='-r 0 -w 65537' \
    cptest.sh; \
} > cptest.out 2>&1 &
cppid=$!
echo "$cppid" > "$cppidf"

if [ x"$auth" = x'yes' ]; then
    cat > "$clientrootprop" << EOF
client.auth.X509.X509PemFile = $certsdir/root.crt
client.auth.X509.PKeyPemFile = $certsdir/root.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
    qfstoolrootauthcfg=$clientrootprop
else
    qfstoolrootauthcfg=
fi

qfstoolpidf="qfstooltest${pidsuf}"
qfstoolmeta="$metahosturl:$metasrvport" \
qfstooltrace=on \
qfstoolrootauthcfg=$qfstoolrootauthcfg \
qfs_tool-test.sh '##??##::??**??~@!#$%^&()=<>`|||' 1>qfs_tool-test.out 2>qfs_tool-test.log &
qfstoolpid=$!
echo "$qfstoolpid" > "$qfstoolpidf"

qfscpidf="qfsctest${pidsuf}"
test-qfsc "$metahost:$metasrvport" 1>test-qfsc.out 2>test-qfsc.log &
qfscpid=$!
echo "$qfscpid" > "$qfscpidf"


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
            -kfanout-extra-opts "-U $p -P 3" \
        || exit
    done > kfanout_test.out 2>&1 &
    fopid=$!
    echo "$fopid" > "$fopidf"
fi

if [ x"$smtest" != x ]; then
   if [ x"$smauthconf" != x ]; then
       cat > "$smauthconf" << EOF
sortmaster.auth.X509.X509PemFile = $certsdir/$clientuser.crt
sortmaster.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
sortmaster.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
    fi
    smpidf="sortmaster_test${pidsuf}"
    echo "Starting sort master test"
    QFS_CLIENT_CONFIG=$clientenvcfg "$smtest" > sortmaster_test.out 2>&1 &
    smpid=$!
    echo "$smpid" > "$smpidf"
fi

if [ x"$accessdir" != x ]; then
    kfsaccesspidf="kfsaccess_test${pidsuf}"
    if [ -f "$clientprop" ]; then
        clientproppool="$clientprop.pool.prp"
        cp "$clientprop" "$clientproppool" || exit
        echo 'client.connectionPool=1' >> "$clientproppool" || exit
        javatestclicfg="FILE:${clientproppool}"
    else
        javatestclicfg="client.connectionPool=1"
    fi
    QFS_CLIENT_CONFIG="$javatestclicfg" \
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

wait $qfstoolpid
qfstoolstatus=$?
rm "$qfstoolpidf"

cat qfs_tool-test.out

wait $qfscpid
qfscstatus=$?
rm "$qfscpidf"

cat test-qfsc.out

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

if [ $status -eq 0 -a $cpstatus -eq 0 -a $qfstoolstatus -eq 0 \
        -a $fostatus -eq 0 -a $smstatus -eq 0 \
        -a $kfsaccessstatus -eq 0 -a $qfscstatus -eq 0 ]; then
    echo "Passed all tests"
else
    echo "Test failure"
    status=1
fi

if [ x"$dontusefuser" = x'yes' ]; then
    trap '' EXIT
fi
exit $status
