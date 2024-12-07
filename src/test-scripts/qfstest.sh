#!/bin/sh
#
# $Id$
#
# Created 2010/07/16
# Author: Mike Ovsiannikov
#
# Copyright 2010-2012,2016 Quantcast Corporation. All rights reserved.
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

csrpctrace=${rpctrace-0}
trdverify=${trdverify-0}
s3debug=0
jerasuretest=''
mycsdebugverifyiobuffers=0
myvalgrindlog='valgrind.log'
myopttestfuse='yes'
chunkserverclithreads=${chunkserverclithreads-3}
metaserverclithreads=${metaserverclithreads-2}
myexmetaconfig=''
myexchunkconfig=''
myexclientconfig=''
installprefix=''
pythonwheeldir=''
mynewlinechar='
'
metasrvdir=

validnumorexit() {
    if [ x"$(expr "$2" - 0 2>/dev/null)" = x ]; then
        echo "invalid argument value $1 $2"
        exit 1
    fi
}

while [ $# -ge 1 ]; do
    if [ x"$1" = x'-valgrind' ]; then
        myvalgrind='valgrind'
        myvalgrind="$myvalgrind"' -v'
        myvalgrind="$myvalgrind"' --log-file='"$myvalgrindlog"
        myvalgrind="$myvalgrind"' --leak-check=full'
        myvalgrind="$myvalgrind"' --leak-resolution=high'
        myvalgrind="$myvalgrind"' --show-reachable=yes'
        myvalgrind="$myvalgrind"' --track-origins=yes'
        myvalgrind="$myvalgrind"' --child-silent-after-fork=yes'
        myvalgrind="$myvalgrind"' --track-fds=yes'
        myvalgrind="$myvalgrind"' --num-callers=24'
        GLIBCPP_FORCE_NEW=1
        export GLIBCPP_FORCE_NEW
        GLIBCXX_FORCE_NEW=1
        export GLIBCXX_FORCE_NEW
    elif [ x"$1" = x'-ipv6' ]; then
        testipv6='yes'
    elif [ x"$1" = x'-noauth' ]; then
        auth='no'
    elif [ x"$1" = x'-s3' ]; then
        s3test='yes'
    elif [ x"$1" = x'-s3debug' ]; then
        s3test='yes'
        s3debug=1
    elif [ x"$1" = x'-auth' ]; then
        auth='yes'
    elif [ x"$1" = x'-csrpctrace' ]; then
        csrpctrace=1
    elif [ x"$1" = x'-jerasure' ]; then
        jerasuretest='yes'
    elif [ x"$1" = x'-no-jerasure' ]; then
        jerasuretest='no'
    elif [ x"$1" = x'-no-fuse' ]; then
        myopttestfuse='no'
    elif [ x"$1" = x'-cs-iobufsverify' ]; then
        mycsdebugverifyiobuffers=1
    elif [ x"$1" = x'-cs-cli-threads' ]; then
        argname=$1
        shift
        chunkserverclithreads=$1
        validnumorexit $argname $chunkserverclithreads
    elif [ x"$1" = x'-meta-cli-threads' ]; then
        argname=$1
        shift
        metaserverclithreads=$1
        validnumorexit $argname $metaserverclithreads
    elif [ x"$1" = x'-meta-ex-config' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        myexmetaconfig=${myexmetaconfig}${mynewlinechar}${1}
    elif [ x"$1" = x'-chunk-ex-config' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        myexchunkconfig=${myexchunkconfig}${mynewlinechar}${1}
    elif [ x"$1" = x'-client-ex-config' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        myexclientconfig=${myexclientconfig}${mynewlinechar}${1}
    elif [ x"$1" = x'-install-prefix' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        installprefix=$1
    elif [ x"$1" = x'-python-wheel-dir' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        pythonwheeldir=$1
    elif [ x"$1" = x'-test-dir' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        testdir=$1
    elif [ x"$1" = x'-meta-test-dir' ]; then
        if [ $# -le 1 ]; then
            echo "invalid argument $1"
        fi
        shift
        metasrvdir=$1
    else
        echo "unsupported option: $1" 1>&2
        echo "Usage: $0 " \
            "[-valgrind]" \
            "[-ipv6]" \
            "[-auth | -noauth]" \
            "[-s3 | -s3debug]" \
            "[-csrpctrace]" \
            "[-trdverify]" \
            "[-jerasure | -no-jerasure]" \
            "[-cs-iobufsverify]" \
            "[-no-fuse]" \
            "[-cs-cli-threads <num>]" \
            "[-meta-cli-threads <num>]" \
            "[-meta-ex-config <param>]" \
            "[-chunk-ex-config <param>]" \
            "[-client-ex-config <param>]" \
            "[-install-prefix <param>]" \
            "[-python-wheel-dir <param>]" \
            "[-test-dir <param>]" \
            "[-meta-test-dir <param>]"
        exit 1
    fi
    shift
done

if [ x"$s3test" = x'yes' ]; then
    if [ x"$QFS_S3_ACCESS_KEY_ID" = x -o \
        x"$QFS_S3_SECRET_ACCESS_KEY" = x -o \
        x"$QFS_S3_BUCKET_NAME" = x ]; then
        echo "environment variables QFS_S3_ACCESS_KEY_ID," \
            "QFS_S3_SECRET_ACCESS_KEY," \
            "QFS_S3_BUCKET_NAME, and optionally" \
            "QFS_S3_REGION_NAME must be set accordintly"
        exit 1
    fi
    if [ x"$QFS_S3_REGION_NAME" = x ]; then
        s3serversideencryption=${s3serversideencryption-0}
    else
        s3serversideencryption=${s3serversideencryption-1}
    fi
fi

export myvalgrind

exec </dev/null
cd ${1-.} || exit

if [ x"$auth" = x ]; then
    if openssl version | grep 'SSL 0\.' >/dev/null; then
        auth='no'
    else
        auth='yes'
    fi
fi

if [ x"$auth" = x'yes' ]; then
    echo "Authentication on"
    openssl version || exit 1
fi

if [ x"$testipv6" = x'yes' ]; then
    metahost='::1'
    metahosturl="[$metahost]"
    iptobind='::'
else
    # metahost='127.0.0.1'
    metahost='localhost'
    metahosturl=$metahost
    iptobind='0.0.0.0'
fi

clientuser=${clientuser-"$(id -un)"}

numchunksrv=${numchunksrv-3}
metasrvport=${metasrvport-20200}
testdir=${testdir-$(pwd)/$(basename "$0" .sh)}
objectstorebuffersize=${objectstorebuffersize-$(expr 500 \* 1024)}

if [ x"$installprefix" = x ]; then
    installbindir=''
    installlibdir=''
else
    installprefix=$(cd "$installprefix" >/dev/null && pwd) || exit
    installbindir=$installprefix/bin
    installlibdir=$installprefix/lib
fi

if [ x"$pythonwheeldir" = x ]; then
    kfspythonwheel=''
else
    pythonwheeldir=$(cd "$pythonwheeldir" >/dev/null && pwd) || exit
    kfspythonwheel=$(find "$pythonwheeldir" -name 'qfs*.whl' -type f)
    if [ ! -f "$kfspythonwheel" ]; then
        echo "$pythonwheeldir: no QFS python wheel found" 1>&2
        exit 1
    fi
    kfspythontest=$(dirname "$0")/../../examples/python
    kfspythontest=$(cd "$kfspythontest" >/dev/null 2>&1 && pwd)
    kfspythontest=$kfspythontest/qfssample.py
    if [ ! -f "$kfspythontest" ]; then
        echo "$kfspythontest: no QFS python test found" 1>&2
        exit 1
    fi
fi

export metahost
export metasrvport
export metahosturl

unset QFS_CLIENT_CONFIG
unset QFS_CLIENT_CONFIG_127_0_0_1_${metasrvport}

# kfanout_test.sh parameters
fanouttestsize=${fanouttestsize-1e5}
fanoutpartitions=${fanoutpartitions-3}
kfstestnoshutdownwait=${kfstestnoshutdownwait-}

metasrvchunkport=$(expr $metasrvport + 100)
chunksrvport=$(expr $metasrvchunkport + 100)
testmetasrvdir=$testdir/meta
metasrvdir=${metasrvdir:-$testmetasrvdir}
chunksrvdir="$testdir/chunk"
metasrvprop='MetaServer.prp'
metasrvlog='metaserver.log'
pidsuf='.pid'
metasrvpid="metaserver${pidsuf}"
metaservercreatefsout='metaservercreatefs.out'
metasrvout='metaserver.out.log'
chunksrvprop='ChunkServer.prp'
chunksrvlog='chunkserver.log'
chunksrvpid="chunkserver${pidsuf}"
chunksrvout='chunkserver.out.log'
csallowcleartext=${csallowcleartext-1}
clustername='qfs-test-cluster'
clientprop="$testdir/client.prp"
clientrootprop="$testdir/clientroot.prp"
certsdir=${certsdir-"$testdir/certs"}
minrequreddiskspace=${minrequreddiskspace-6.5e9}
minrequreddiskspacefanoutsort=${minrequreddiskspacefanoutsort-11e9}
lowrequreddiskspace=${lowrequreddiskspace-20e9}
lowrequreddiskspacefanoutsort=${lowrequreddiskspacefanoutsort-30e9}
csheartbeatinterval=${csheartbeatinterval-5}
cptestextraopts=${cptestextraopts-}
mkcerts=$(dirname "$0")
mkcerts="$(cd "$mkcerts" && pwd)/qfsmkcerts.sh"

[ x"$(uname)" = x'Darwin' ] && dontusefuser=yes
if [ x"$dontusefuser" != x'yes' ]; then
    fuser "$0" >/dev/null 2>&1 || dontusefuser=yes
fi

# cptest.sh parameters
sizes=${sizes-'0 1 2 3 127 511 1024 65535 65536 65537 70300 1e5 10e6 100e6 250e6'}
meta=${meta-"-s $metahost -p $metasrvport"}
export sizes
export meta
if find "$0" -type f -print0 2>/dev/null |
    xargs -0 echo >/dev/null 2>/dev/null; then
    findprint=-print0
    xargsnull=-0
else
    findprint=-print
    xargsnull=''
fi

findpids() {
    find . -name \*"${pidsuf}" $findprint | xargs $xargsnull ${1+"$@"}
}

getpids() {
    findpids cat
}

showpids() {
    findpids grep -v x /dev/null
}

myrunprog() {
    p=$(which "$1")
    shift
    if [ x"$myvalgrind" = x ]; then
        exec "$p" ${1+"$@"}
    else
        eval exec "$myvalgrind" '"$p" ${1+"$@"}'
    fi
}

ensurerunning() {
    rem=${2-10}
    until kill -0 "$1"; do
        rem=$(expr $rem - 1)
        [ $rem -le 0 ] && return 1
        sleep 1
    done
    return 0
}

mytailpids=''

mytailwait() {
    exec tail -1000f "$2" &
    mytailpids="$mytailpids $!"
    wait $1
    myret=$?
    return $myret
}

waitqfscandcptests() {
    mytailwait $qfscpid test-qfsc.out
    qfscstatus=$?
    rm "$qfscpidf"

    if [ x"$accessdir" = x ]; then
        kfsaccessstatus=0
    else
        mytailwait $kfsaccesspid kfsaccess_test.out
        kfsaccessstatus=$?
        rm "$kfsaccesspidf"
    fi

    if [ x"$kfsgopid" = x ]; then
        kfsgostatus=0
    else
        mytailwait $kfsgopid kfsgo_test.out
        kfsgostatus=$?
        rm "$kfsgopidf"
    fi

    if [ x"$kfspythonpid" = x ]; then
        kfspythonstatus=0
    else
        mytailwait $kfspythonpid kfspython_test.out
        kfspythonstatus=$?
        rm "$kfspythonpidf"
    fi

    mytailwait $cppid cptest.out
    cpstatus=$?
    rm "$cppidf"
}

fodir='src/cc/fanout'
smsdir='src/cc/sortmaster'
if [ x"$sortdir" = x -a \( -d "$smsdir" -o -d "$fodir" \) ]; then
    sortdir="$(dirname "$0")/../../../sort"
fi
if [ -d "$sortdir" ]; then
    sortdir=$(cd "$sortdir" >/dev/null 2>&1 && pwd)
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
    smdir=$(cd "$smdir" >/dev/null 2>&1 && pwd)
    if [ x"$installbindir" = x ]; then
        builddir="$(pwd)/src/cc"
    else
        builddir=xxx-sortmaster-do-not-add-build-dirs-to-path
        quantsort=$installbindir/quantsort/quantsort
        export quantsort
    fi
    export builddir
    metaport=$metasrvport
    export metaport
    smtest="$smdir/sortmaster_test.sh"
    if [ x"$myvalgrind" = x ]; then
        smtestqfsvalgrind='no'
    else
        smtestqfsvalgrind='yes'
    fi
    export smtestqfsvalgrind
    # Use QFS_CLIENT_CONFIG for sort master.
    #    if [ x"$auth" = x'yes' ]; then
    #        smauthconf="$testdir/sortmasterauth.prp"
    #        export smauthconf
    #    fi
    for name in \
        "${installbindir:-$smsdir}/ksortmaster" \
        "$smtest" \
        "${installbindir:+$installbindir/}quantsort/quantsort" \
        "$smdir/../../../glue/ksortcontroller"; do
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

accessdir=${installlibdir:-'src/cc/access'}
if [ -e "$accessdir/libqfs_access."* -a -x "$(which java 2>/dev/null)" ]; then
    kfsjar="$(dirname "$0")"
    kfsjarvers=$($kfsjar/../cc/common/buildversgit.sh --release)
    kfsjar="$(cd "$kfsjar/../../build/java/qfs-access" >/dev/null 2>&1 && pwd)"
    kfsjar="${kfsjar}/qfs-access-${kfsjarvers}.jar"
    if [ -e "$kfsjar" ]; then
        accessdir="$(cd "${accessdir}" >/dev/null 2>&1 && pwd)"
    else
        accessdir=''
    fi
else
    accessdir=''
fi

qfscdir=${installlibdir:-$(cd src/cc/qfsc >/dev/null 2>&1 && pwd)}
kfsgosrcdir="$(dirname "$0")"
kfsgosrcdir="$(cd "$kfsgosrcdir/../go" >/dev/null 2>&1 && pwd)"
monitorpluginlib="$(pwd)/$(echo 'contrib/plugins/libqfs_monitor.'*)"

fusedir='src/cc/fuse'
if [ x'yes' = x"$myopttestfuse" -a -d "${installbindir:-$fusedir}" ] &&
    { [ x'Darwin' = x"$(uname)" -a -w /dev/osxfuse0 ] ||
        [ x'FreeBSD' = x"$(uname)" -a -w /dev/fuse ] ||
        { [ x'Linux' = x"$(uname)" -a -w /dev/fuse ] &&
            fusermount -V >/dev/null 2>&1; }; }; then
    testfuse=1
else
    testfuse=0
    fusedir=''
fi

if [ x"$installbindir" = x ]; then
    qfsbindirs=$(
        echo \
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
            'src/cc/emulator'
    )
else
    qfsbindirs=''
    fusedir=''
    fodir=${fodir:+$installbindir/fanout}
fi

qfsshareddirs=''
for dir in \
    $qfsbindirs \
    "${installbindir}" \
    "${installbindir:+$installbindir/tools}" \
    "${installbindir:+$installbindir/devtools}" \
    "${installbindir:+$installbindir/emulator}" \
    "$fusedir" \
    "$(dirname "$0")" \
    "$fosdir" \
    "$fodir"; do
    if [ x"${dir}" = x ]; then
        continue
    fi
    if [ -d "${dir}" ]; then
        dir=$(cd "${dir}" >/dev/null 2>&1 && pwd)
        dname=$(basename "$dir")
        if [ x"$dname" = x'meta' ]; then
            metabindir=$dir
        elif [ x"$dname" = x'chunk' ]; then
            chunkbindir=$dir
        fi
    fi
    if [ ! -d "${dir}" ]; then
        echo "missing directory: ${dir}"
        exit 1
    fi
    PATH="${dir}:${PATH}"
    qfsshareddirs="${dir}${qfsshareddirs:+:$qfsshareddirs}"
done

if [ x"$installlibdir" = x ]; then
    for dir in \
        'gf-complete/lib' \
        'jerasure/lib'; do
        if [ -d "${dir}" ]; then
            dir=$(cd "${dir}" >/dev/null 2>&1 && pwd)
            if [ -d "${dir}" ]; then
                if [ x"$(uname)" = x'Darwin' ]; then
                    # Link on MacOS as run time path has been changed.
                    ln -snf "$dir"/*.dylib src/cc/libclient || exit
                else
                    qfsshareddirs="${dir}${qfsshareddirs:+:$qfsshareddirs}"
                fi
            fi
        fi
    done
else
    qfsshareddirs=$installlibdir
    metabindir=$installbindir
    chunkbindir=$installbindir
fi

# fuser might be in sbin
PATH="${PATH}:/sbin:/usr/sbin"
LD_LIBRARY_PATH="${qfsshareddirs}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PATH
export LD_LIBRARY_PATH

rm -rf "$testdir"
rm -rf "$metasrvdir"
mkdir "$testdir" || exit
mkdir "$metasrvdir" || exit
mkdir "$chunksrvdir" || exit

if [ ! -d "$testmetasrvdir" ]; then
    # Sym link to make other tests work.
    absmetasrvdir=$(cd -- "$metasrvdir" && pwd) &&
        ln -snf "$absmetasrvdir" "$testmetasrvdir" || exit
fi

if [ x"$(uname)" = x'Darwin' ]; then
    # Note: on macos DYLD_LIBRARY_PATH will disappear in sub shell due to
    # integrity system protection.
    DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
    export DYLD_LIBRARY_PATH
fi

cabundlefileos='/etc/pki/tls/certs/ca-bundle.crt'
cabundlefile="$chunksrvdir/ca-bundle.crt"
objectstoredir="$chunksrvdir/object_store"
cabundleurl='https://raw.githubusercontent.com/bagder/ca-bundle/master/ca-bundle.crt'
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
    mkdir "$objectstoredir" || exit
fi

if [ $fotest -ne 0 ]; then
    mindiskspace=$minrequreddiskspacefanoutsort
    lowdiskspace=$lowrequreddiskspacefanoutsort
else
    mindiskspace=$minrequreddiskspace
    lowdiskspace=$lowrequreddiskspace
fi

df -P -k "$testdir" | awk '
    BEGIN {
        msp='"${mindiskspace}"'
        scp='"${lowdiskspace}"'
    }
    {
        lns = lns $0 "\n"
    }
    /^\// {
    asp = $4 * 1024
    if (asp < msp) {
        print lns
        printf(\
            "Insufficient host file system available space:" \
            " %5.2e, at least %5.2e required for the test.\n", \
            asp, msp)
        exit 1
    }
    if (asp < scp) {
        print lns
        printf(\
            "Running tests sequentially due to low disk space:" \
            " %5.2e, at least %5.2e required to run tests concurrently.\n", \
            asp, scp)
        exit 2
    }
    }'
spacecheck=$?
if [ $spacecheck -eq 1 ]; then
    exit 1
fi

if [ x"$auth" = x'yes' ]; then
    "$mkcerts" "$certsdir" meta root "$clientuser" || exit
    cat >"$clientprop" <<EOF
client.auth.X509.X509PemFile = $certsdir/$clientuser.crt
client.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
else
    cp /dev/null "$clientprop"
fi

QFS_CLIENT_CONFIG="FILE:${clientprop}"
export QFS_CLIENT_CONFIG

ulimit -c unlimited
echo "Running RS unit test with 6 data stripes"
mytimecmd='time'
{ $mytimecmd true; } >/dev/null 2>&1 || mytimecmd=
$mytimecmd rstest 6 65536 2>&1 || exit

# Cleanup handler
if [ x"$dontusefuser" = x'yes' ]; then
    trap 'sleep 1; kill -KILL 0' TERM
    trap 'kill -TERM 0' EXIT INT HUP
else
    trap 'cd "$testdir" && find . -type f $findprint | xargs $xargsnull fuser 2>/dev/null | xargs kill -KILL 2>/dev/null' EXIT INT HUP
fi

echo "Starting meta server $metahosturl:$metasrvport"

if [ x"$myvalgrind" = x ]; then
    csheartbeattimeout=60
    csheartbeatskippedinterval=50
    cssessionmaxtime=$(expr $csheartbeatinterval + 10)
    clisessionmaxtime=5
    csmininactivityinterval=25
else
    # Run test sequentially with valgrind
    if [ $spacecheck -ne 2 ]; then
        spacecheck=3
    fi
    csheartbeattimeout=900
    csheartbeatskippedinterval=800
    cssessionmaxtime=$(expr $csheartbeatinterval + 50)
    clisessionmaxtime=25
    csmininactivityinterval=200
    cat >>"$clientprop" <<EOF
client.defaultOpTimeout=600
client.defaultMetaOpTimeout=600
EOF
fi
if [ x"$myexclientconfig" != x ]; then
    cat >>"$clientprop" <<EOF
$myexclientconfig
EOF
fi

cd "$metasrvdir" || exit
mkdir kfscp || exit
mkdir kfslog || exit
cat >"$metasrvprop" <<EOF
metaServer.clientIp = $iptobind
metaServer.chunkServerIp = $iptobind
metaServer.clientPort = $metasrvport
metaServer.chunkServerPort = $metasrvchunkport
metaServer.clusterKey = $clustername
metaServer.cpDir = kfscp
metaServer.logDir = kfslog
metaServer.chunkServer.heartbeatTimeout  = $csheartbeattimeout
metaServer.chunkServer.heartbeatInterval = $csheartbeatinterval
metaServer.chunkServer.heartbeatSkippedInterval = $csheartbeatskippedinterval
metaServer.chunkServer.minInactivityInterval = $csmininactivityinterval
metaServer.recoveryInterval = 2
metaServer.minChunkservers = $numchunksrv
metaServer.loglevel = DEBUG
metaServer.rebalancingEnabled = 1
metaServer.allocateDebugVerify = 1
metaServer.panicOnInvalidChunk = 1
metaServer.panicOnRemoveFromPlacement = 1
metaServer.clientSM.auditLogging = 1
metaServer.clientSM.minProtocolVersion = 10000000
metaServer.auditLogWriter.logFilePrefixes = audit.log
metaServer.auditLogWriter.maxLogFileSize = 1e9
metaServer.auditLogWriter.maxLogFiles = 5
metaServer.auditLogWriter.waitMicroSec = 36000e6
metaServer.rootDirUser = $(id -u)
metaServer.rootDirGroup = $(id -g)
metaServer.rootDirMode = 0777
metaServer.maxSpaceUtilizationThreshold = 0.99999
metaServer.clientCSAllowClearText = $csallowcleartext
metaServer.appendPlacementIgnoreMasterSlave = 1
metaServer.clientThreadCount = $metaserverclithreads
metaServer.startupAbortOnPanic = 1
metaServer.objectStoreEnabled  = 1
metaServer.objectStoreDeleteDelay = 2
metaServer.objectStoreReadCanUsePoxoyOnDifferentHost = 1
metaServer.objectStoreWriteCanUsePoxoyOnDifferentHost = 1
# metaServer.objectStorePlacementTest = 1
metaServer.replicationCheckInterval = 0.5
metaServer.checkpoint.lockFileName = ckpt.lock
metaServer.maxDumpsterCleanupInFlight = 2
metaServer.maxTruncateChunksDeleteCount = 1
metaServer.maxTruncatedChunkDeletesInFlight = 3
metaServer.minWritesPerDrive = 256
metaServer.ATimeUpdateResolution = 0
metaServer.dirATimeUpdateResolution = 0
metaServer.allowChunkServerRetire = 1
metaServer.pingUpdateInterval = 0
metaServer.debugPanicOnHelloResumeFailureCount = 0
metaServer.userAndGroup.updatePeriodSec = 4
metaServer.vr.id = 0
metaServer.log.debugCommitted.size = 111
metaServer.log.debugCommitted.fileName = committeddebug.txt
metaServer.readUseProxyOnDifferentHost = 1
metaServer.readUseProxyOnDifferentHostMode = 1
metaServer.writeUseProxyOnDifferentHost = 1
metaServer.writeUseProxyOnDifferentHostMode = 2
chunkServer.getFsSpaceAvailableIntervalSecs = 2
chunkServer.objecStorageTierPrefixes = s3://__test_0 0 __test_2 2
EOF

if [ x"$myvalgrind" != x ]; then
    cat >>"$metasrvprop" <<EOF
metaServer.chunkServer.chunkAllocTimeout   = 500
metaServer.chunkServer.chunkReallocTimeout = 500
EOF
fi

if [ x"$auth" = x'yes' ]; then
    cat >>"$metasrvprop" <<EOF
metaServer.clientAuthentication.X509.X509PemFile = $certsdir/meta.crt
metaServer.clientAuthentication.X509.PKeyPemFile = $certsdir/meta.key
metaServer.clientAuthentication.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
metaServer.clientAuthentication.whiteList        = $clientuser root

# Set short valid time to test session time enforcement.
metaServer.clientAuthentication.maxAuthenticationValidTimeSec = $clisessionmaxtime
# Insure that the write lease is valid for at least 10 min to avoid spurious
# write retries with 5 seconds authentication timeous.
metaServer.minWriteLeaseTimeSec = 600

metaServer.CSAuthentication.X509.X509PemFile     = $certsdir/meta.crt
metaServer.CSAuthentication.X509.PKeyPemFile     = $certsdir/meta.key
metaServer.CSAuthentication.X509.CAFile          = $certsdir/qfs_ca/cacert.pem
metaServer.CSAuthentication.blackList            = none

# Set short valid time to test chunk server re-authentication.
metaServer.CSAuthentication.maxAuthenticationValidTimeSec = $cssessionmaxtime

metaServer.cryptoKeys.keysFileName               = keys.txt
EOF
fi

# Test meta server distributing S3 configuration to chunk servers.
if [ x"$s3test" = x'yes' ]; then
    cat >>"$metasrvprop" <<EOF
chunkServer.diskQueue.aws.bucketName                 = $QFS_S3_BUCKET_NAME
chunkServer.diskQueue.aws.accessKeyId                = $QFS_S3_ACCESS_KEY_ID
chunkServer.diskQueue.aws.secretAccessKey            = $QFS_S3_SECRET_ACCESS_KEY
chunkServer.diskQueue.aws.region                     = $QFS_S3_REGION_NAME
chunkServer.diskQueue.aws.useServerSideEncryption    = $s3serversideencryption
chunkServer.diskQueue.aws.ssl.verifyPeer             = 1
chunkServer.diskQueue.aws.ssl.CAFile                 = $cabundlefile
chunkServer.diskQueue.aws.debugTrace.requestHeaders  = $s3debug
chunkServer.diskQueue.aws.debugTrace.requestProgress = $s3debug
EOF
else
    # Create fake bucket to add default tier with non empty
    # chunkServer.objecStorageTierPrefixes
    cat >>"$metasrvprop" <<EOF
chunkServer.diskQueue._test.bucketName = fale_default_15
EOF
fi

QFS_DEBUG_CHECK_LEAKS_ON_EXIT=1
export QFS_DEBUG_CHECK_LEAKS_ON_EXIT

"$metabindir"/metaserver \
    -c "$metasrvprop" >"${metaservercreatefsout}" 2>&1 || {
    status=$?
    cat "${metaservercreatefsout}"
    exit $status
}

cat >>"$metasrvprop" <<EOF
metaServer.csmap.unittest = 1
EOF

if [ x"$myexmetaconfig" != x ]; then
    cat >>"$metasrvprop" <<EOF
$myexmetaconfig
EOF
fi

echo "Sync before to starting tests."
sync

if [ x"$myvalgrind" = x ]; then
    myqfsleakscheck=0
else
    myqfsleakscheck=1
fi

QFS_DEBUG_CHECK_LEAKS_ON_EXIT=$myqfsleakscheck \
    myrunprog "$metabindir"/metaserver \
    "$metasrvprop" "$metasrvlog" >"${metasrvout}" 2>&1 &
metapid=$!
echo "$metapid" >"$metasrvpid"

cd "$testdir" || exit
ensurerunning "$metapid" || exit
echo "Waiting for the meta server startup unit tests to complete."
if [ x"$myvalgrind" != x ]; then
    echo "With valgrind meta server unit tests might take serveral minutes."
fi

myfsurl="qfs://${metahost}:${metasrvport}/"

runqfsuser() {
    qfs -D fs.msgLogWriter.logLevel=ERROR -fs "$myfsurl" ${1+"$@"}
}

remretry=20
until runqfsuser -test -e / 1>/dev/null; do
    kill -0 "$metapid" || exit
    remretry=$(expr $remretry - 1)
    if [ $remretry -le 0 ]; then
        echo "Wait for meta server startup timed out."
        exit 1
    fi
    sleep 3
done

if [ x"$myvalgrind" = x ]; then
    csmetainactivitytimeout=180
else
    csmetainactivitytimeout=300
fi

i=$chunksrvport
e=$(expr $i + $numchunksrv)
while [ $i -lt $e ]; do
    dir="$chunksrvdir/$i"
    mkdir "$dir" || exit
    mkdir "$dir/kfschunk" || exit
    mkdir "$dir/kfschunk-tier0" || exit
    cat >"$dir/$chunksrvprop" <<EOF
chunkServer.clientIp = $iptobind
chunkServer.metaServer.hostname = $metahost
chunkServer.metaServer.port = $metasrvchunkport
chunkServer.meta.abortOnRequestParseError = 1
chunkServer.clientPort = $i
chunkServer.clusterKey = $clustername
chunkServer.rackId = $i
chunkServer.chunkDir = kfschunk kfschunk-tier0 non-existent-dir
chunkServer.logDir = kfslog
chunkServer.diskIo.crashOnError = 1
chunkServer.diskIo.debugValidateIoBuffers = 1
chunkServer.diskIo.debugPinIoBuffers = 1
chunkServer.diskIo.debugVerifyIoBuffers = $mycsdebugverifyiobuffers
chunkServer.abortOnChecksumMismatchFlag = 1
chunkServer.msgLogWriter.logLevel = DEBUG
chunkServer.recAppender.closeEmptyWidStateSec = 5
chunkServer.bufferManager.maxClientQuota = 4202496
chunkServer.requireChunkHeaderChecksum = 1
chunkServer.storageTierPrefixes = kfschunk-tier0 2
chunkServer.exitDebugCheck = 1
chunkServer.rsReader.debugCheckThread = 1
chunkServer.clientThreadCount = $chunkserverclithreads
chunkServer.forceVerifyDiskReadChecksum = $trdverify
chunkServer.debugTestWriteSync = $twsync
chunkServer.clientSM.traceRequestResponse   = $csrpctrace
chunkServer.remoteSync.traceRequestResponse = $csrpctrace
chunkServer.meta.traceRequestResponseFlag   = $csrpctrace
chunkServer.placementMaxWaitingAvgSecsThreshold = 600
chunkServer.maxSpaceUtilizationThreshold = 0.00001
chunkServer.meta.inactivityTimeout = $csmetainactivitytimeout
chunkServer.minChunkCountForHelloResume = 0
chunkServer.recAppender.dropLockMinSize = 0
# chunkServer.recAppender.relockDebugRnd = 1024
# chunkServer.forceVerifyDiskReadChecksum = 1
# chunkServer.debugTestWriteSync = 1
# chunkServer.diskQueue.trace = 1
# chunkServer.diskQueue.maxDepth = 8
# chunkServer.diskErrorSimulator.enqueueFailInterval = 5
EOF
    if [ $i -eq $chunksrvport ]; then
        cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.hostname = 0.0.0.0
chunkServer.nodeId = FILE:$chunksrvprop
EOF
    else
        cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.nodeId = ${i}_node_id
EOF
    fi
    if [ x"$myvalgrind" != x ]; then
        cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.diskIo.maxIoTimeSec = 580
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
chunkServer.objectDir = s3://aws.
# Give the buffer manager the same as with no S3 8192*0.4, appender
# 8192*(1-0.4)*0.4, and the rest to S3 write buffers: 16 chunks by 10MB + 64KB
# buffer for each.
chunkServer.objStoreBufferDataRatio           = 0.8871
chunkServer.recAppender.bufferLimitRatio      = 0.0424
chunkServer.bufferManager.maxRatio            = 0.0705
chunkServer.ioBufferPool.partitionBufferCount = 46460
EOF
    else
        cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.ioBufferPool.partitionBufferCount = 8192
chunkServer.objStoreBlockWriteBufferSize      = $objectstorebuffersize
chunkServer.objectDir                         = $objectstoredir
EOF
    fi
    if [ $(expr $chunksrvport + 1) -eq $i ]; then
        cat >>"$dir/$chunksrvprop" <<EOF
chunkServer.resolverCacheExpiration = 5
chunkServer.useOsResolver           = 1
EOF
    fi
    if [ x"$myexchunkconfig" != x ]; then
        cat >>"$dir/$chunksrvprop" <<EOF
$myexchunkconfig
EOF
    fi
    cd "$dir" || exit
    echo "Starting chunk server $i"
    myrunprog "$chunkbindir"/chunkserver \
        "$chunksrvprop" "$chunksrvlog" >"${chunksrvout}" 2>&1 &
    echo $! >"$chunksrvpid"
    i=$(expr $i + 1)
done

cd "$testdir" || exit

# Ensure that chunk and meta servers are running.
for pid in $(getpids); do
    ensurerunning "$pid" || exit
done

if [ x"$auth" = x'yes' ]; then
    clientdelegation=$(runqfsuser -delegate | awk '
    { if ($1 == "Token:") t=$2; else if ($1 == "Key:") k=$2; }
    END{printf("client.auth.psk.key=%s client.auth.psk.keyId=%s", k, t); }')
    clientenvcfg="${clientdelegation} client.auth.allowChunkServerClearText=0"

    cat >"$clientrootprop" <<EOF
client.auth.X509.X509PemFile = $certsdir/root.crt
client.auth.X509.PKeyPemFile = $certsdir/root.key
client.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
else
    clientenvcfg=
    cat >"$clientrootprop" <<EOF
client.euser=0
EOF
fi
qfstoolrootauthcfg=$clientrootprop
clientenvcfg="${clientenvcfg} client.nodeId=$(expr $chunksrvport + 1)_node_id"

if [ x"$myvalgrind" != x ]; then
    cat >>"$clientrootprop" <<EOF
client.defaultOpTimeout=600
client.defaultMetaOpTimeout=600
EOF
fi

runqfsadmin() {
    QFS_CLIENT_CONFIG= \
        qfsadmin -s "$metahost" -p "$metasrvport" -f "$clientrootprop" ${1+"$@"}
}

report_test_status() {
    if [ $2 -ne 0 ]; then
        echo "$1 test failure"
    else
        echo "$1 test passed"
    fi
}

metaserversetparameter() {
    {
        cat >>"$metasrvdir/$metasrvprop" <<EOF
$1
EOF
    } || exit
    kill -HUP $metapid || exit

    # Ensure that parameter has changed.
    i=10
    until runqfsadmin ping |
        grep -E 'Config:.*( |;)'"$1"'(;|$)' \
            >/dev/null; do
        i=$(expr $i - 1)
        if [ $i -le 0 ]; then
            echo "meta server parameter update has failed" 1>&2
            exit 1
        fi
        sleep 1
    done
}

waitrecoveryperiodend() {
    remretry=30
    until runqfsadmin ping 2>/dev/null |
        grep 'System Info:' |
        tr '\t' '\n' |
        grep 'In recovery= 0' >/dev/null; do
        kill -0 "$metapid" || exit
        remretry=$(expr $remretry - 1)
        if [ $remretry -le 0 ]; then
            echo "Wait for QFS chunk servers to connect timed out" 1>&2
            exit 1
        fi
        sleep 1
    done
}

echo "Waiting for chunk servers to connect to meta server."
waitrecoveryperiodend

echo "Testing dumpster"
runqfsroot() {
    QFS_CLIENT_CONFIG= \
        qfs -D fs.msgLogWriter.logLevel=ERROR \
        -cfg "$clientrootprop" -fs "$myfsurl" -D fs.euser=0 \
        ${1+"$@"}
}
runqfsroot -touchz '/dumpstertest' || exit
runqfsroot -rm -skipTrash /dumpstertest || exit
dumpstertest="$(runqfsroot -ls /dumpster | awk '/dumpstertest/{print $NF}')"
runqfsroot -D dfs.force.remove=true -rm "$dumpstertest" && exit 1
runqfsroot -chmod -w "$dumpstertest" || exit
runqfsroot -mv "$dumpstertest" '/dumpster/test' && exit 1
runqfsroot -mv "$dumpstertest" '/dumpstertest' || exit
runqfsroot -mkdir '/dumpster/test' && exit 1
runqfsroot -touchz '/dumpster/test' && exit 1
runqfsroot -rm -skipTrash '/dumpster' && exit 1
runqfsroot -ls '/dumpster/deletequeue' || exit
runqfsroot -rm -skipTrash '/dumpster/deletequeue' && exit 1
runqfsroot -mv '/dumpstertest' '/dumpster/deletequeue' && exit 1
runqfsroot -mv '/dumpster/deletequeue' '/' && exit 1
runqfsroot -chmod +rw '/dumpster/deletequeue' && exit 1
runqfsroot -mv '/dumpster/deletequeue' '/dumpster/deletequeue1' && exit 1
runqfsroot -rmr -skipTrash '/dumpster' && exit 1
runqfsroot -ls '/dumpster/deletequeue' || exit 1
runqfsroot -D 'fs.createParams=0,0,0,0,1,1,1' \
    -touchz /object_store_file_tier_1 && exit 1
runqfsroot -D 'fs.createParams=0' \
    -touchz /object_store_file_tier_15 || exit 1
runqfsroot -rm -skipTrash /object_store_file_tier_15 || exit 1
runqfsroot -D 'fs.createParams=0,0,0,0,1,2,2' \
    -touchz /object_store_file_tier_2 || exit 1

# Test with OS DNS resolver.
clientpropresolver=${clientprop}.res.cfg
cp "$clientprop" "$clientpropresolver" || exit
cat >>"$clientpropresolver" <<EOF
client.useOsResolver           = 1
client.resolverCacheExpiration = 10
EOF
QFS_CLIENT_CONFIG= \
    qfs -D fs.msgLogWriter.logLevel=ERROR \
    -cfg "$clientpropresolver" -ls / >/dev/null || exit 1

until runqfsroot -rmr -skipTrash '/dumpster' \
    2>"$testdir/dumpster-test-run.err"; do
    sleep 0.1
done >"$testdir/dumpster-test.log" 2>&1 &
dumpstertestpid=$!
dumpstertestpidf="$testdir/dumpster-test${pidsuf}"
echo $dumpstertestpid >"$dumpstertestpidf"

runcptoqfsroot() {
    QFS_CLIENT_CONFIG= \
        cptoqfs -s "$metahost" -p "$metasrvport" -f "$clientrootprop" ${1+"$@"}
}

truncatetest='/truncate.test'
rand-sfmt -g 24577 1234 |
    runcptoqfsroot -t -u 4096 -y 6 -z 3 -r 3 -d - -k "$truncatetest" || exit

# Test move into chunk delete queue enforcement..
metaserversetparameter 'metaServer.maxTruncateChunksQueueCount=1'
runcptoqfsroot -t -d /dev/null -k "$truncatetest" && exit 1

# Restore parameter, and inssue truncate.
metaserversetparameter 'metaServer.maxTruncateChunksQueueCount=1048576'
runcptoqfsroot -t -d /dev/null -k "$truncatetest" || exit

runqfsroot -rm -skipTrash "$truncatetest" || exit
#  runqfsroot -rm -skipTrash '/dumpstertest' || exit

# Shorten dumpster cleanup interval to reclaim space faster.
metaserversetparameter 'metaServer.dumpsterCleanupDelaySec=2'

# Create small chunk to test chunk inventory sync. and fsck later on at the end
# of the test. Do not create too many chunks as each writable chunk takes 64MB.
chunkinventorytestdir='/chunkinventorytest'
echo "Creating test files in $chunkinventorytestdir"
runqfsuser -mkdir "$chunkinventorytestdir" || exit
i=0
while [ $i -lt 10 ]; do
    echo "$i" | runqfsuser -put - "$chunkinventorytestdir/$i.dat" || exit
    i=$(expr $i + 1)
done
[ $spacecheck -ne 0 ] && sleep 2
# Create object store files.
i=0
while [ $i -lt 10 ]; do
    echo "$i" | runqfsuser -D fs.createParams=0,1,0,0,1,15,15 \
        -put - "$chunkinventorytestdir/os.$i.dat" || exit
    i=$(expr $i + 1)
done
[ $spacecheck -ne 0 ] && sleep 3

if [ x"$jerasuretest" = 'x' ]; then
    if qfs -ecinfo | grep -w jerasure >/dev/null; then
        jerasuretest='yes'
    else
        jerasuretest='no'
    fi
fi

ostestname='object store file overwrite'
echo "Testing $ostestname"
myostestlog='os-overwrite-test.log'
ostestrunqfs() {
    qfs -v -D fs.createParams=0 -fs "$myfsurl" ${1+"$@"}
}

(
    set -e
    myostestdir='os-overwrite-test'
    myostestfile='os-overwrite-test/test-file.txt'
    myostestfile1='os-overwrite-test/test-file1.txt'
    set -x
    ostestrunqfs -mkdir "$myostestdir"
    echo test | ostestrunqfs -put - "$myostestfile"
    echo test | ostestrunqfs -put - "$myostestfile" || true
    ostestrunqfs -cp "$myostestfile" "$myostestfile1"
    ostestrunqfs -cp "$myostestfile" "$myostestfile1"
    ostestrunqfs -cp "$myostestfile1" "$myostestfile"
    ostestrunqfs -rmr -skipTrash "$myostestdir"
) >"$myostestlog" 2>&1 ||
    {
        echo "Test $ostestname failed"
        cat "$myostestlog"
        exit 1
    }
echo "Test $ostestname passed"

echo "Starting copy test. Test file sizes: $sizes"
# Run normal test first, then rs test.
# Enable read ahead and set buffer size to an odd value.
# For RS disable read ahead and set odd buffer size.
# Schedule meta server checkpoint after the first two tests.

if [ x"$myvalgrind" = x ]; then
    if [ x"$cptestextraopts" != x ]; then
        $cptestextraopts=" $cptestextraopts"
    fi
    if [ $spacecheck -ne 0 ] || uname | grep CYGWIN >/dev/null; then
        # Sleep before renaming test directories to ensure that all files
        # are closed / flushed by QFS / os
        cptestendsleeptime=3
    else
        cptestendsleeptime=0
    fi
else
    cptestextraopts=" -T 240 $cptestextraopts"
    cptestendsleeptime=5
fi

cp /dev/null cptest.out
cppidf="cptest${pidsuf}"
{
    #    cptokfsopts='-W 2 -b 32767 -w 32767' && \
    QFS_CLIENT_CONFIG=$clientenvcfg \
        cptokfsopts='-r 0 -m 15 -l 15 -R 20 -w -1'"$cptestextraopts" \
        cpfromkfsopts='-r 0 -w 65537'"$cptestextraopts" \
        cptest.sh &&
        sleep $cptestendsleeptime &&
        mv cptest.log cptest-os.log &&
        cptokfsopts='-r 3 -m 1 -l 15 -w -1'"$cptestextraopts" \
            cpfromkfsopts='-r 1e6 -w 65537'"$cptestextraopts" \
            cptest.sh &&
        sleep $cptestendsleeptime &&
        mv cptest.log cptest-0.log &&
        kill -USR1 $metapid &&
        cptokfsopts='-S -m 2 -l 2 -w -1'"$cptestextraopts" \
            cpfromkfsopts='-r 0 -w 65537'"$cptestextraopts" \
            cptest.sh &&
        {
            [ x"$jerasuretest" = x'no' ] || {
                sleep $cptestendsleeptime &&
                    mv cptest.log cptest-rs.log &&
                    cptokfsopts='-u 65536 -y 10 -z 4 -r 1 -F 3 -m 2 -l 2 -w -1'"$cptestextraopts" \
                        cpfromkfsopts='-r 0 -w 65537'"$cptestextraopts" \
                        cptest.sh
            }
        }
} >>cptest.out 2>&1 &
cppid=$!
echo "$cppid" >"$cppidf"

qfscpidf="qfsctest${pidsuf}"
cp /dev/null test-qfsc.out
QFS_CLIENT_LOG_LEVEL=DEBUG \
    test-qfsc "$metahost:$metasrvport" 1>>test-qfsc.out 2>test-qfsc.log &
qfscpid=$!
echo "$qfscpid" >"$qfscpidf"

if [ x"$accessdir" != x ]; then
    kfsaccesspidf="kfsaccess_test${pidsuf}"
    clientproppool="$clientprop.pool.prp"
    if [ -f "$clientprop" ]; then
        cp "$clientprop" "$clientproppool" || exit
    else
        cp /dev/null "$clientproppool" || exit
    fi
    cat >>"$clientproppool" <<EOF
client.connectionPool = 1
client.rackId = 0
EOF
    if [ -f "$monitorpluginlib" ]; then
        cat >>"$clientproppool" <<EOF
client.monitorPluginPath=$monitorpluginlib
EOF
    fi
    javatestclicfg="FILE:${clientproppool}"
    cp /dev/null kfsaccess_test.out
    QFS_CLIENT_MONITOR_LOG_DIR="$testdir/monitor_plugin" \
        QFS_CLIENT_CONFIG="$javatestclicfg" \
        java \
        -Xms800M \
        -Djava.library.path="$accessdir" \
        -classpath "$kfsjar" \
        -Dkfs.euid="$(id -u)" \
        -Dkfs.egid="$(id -g)" \
        com.quantcast.qfs.access.KfsTest "$metahost" "$metasrvport" \
        >>kfsaccess_test.out 2>&1 &
    kfsaccesspid=$!
    echo "$kfsaccesspid" >"$kfsaccesspidf"
fi

if [ x"$kfsgosrcdir" != x ] && go version >/dev/null 2>&1; then
    kfsgopidf="kfsgo${pidsuf}"
    cp /dev/null kfsgo_test.out
    {
        if [ x"$installprefix" = x ]; then
            qfscincludedir=$(pwd)/include &&
                qfscincludesubdir=$qfscincludedir/kfs/c &&
                mkdir -p "$qfscincludesubdir" &&
                cp "$kfsgosrcdir/../cc/qfsc/qfs.h" "$qfscincludesubdir"
        else
            qfscincludedir=$installprefix/include
        fi &&
            cd "$kfsgosrcdir" &&
            go get -t -v &&
            CGO_CFLAGS="-I$qfscincludedir" \
                CGO_LDFLAGS="-L$qfscdir" \
                QFS_CLIENT_CONFIG=$clientenvcfg \
                go test -qfs.addr "$metahost:$metasrvport"
    } >>kfsgo_test.out 2>&1 &
    kfsgopid=$!
    echo "$kfsgopid" >"$kfsgopidf"
else
    kfsgopid=''
fi

if [ x"$kfspythonwheel" = x ]; then
    kfspythonpid=''
else
    kfspythonpidf="kfspython${pidsuf}"
    cp /dev/null kfspython_test.out
    {
        cd "$testdir" &&
            python3 -m venv .venv &&
            . .venv/bin/activate &&
            python -m pip install "$kfspythonwheel" &&
            kfspythonconf=kfspython.cfg &&
            cat >"$kfspythonconf" <<EOF || exit 1
metaServer.name = $metahost
metaServer.port = $metasrvport
EOF
        python "$kfspythontest" "$kfspythonconf"
    } >>kfspython_test.out 2>&1 &
    kfspythonpid=$!
    echo "$kfspythonpid" >"$kfspythonpidf"
fi

if [ $spacecheck -ne 0 ]; then
    waitqfscandcptests
    pausesec=$(expr $csheartbeatinterval \* 2)
    echo "Pausing for two chunk server chunk server heartbeat intervals:" \
        "$pausesec sec. to give a chance for space update to occur."
    sleep $pausesec
    n=0
    until df -P -k "$testdir" | awk '
    BEGIN {
        msp='"${mindiskspace}"' * .8
        n='"$n"'
    }
    /^\// {
        asp = $4 * 1024
        if (asp < msp) {
            if (0 < n) {
                printf("Wating for chunk files cleanup to occur.\n")
                printf("Disk space: %5.2e is less thatn %5.2e\n", asp, msp)
            }
            exit 1
        } else {
            exit 0
        }
    }'; do
        sleep 1
        n=$(expr $n + 1)
        [ $n -le 30 ] || break
    done
fi

cp /dev/null qfs_tool-test.out
qfstoolpidf="qfstooltest${pidsuf}"
qfstoolopts='-v' \
    qfstoolmeta="$metahosturl:$metasrvport" \
    qfstooltrace=on \
    qfstoolrootauthcfg=$qfstoolrootauthcfg \
    qfs_tool-test.sh '##??##::??**??~@!#$%^&()=<>`|||' \
    1>>qfs_tool-test.out 2>qfs_tool-test.log &
qfstoolpid=$!
echo "$qfstoolpid" >"$qfstoolpidf"

if [ $fotest -ne 0 ]; then
    if [ x"$myvalgrind" = x ]; then
        foextraopts=''
    else
        foextraopts=' -c 240'
    fi
    echo "Starting fanout test. Fanout test data size: $fanouttestsize"
    fopidf="kfanout_test${pidsuf}"
    # Do two runs one with connection pool off and on.
    cp /dev/null kfanout_test.out
    for p in 0 1; do
        kfanout_test.sh \
            -coalesce 1 \
            -host "$metahost" \
            -port "$metasrvport" \
            -size "$fanouttestsize" \
            -partitions "$fanoutpartitions" \
            -read-retries 1 \
            -kfanout-extra-opts "-U $p -P 3""$foextraopts" \
            -cpfromkfs-extra-opts "$cptestextraopts" \
            ${installbindir:+-bin-prefix xxx-no-add-fanout-build-dir-to-path} ||
            exit
    done >>kfanout_test.out 2>&1 &
    fopid=$!
    echo "$fopid" >"$fopidf"
fi

if [ x"$smtest" != x ]; then
    if [ x"$smauthconf" != x ]; then
        cat >"$smauthconf" <<EOF
sortmaster.auth.X509.X509PemFile = $certsdir/$clientuser.crt
sortmaster.auth.X509.PKeyPemFile = $certsdir/$clientuser.key
sortmaster.auth.X509.CAFile      = $certsdir/qfs_ca/cacert.pem
EOF
    fi
    smpidf="sortmaster_test${pidsuf}"
    echo "Starting sort master test"
    cp /dev/null sortmaster_test.out
    QFS_CLIENT_CONFIG=$clientenvcfg "$smtest" >>sortmaster_test.out 2>&1 &
    smpid=$!
    echo "$smpid" >"$smpidf" || exit
fi

if [ $spacecheck -eq 0 ]; then
    waitqfscandcptests
fi

if [ x"$qfstoolpid" = x ]; then
    qfstoolstatus=0
else
    mytailwait $qfstoolpid qfs_tool-test.out
    qfstoolstatus=$?
    rm "$qfstoolpidf"
fi

if [ $fotest -ne 0 ]; then
    mytailwait $fopid kfanout_test.out
    fostatus=$?
    rm "$fopidf"
else
    fostatus=0
fi

if [ x"$smtest" = x ]; then
    smstatus=0
else
    mytailwait $smpid sortmaster_test.out
    smstatus=$?
    rm "$smpidf"
fi

fusestatus=0
if [ $testfuse -ne 0 ]; then
    cd "$testdir" || exit
    echo "Testing fuse"
    fusetest.sh "${metahost}:${metasrvport}"
    fusestatus=$?
fi

cd "$metasrvdir" || exit
echo "Running online fsck"
qfsfsck -s "$metahost" -p "$metasrvport" -f "$clientrootprop"
fsckstatus=$?

status=0

cd "$testdir" || exit

# Clean up write leases, if any, (sorters' speculative sorts might leave
# stale leases), and force chunks deletion by truncating files in dumpster.
# This is needed to minimize the "retire" test time, as write leases, and files
# in the dumpster with replication larger than the number of chunk servers
# would prevent chunk server "retirement", until expiration / cleanup.

echo "Cleaning up write leases by removing and truncating files"

rootrmlist=$(runqfsroot -ls '/' |
    awk '/^[d-]/{ if ($NF != "/dumpster" &&
    $NF != "'"$chunkinventorytestdir"'") print $NF; }')
if [ x"$rootrmlist" != x ]; then
    runqfsroot -rmr -skipTrash $rootrmlist || exit
fi

movefromdumpster='/movefromdumpster.tmp'
runqfsroot -mkdir "$movefromdumpster" || exit
runqfsroot -ls '/dumpster' |
    awk '/^-/{ if (0 < $2 && $NF != "/dumpster/deletequeue") print $NF; }' |
    while read fn; do
        if runqfsroot -mv "$fn" "$movefromdumpster/" 2>/dev/null; then
            n=$(basename "$fn")
            runcptoqfsroot -t -d /dev/null -k "$movefromdumpster/$n" || exit
        else
            # Check if the file has already been removed.
            if runqfsroot -test -e "$fn"; then
                # Try again, emitting error / diagnostic message.
                if runqfsroot -mv "$fn" "$movefromdumpster/"; then
                    continue
                fi
                # The meta server is likely started deleting the file, by truncating
                # n chunks at a time.
                # List and stat the file for diagnostics.
                runqfsroot -ls "$fn"
                runqfsroot -astat "$fn"
            fi
        fi
    done || exit

if kill -0 $dumpstertestpid; then
    kill $dumpstertestpid
    rm "$dumpstertestpidf"
else
    echo "Dumpster test failure"
    exit 1
fi

echo "Testing admin commands"

adminstatus=0
myfsid=$(awk -F / '/^filesysteminfo\//{print $3; exit 0;}' \
    "$metasrvdir/kfscp/latest")
mymetareadargs="-F FsId=$myfsid -F Read-pos=0 -F Read-size=2047"
for cmd in \
    get_chunk_server_dirs_counters \
    get_chunk_servers_counters \
    get_request_counters \
    ping \
    dump_chunkreplicationcandidates \
    dump_chunktoservermap \
    open_files \
    stats \
    upservers \
    check_leases \
    recompute_dirsize \
    "vr_get_status || true" \
    "-F op-type=help vr_reconfiguration" \
    "-F Toggle-WORM=1 toggle_worm" \
    "-F Toggle-WORM=0 toggle_worm" \
    "-F Checkpoint=1      $mymetareadargs read_meta_data && echo ''" \
    "-F Start-log='0 0 0' $mymetareadargs read_meta_data && echo ''"; do
    echo "===================== start $cmd ==================================="
    if ! eval runqfsadmin $cmd; then
        adminstatus=$(expr $adminstatus + 1)
    fi
    echo "===================== end $cmd ====================================="
done >qfsadmintest.out 2>qfsadmintest.err

echo "Testing chunk server hibernate and retire"

# Turn off spurious chunk server disconnect debug.
metaserversetparameter 'metaServer.panicOnRemoveFromPlacement=0'

upserverslist='upservers.tmp'
runqfsadmin upservers >"$upserverslist" || exit

# Tell chunk servers to re-connect to the meta server, in order to exercise
# chunk inventory sync. logic.
while read server port; do
    pidf=$chunksrvdir/$port/$chunksrvpid
    xargs kill -HUP <"$pidf" || exit
done <"$upserverslist"
# Give chunk servers couple seconds to initiate re-connect.
sleep 2
waitrecoveryperiodend

hibernatesleep=0
hibernateshutdowntimeout=600
while read server port; do
    qfshibernate -m "$metahost" -p "$metasrvport" -s "$hibernatesleep" \
        -f "$clientrootprop" -c "$server" -d "$port" || exit
    pidf=$chunksrvdir/$port/$chunksrvpid
    pid=$(cat "$pidf")
    i=0
    while kill -0 "$pid" 2>/dev/null; do
        i=$(expr $i + 1)
        if [ $i -gt $hibernateshutdowntimeout ]; then
            echo "hibernate failed; chunk server still up after" \
                " $hibernateshutdowntimeout sec; $pidf" 1>&2
            exit 1
        fi
        sleep 1
    done
    wait "$pid"
    estatus=$?
    rm "$pidf" || exit
    if [ $estatus -ne 0 ]; then
        echo "Exit status: $estatus $pidf" 1>&2
        exit 1
    fi
    [ $hibernatesleep -gt 0 ] && break
    hibernatesleep=10000
done <"$upserverslist"

# Turn off chunk inventory mismatch debug, as retire does not delete evacuated
# chunks.
metaserversetparameter 'metaServer.debugPanicOnHelloResumeFailureCount=-1'

i=$chunksrvport
e=$(expr $i + $numchunksrv)
while [ $i -lt $e ]; do
    cd "$chunksrvdir/$i" || exit
    if [ ! -e "$chunksrvpid" ]; then
        echo "Restarting chunk server $i"
        if [ -e "$myvalgrindlog" ]; then
            mv "$myvalgrindlog" "$myvalgrindlog"'.run.log' || exit
        fi
        myrunprog "$chunkbindir"/chunkserver \
            "$chunksrvprop" "$chunksrvlog" >>"${chunksrvout}" 2>&1 &
        echo $! >"$chunksrvpid"
    fi
    i=$(expr $i + 1)
done
cd "$testdir" || exit
waitrecoveryperiodend

echo "Testing chunk server directory evacuation"
i=$(expr $e - 1)
myevacuatefile="$chunksrvdir/$i/kfschunk/evacuate"
touch "$myevacuatefile" || exit

echo "Waiting chunk server directory evacuation to complete"
myevacuatefile="$myevacuatefile.done"
myevacuatestatus=0
i=0
until [ -f "$myevacuatefile" ]; do
    if [ 60 -le $i ]; then
        echo "Wait for evacuation completion timed out ($myevacuatefile)"
        myevacuatestatus=1
        break
    fi
    i=$(expr $i + 1)
    sleep 1
done
echo "Chunk server directory evacuation is now complete"

# Allow meta server to run re-balancer
sleep 5
echo "Shutting down"
kill -QUIT "$metapid" || exit

pids=$(getpids | grep -v "$metapid")
# For now pause to let chunk server IOs complete
sleep 2
for pid in $pids; do
    kill -QUIT "$pid" || exit
done

# Wait 30 sec for shutdown to complete
nsecwait=30
i=0
while true; do
    rpids=
    for pid in $pids; do
        if kill -0 "$pid" 2>/dev/null; then
            rpids="$rpids $pid"
        elif [ x"$kfstestnoshutdownwait" = x ]; then
            wait "$pid"
            estatus=$?
            if [ $estatus -ne 0 ]; then
                echo "Exit status: $estatus pid: $pid"
                status=$estatus
            fi
        fi
    done
    pids=$rpids
    [ x"$pids" = x ] && break
    i=$(expr $i + 1)
    if [ $i -le $nsecwait ]; then
        sleep 1
    else
        echo "Wait timed out, sending abort singnal to: $rpids"
        kill -ABRT $rpids
        status=1
        break
    fi
done
report_test_status "Shutdown" $status

if [ $status -ne 0 ]; then
    showpids
fi

if [ x"$mytailpids" != x ]; then
    # Let tail -f poll complete, then shut them down.
    {
        sleep 1
        kill -TERM $mytailpids
    } &
    wait 2>/dev/null
fi

if [ $status -eq 0 ]; then
    cd "$metasrvdir" || exit
    echo "Running meta server log compactor"
    logcompactor -T newlog -C newcp
    status=$?
    report_test_status "Log compactor" $status
fi
if [ $status -eq 0 ]; then
    cd "$metasrvdir" || exit
    echo "Running meta server fsck"
    qfsfsck -c kfscp -F
    status=$?
    # Status 1 might be returned in the case if chunk servers disconnects
    # were commited, do run whout the last log segment, the disconnects
    # should be in the last one.
    if [ $status -eq 1 ]; then
        newcpfsckopt=''
    else
        newcpfsckopt='-F'
    fi
    if [ $status -le 1 ]; then
        qfsfsck -c kfscp -A 0 -F
        status=$?
    fi
    report_test_status "fsck" $status
fi
if [ $status -eq 0 ]; then
    qfsfsck -c newcp $newcpfsckopt
    status=$?
    report_test_status "fsck new checkpoint" $status
fi
if [ $status -eq 0 ] && [ -d "$objectstoredir" ]; then
    echo "Running meta server object store fsck"
    ls -1 "$objectstoredir" | qfsobjstorefsck
    status=$?
    report_test_status "Meta server object store fsck" $status
fi
if [ $status -eq 0 ]; then
    echo "Running re-balance planner"
    rebalanceplanner -d -L ERROR
    status=$?
    report_test_status "Re-balance planner" $status
fi

# Check for core files, and fail the test if any.
find "$testdir" -path "$testdir/.venv" -prune -o -type f -name core\* -print |
    awk '{ print; } END{ exit (NR > 0 ? 1 : 0); }' || status=1

if [ $status -eq 0 \
    -a $cpstatus -eq 0 \
    -a $qfstoolstatus -eq 0 \
    -a $fostatus -eq 0 \
    -a $smstatus -eq 0 \
    -a $kfsaccessstatus -eq 0 \
    -a $kfsgostatus -eq 0 \
    -a $kfspythonstatus -eq 0 \
    -a $qfscstatus -eq 0 \
    -a $fsckstatus -eq 0 \
    -a $fusestatus -eq 0 \
    -a $adminstatus -eq 0 \
    -a $myevacuatestatus -eq 0 \
    ]; then
    echo "Passed all tests"
else
    report_test_status "Copy" $cpstatus
    report_test_status "Qfs tool" $qfstoolstatus
    report_test_status "Fanout" $fostatus
    report_test_status "Sort master" $smstatus
    report_test_status "Java shim" $kfsaccessstatus
    report_test_status "Go shim" $kfsgostatus
    report_test_status "Python shim" $kfspythonstatus
    report_test_status "C bindings" $qfscstatus
    report_test_status "Fsck" $fsckstatus
    report_test_status "Fuse" $fusestatus
    report_test_status "Admin" $adminstatus
    report_test_status "Evacuate" $myevacuatestatus
    echo "Test failure"
    status=1
fi

if [ x"$dontusefuser" = x'yes' ]; then
    trap '' EXIT
fi
exit $status
