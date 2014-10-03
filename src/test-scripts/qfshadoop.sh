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
myqfssrc="`cd "$mydir/../.." > /dev/null && pwd`"

myhadoophomebin='bin/hadoop'
if [ x"$HADOOP_HOME" = x ]; then
    true
else
    myhadoophomebin="$HADOOP_HOME/$myhadoophomebin"
fi

metaserverhost=${metaserverhost-127.0.0.1}
metaserverport=${metaserverport-20000}
clientconfig=${clientconfig-"$HOME/qfsbase/client/client.prp"}
qfssrcprefix=${qfssrcprefix-$myqfssrc}
hadoopbin=${hadoopbin-bin/hadoop}

showhelp()
{
    if [ x"$HADOOP_HOME" = x ]; then
        HADOOP_HOME='$HADOOP_HOME'
    fi
    cat << EOF

Usage: "$0" {fs|jar|-env|-h}

This script is intended to be executed in Hadoop install directory
    $HADOOP_HOME, in place of $HADOOP_HOME/bin/hadoop with "jar" and "fs"
    arguments.

The assumption is that the sample servers are already successfully installed,
    and running.

    QFS servers can be started by the following:
     ${myqfssrc}/examples/sampleservers/sample_setup.py -a install
    With sample_setup.py an optional --auth parameter can be used in order to
    configure and use QFS authentication.

The following can be used to build required QFS executables and jar files:
    cd ${myqfssrc} && make hadoop-jars

The following environment variable can be set prior to this script
    invocation in order to enable QFS client debug level:
    QFS_CLIENT_LOG_LEVEL=DEBUG

-env command line option can be used to emit environment variables that have to
    be added to $HADOOP_HOME/conf/hadoop-env.sh
    [or $HADOOP_HOME/etc/hadoop/hadoop-env.sh with hadoop 2.x] file in order to
    make Hadoop work with QFS in pseudo distributed mode.
    This option should be used only *after* [re]configuring and starting QFS
    file system with sample_setup.py, and *before* starting haddop job and task
    trackers with $HADOOP_HOME/bin/start-mapred.sh
    [or $HADOOP_HOME/sbin/start-yarn.sh with hadoop 2.x]

    For pseudo distributed mode to work with Hadoop 1.x, the following
    properties need to be set in $HADOOP_HOME/conf/core-site.xml

    <property>
        <name>fs.default.name</name>
        <value>qfs://${metaserverhost}:${metaserverport}</value>
    </property>
    <property>
        <name>fs.qfs.impl</name>
        <value>com.quantcast.qfs.hadoop.QuantcastFileSystem</value>
    </property>

    For hadoop 2.x the the above properties should be set in the
    $HADOOP_HOME/etc/hadoop/core-site.xml and property name "fs.defaultFS"
    should be used in place of "fs.default.name".

  *** Yarn support is not complete yet*: AM / container launch succeeds,
    but all the map tasks appears to fail for no obvious reason.

    The following notes are primarily intended for myself, if/when I will have
    time to experiment with yarn again.
    Passing environment variables, including delegation token, and QFS client
    trace log level using property "mapred.child.env" does not seem to work
    with yarn.
    Setting \$HADOOP_CLASSPATH in is not sufficient to make yarn work.
    One way around this is to sym link qfs and qfs hadoop jars to
    $HADOOP_HOME/share/hadoop/common ($HADOOP_HOME/lib is not in yarn
    default).
    To experiment with yarn the following has to be added to
    $HADOOP_HOME/etc/hadoop/core-site.xml

    <property>
        <name>fs.AbstractFileSystem.qfs.impl</name>
        <value>com.quantcast.qfs.hadoop.Qfs</value>
    </property>

-h argument emits this message.
EOF
}

if [ x"$1" = x"-h" -o x"$1" = x"-help" -o x"$1" = x"--help" ]; then
    showhelp
    exit 0
fi

mybuildprefix="$qfssrcprefix/build"
myjbuildprefix="$mybuildprefix/java"
mycbuildprefix="$mybuildprefix/release"
mylibbuildprefix="$mycbuildprefix/lib"

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
            showhelp
            exit 1
        fi
    fi
fi
myhadoopdir="`dirname "$myhadoop"`"
myhadoopdir="`cd "$myhadoopdir/.." > /dev/null && pwd`"
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

if grep -v '#' "$myhadoop" | grep "JAVA_LIBRARY_PATH=''" > /dev/null; then
    if [ -f "$myhadoopdir/lib/native/`uname`"*/*qfs_access* ]; then
        true
    else
        cat << EOF
The following line in $myhadoop script has to be commented out, or qfs native
libraries, have to be sym linked or copied to the platform native libraries
directory.
    JAVA_LIBRARY_PATH=''
EOF
    if tty -s ; then
        echo "Would you like to comment out the line?(Y/n)"
        read ln
        if [ x"$ln" = x'Y' -o x"$ln" = x'y' -o x"$ln" = x \
                -o x"$ln" = x'yes' ]; then
            mytemp="$myhadoop.$$.tmp"
            sed -e 's/\(JAVA_LIBRARY_PATH='\'''\''*.$\)/# \1/' \
                "$myhadoop" > "$mytemp"         || exit
            chmod +x "$mytemp"                  || exit
            mv "$myhadoop" "$myhadoop.qfs.orig" || exit
            mv "$mytemp"   "$myhadoop"          || exit
        else
            exit 1
            fi
        else
            exit 1
        fi
    fi
fi

if [ x"$1" = x'-env' ]; then
    cat << EOF
HADOOP_CLASSPATH="${myqfsaccessjar}:${myqfshadoopjar}:\${HADOOP_CLASSPATH}"
export HADOOP_CLASSPATH
EOF
    myosname="`uname`"
    myqfsaccess="`echo "$myhadoopdir/lib/native/${myosname}"*/*qfs_access*`"
    if [ -f "$myqfsaccess" ]; then
        myqfsaccessdir="`dirname "$myqfsaccess"`"
        cat << EOF
LD_LIBRARY_PATH="${myqfsaccessdir}:\${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH
EOF
    else
        cat << EOF
JAVA_LIBRARY_PATH="${mylibbuildprefix}:\${JAVA_LIBRARY_PATH}"
export JAVA_LIBRARY_PATH
LD_LIBRARY_PATH="${mylibbuildprefix}:\${LD_LIBRARY_PATH}"
export LD_LIBRARY_PATH
EOF
    fi
    if [ -f "$clientconfig" ]; then
        cat << EOF
QFS_CLIENT_CONFIG='FILE:${clientconfig}'
export QFS_CLIENT_CONFIG
EOF
    fi
    exit 0
fi

HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${myqfsaccessjar}"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:${myqfshadoopjar}"
export HADOOP_CLASSPATH

if [ x"$LD_LIBRARY_PATH" = x ]; then
    JAVA_LIBRARY_PATH="${mylibbuildprefix}"
else
    JAVA_LIBRARY_PATH="${mylibbuildprefix}:${JAVA_LIBRARY_PATH}"
fi
export JAVA_LIBRARY_PATH

if [ x"$LD_LIBRARY_PATH" = x ]; then
    LD_LIBRARY_PATH="${mylibbuildprefix}"
else
    LD_LIBRARY_PATH="${mylibbuildprefix}:${LD_LIBRARY_PATH}"
fi
export LD_LIBRARY_PATH

if [ $# -le 0 ]; then
    showhelp
    exit 1
    # echo ''
    # exec "$myhadoop"
fi

mycmd=$1
shift
if [ x"$mycmd" = x"jar" ]; then
    myjarname=$1
    shift
    if [ $# -gt 0 ]; then
        myprogname=$1
        shift
    fi
else
    unset myjarname
    unset myprogname
fi

unset mychildenv
if [ -f "$clientconfig" ]; then
    myqfstool="${mycbuildprefix}/bin/tools/qfs"
    if [ x"$mycmd" = x"jar" -a \
            \( x"$forcedelegation" = x'yes' -o  -x "${myqfstool}" \) ]; then
        if [ -x "${myqfstool}" ]; then
            true
        else
            echo "Failed to find executable: ${myqfstool}" 1>&2
            exit 1
        fi
        eval `QFS_CLIENT_CONFIG='' "${myqfstool}" \
            -cfg "$clientconfig" \
            -fs  qfs://"${metaserverhost}:${metaserverport}" \
            -delegate 0 3600000 \
        | awk '
            BEGIN {
                tok="";
                key="";
            }
            /^Token:/{ tok = $2; }
            /^Key:/  { key = $2; }
            END      {
                if (tok == "" || key == "") {
                    exit 1;
                }
                printf("myqfstoken='\''%s'\'';", tok);
                printf("myqfskey='\''%s'\'';",   key);
            }
        '`
        if [ x"$myqfstoken" = x -o x"$myqfskey" = x ]; then
            echo "Failed to create QFS delegation token" 1>&2
            exit 1
        fi
        # Use delimiters different than "=" and "," as those are already used by
        # hadoop environment variables passing logic.
        mydelegation="DELIM::|client.auth.psk.keyId:${myqfstoken}"
        mydelegation="${mydelegation}|client.auth.psk.key:${myqfskey}"
        myenvqfscliconfig=${mydelegation}
        # WARNING: passing QFS delegation key as a parameter is not "secure",
        # as exposes the key via "ps" and such.
        # It is here for illustration purposes only.
        mychildenv="QFS_CLIENT_CONFIG=${myenvqfscliconfig}"
    else
        myenvqfscliconfig="FILE:${clientconfig}"
    fi
fi

if echo "$myhadoopvers" | awk -F . '{exit(2 <= $1 ? 0 : 1);}'; then
    myfsdefault='fs.defaultFS'
    myabstractfs='-Dfs.AbstractFileSystem.qfs.impl=com.quantcast.qfs.hadoop.Qfs'
else
    myfsdefault='fs.default.name'
    unset myabstractfs
fi

if [ x"$QFS_CLIENT_LOG_LEVEL" = x ]; then
    true
else
    if [ x"$mychildenv" = x ]; then
        mychildenv="${mychildenv},"
    fi
    mychildenv="${mychildenv}QFS_CLIENT_LOG_LEVEL=${QFS_CLIENT_LOG_LEVEL}"
fi

# QFS_CLIENT_LOG_LEVEL=DEBUG \
# HADOOP_ROOT_LOGGER="ALL,console" \
QFS_CLIENT_CONFIG="$myenvqfscliconfig" \
"$myhadoop" \
    "$mycmd" \
    ${myjarname+"$myjarname"} \
    ${myprogname+"$myprogname"} \
    -Dfs.qfs.impl=com.quantcast.qfs.hadoop.QuantcastFileSystem  \
    ${myabstractfs+"$myabstractfs"}  \
    -D"${myfsdefault}=qfs://${metaserverhost}:${metaserverport}" \
    ${mychildenv+"-Dmapred.child.env=${mychildenv}"} \
    ${1+"$@"}

status=$?

if [ x"$myqfstoken" = x -o x"$myqfskey" = x ]; then
    exit $status
fi

{ echo "$myqfstoken" ; echo "$myqfskey" ; } | "${myqfstool}" \
    -cfg "$clientconfig" \
    -fs  qfs://"${metaserverhost}:${metaserverport}" \
    -cancel

exit $status
