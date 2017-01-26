#!/bin/sh
#
# $Id$
#
# Created 2009
# Author: Mike Ovsiannikov
#
# Copyright 2009-2012,2016 Quantcast Corporation. All rights reserved.
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

meta=${meta-'-s 127.0.0.1 -p 20000'}
removetestdir=${removetestdir-'yes'}
cptokfsopts=${cptokfsopts-}
dir="/kfstest/`hostname`/`basename "$0" .sh`${1-}"
log="`basename "$0" .sh`${1-}.log"
kfstools=${kfstools-'src/cc/tools'}
cptokfs="cptoqfs $meta $cptokfsopts"
cpfromkfsopts=${cpfromkfsopts-}
cpfromkfs="cpfromqfs $meta $cpfromkfsopts"
qfsshellloglevel=${qfsshellloglevel-'NOTICE'}
rand='rand-sfmt'
randseed=1234
sizes=${sizes-'1 2 3 127 511 1024 65535 65536 65537 70300 1e5 67108864 67108865 100e6 250e6'}
chksum=${chksum-sha1sum}

if [  x"`{ cat /dev/null | $chksum ; } 2>/dev/null`" = x ]; then
    chksum='openssl sha1'
fi
if [  x"`cat /dev/null | $chksum`" = x ]; then
    exit 1
fi

if [ -d "${kfstools}" ]; then
    kfstools=`cd "${kfstools}" && pwd`
fi
PATH="`pwd`:${PATH}"
[ -d "${kfstools}" ] && PATH="${kfstools}:${PATH}"
export PATH

xdatesec='date -u +%s'

datetimesec()
{
    if [ x"$xdatesec" = x ]; then
        echo 0
    else
        $xdatesec
    fi
}

mytime()
{
    if [ x"$xtime" = x ]; then
        mystart=`datetimesec`
        ${1+"$@"}
        ret=$?
        {
            mytime=`datetimesec`
            mytime=`expr $mytime - $mystart`
            echo "Command: ${1+"$@"}"
            echo "Elapsed time: $mytime"
            echo "Exit status: $ret"
        } 1>&2
        return $ret
    else
        $xtime ${1+"$@"}
    fi
}

myqfsshell()
{
    qfsshell $meta -l $qfsshellloglevel -q -- ${1+"$@"}
}

if { time -v true ; } >/dev/null 2>&1; then
    xtime='time -v'
elif { time true ; } >/dev/null 2>&1; then
    xtime='time'
fi

if expr `$xdatesec 2>/dev/null` - 0 > /dev/null 2>&1; then
    true
else
    xdatesec=''
fi

ulimit -c unlimited

if [ ! -d "$log" ]; then
    mkdir -p "$log" || exit
fi

cd "$log" || exit

# Ensure that the test directory is empty creating it and them removing,
# and then creating again.
echo "Starting test: $cptokfs; directory: $dir"
myqfsshell mkdir "$dir" || exit
myqfsshell rm    "$dir" || exit
myqfsshell mkdir "$dir" || exit
# Directory must be empty.
myqfsshell rmdir "$dir" || exit
myqfsshell mkdir "$dir" || exit

rseed=$randseed
for s in $sizes; do
    szchk="$s.$rseed.chk"
    if [ ! -f "$szchk" ]; then
        $rand -g $s $rseed | $chksum > "$szchk" &
    fi
    rseed=`expr $rseed + 1`
done

rseed=$randseed
for s in $sizes; do
    if [ -f "$s.chk" ]; then
        rm "$s.chk" || exit
    fi
    $rand -g $s $rseed | \
        mytime $cptokfs -k "$dir/$s.test" -d - -v > "$s.wr.log" 2>&1 &
    rseed=`expr $rseed + 1`
done
wait

for s in $sizes; do
    mytime $cpfromkfs -k "$dir/$s.test" -d - -v 2>"$s.rd.log" \
        | $chksum > "$s.chk" &
done
wait

status=0
rseed=$randseed
for s in $sizes; do
    if cmp "$s.$rseed.chk" "$s.chk"; then
        echo "$s test passed"
    else
        echo "$s test failed"
        status=1
    fi
    rseed=`expr $rseed + 1`
done
if [ $status -eq 0 ]; then
    if [ x"$removetestdir" = x'yes' ]; then
        myqfsshell rm "$dir"
    fi
    echo "Passed all tests."
fi

exit $status
