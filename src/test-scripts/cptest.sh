#!/bin/sh
#
# $Id$
#
# Created 2009
# Author: Mike Ovsiannikov
#
# Copyright 2009-2012 Quantcast Corp.
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
cptokfsopts=${cptokfsopts-}
dir="/kfstest/`hostname`/`basename "$0" .sh`${1-}"
log="`basename "$0" .sh`${1-}.log"
kfstools=${kfstools-'src/cc/tools'}
cptokfs="cptoqfs $meta $cptokfsopts"
cpfromkfsopts=${cpfromkfsopts-}
cpfromkfs="cpfromqfs $meta $cpfromkfsopts"
kfsshell="qfsshell $meta -q --"
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

mytime()
{
    if [ x"$xtime" = x ]; then
        time ${1+"$@"}
    else
        $xtime ${1+"$@"}
    fi
}

if [ -x /usr/bin/time ] && /usr/bin/time -v true >/dev/null 2>&1; then
    xtime='/usr/bin/time -v'
fi

ulimit -c unlimited

if [ ! -d "$log" ]; then
    mkdir -p "$log" || exit
fi

cd "$log" || exit

$kfsshell rm "$dir" > /dev/null 2>&1
$kfsshell mkdir "$dir" || exit
# sleep 3

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
        mytime $cptokfs -k "$dir/$s.test" -d - -v > "$s.log" 2>&1 &
    rseed=`expr $rseed + 1`
done
wait

for s in $sizes; do
    mytime $cpfromkfs -k "$dir/$s.test" -d - -v 2>>"$s.log" \
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
    echo "Passed all tests."
fi

exit $status
