#!/bin/sh
#
# $Id$
#
# Created 2008
# Author: Mike Ovsiannikov
#
# Copyright 2008 Quantcast Corp.
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

extraopts=-v
nitr=1000
ncp=1
meta='-s mrs239 -p 45200'
dir="/kfstest/`hostname`/`basename "$0" .sh`"
log="`basename "$0" .sh`.log"
kfstools='.'
cptokfs="$kfstools/cptoqfs $extraopts $meta"
cpfromkfs="$kfstools/cpfromqfs $extraopts $meta"
kfsshell="$kfstools/qfsshell $extraopts $meta -q"
rand='./rand-sfmt'
randseed=1234
sizes='3.0000e9 3.0001e9 3.0002e9 3.0003e9'
chksum=sha1sum
time=time

[ -x /usr/bin/time ] && /usr/bin/time -v true >/dev/null 2>&1 \
    && time='/usr/bin/time -v'

ulimit -c unlimited

if [ ! -d "$log" ]; then
    mkdir -p "$log" || exit
fi

$kfsshell rmdir "$dir" > /dev/null 2>&1
$kfsshell mkdir "$dir" || exit

for s in $sizes; do
    szchk="$log/$s.$randseed.chk"
    if [ ! -f "$szchk" ]; then
        $rand -g $s $randseed | $chksum > "$szchk" &
    fi
done

for s in $sizes; do
    if [ -f "$log/$s.chk" ]; then
        rm "$log/$s.chk" || exit
    fi
    $rand -g $s $randseed | \
        $time $cptokfs -k "$dir/$s.test" -d - > "$log/$s.log" 2>&1 &
done
wait

for s in $sizes; do
    $time $cpfromkfs -k "$dir/$s.test" -d - 2>>"$log/$s.log" \
        | $chksum > "$log/$s.chk" &
done
wait

status=0
for s in $sizes; do
    if cmp "$log/$s.$randseed.chk" "$log/$s.chk"; then
        echo "$s test passed"
    else
        echo "$s test failed"
        status=1
    fi
done

if [ $status -eq 0 ]; then
    echo "Test files created."
else
    exit $status
fi

itr=0
while [ $itr -le $nitr ]; do
    [ -f stop ] && break;
    i=0
    while [ $i -le $ncp ]; do
        [ -f stop ] && break;
        for s in $sizes; do
            { $time $cpfromkfs -k "$dir/$s.test" -d - 2>>"$log/$s.read.err" \
                | $time $cptokfs -k "$dir/$s.test.cp" -d - 2>>"$log/$s.write.err" \
                && $kfsshell mv "$dir/$s.test.cp" "$dir/$s.test" \
                || touch stop \
            ; } >>"$log/$s.log" 2>&1 &
        done
        wait
        endcp="`date`: ========= end copy =============="
        for s in $sizes; do
            echo "$endcp" >> "$log/$s.read.err" 
            echo "$endcp" >> "$log/$s.write.err"
            echo "$endcp" >> "$log/$s.log"
        done
        i=`expr $i + 1`
    done

    for s in $sizes; do
        $time $cpfromkfs -k "$dir/$s.test" -d - 2>>"$log/$s.log" \
            | $chksum > "$log/$s.chk" &
    done
    wait

    status=0
    for s in $sizes; do
        if cmp "$log/$s.$randseed.chk" "$log/$s.chk"; then
            echo "$s test passed"
        else
            echo "$s test failed"
            status=1
        fi
    done

    if [ $status -eq 0 ]; then
        echo "Passed all tests."
    else
        exit $status
    fi
    itr=`expr $itr + 1`
done
