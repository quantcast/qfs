#!/bin/sh
#
# $Id$
#
# Created 2013/05/03
# Author: Mike Ovsiannikov
#
# Copyright 2013 Quantcast Corp.
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

qfstoolmeta=${qfstoolmeta-'127.0.0.1:20000'}
qfstoolopts=${qfstoolopts-}
udir="/user/$USER"
dir="$udir/qfstool/`hostname`/`basename "$0" .sh`${1-}"
log="`basename "$0" .sh`${1-}.log"
kfstools=${kfstools-'src/cc/tools'}
qfstool="qfs $qfstoolopts -fs qfs://$qfstoolmeta"
rand='rand-sfmt'
randseed=1234

if [ -d "${kfstools}" ]; then
    kfstools=`cd "${kfstools}" && pwd`
fi
PATH="`pwd`:${PATH}"
[ -d "${kfstools}" ] && PATH="${kfstools}:${PATH}"
export PATH

set -e
$qfstool -D fs.euser=0 -mkdir "$udir"
$qfstool -D fs.euser=0 -chown "$USER" "$udir"

# Test move to trash restrictions.
tdir="$udir/d$$"
$qfstool -mkdir "$tdir"
$qfstool -touchz "$tdir/0"
$qfstool -touchz "$tdir/1"
$qfstool -touchz "$tdir-f"
$qfstool -rm  "$tdir-f" && exit 1
$qfstool -D dfs.force.remove=true -rm "$tdir-f"
$qfstool -rm  "$tdir" && exit 1
$qfstool -rmr "$tdir" && exit 1
$qfstool -D dfs.force.remove=true -rm "$tdir" && exit 1
$qfstool -D dfs.force.remove=true -rmr "$tdir"

# Remove trash, to simplify checks later.
$qfstool -expunge
$qfstool -rmr -skipTrash "$udir/.Trash"
$qfstool -expunge
$qfstool -lsr "$udir/.Trash" && exit 1

$qfstool -mkdir "$dir"
$qfstool -rmr -skipTrash "$dir"
$qfstool -test -e "$dir" && exit 1
$qfstool -mkdir "$dir"
$qfstool -test -d "$dir"
$qfstool -test -e "$dir"
dst="$dir/f0"
$qfstool -touchz "$dir" && exit 1
$qfstool -touchz "$dst"
$qfstool -test -z "$dst"
$qfstool -du "$dst"
$qfstool -ls "$dst/*" && exit 1
ndst="$dir/f0.n"
$qfstool -mv "$dst" "$ndst"
$qfstool -test -e "$dst" && exit 1
$qfstool -mv "$ndst" "$dst"
$qfstool -test -e "$dst"

test x"`$qfstool -stat '%b %o %f %n' "$dst"`" = x"0 67108864 f `basename "$dst"`"
printf 'one' | $qfstool -put - "$dst" && exit 1

$qfstool -rm -skipTrash "$dst"
printf 'one' | $qfstool -put - "$dst"
test x"`$qfstool -get "$dst" -`" =  x'one'
test x"`$qfstool -stat '%b %o %F %n' "$dst"`" = x"3 67108864 regular file `basename "$dst"`"
test x"`$qfstool -stat '%b %o %F %n' "$dir"`" = x"3 67108864 directory `basename "$dir"`"

$qfstool -rm "$dst"
$qfstool -test -e "$udir/.Trash/Current/$dst"
$qfstool -test -e "$dst" && exit 1
$qfstool -ls "$dst/*" && exit

echo "TEST PASSED"

