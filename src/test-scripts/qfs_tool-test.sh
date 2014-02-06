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

# The following assumes that user name, and host name have no spaces.

qfstooluser=${qfstooluser-`id -un`}
qfstoolgroup=${qfstoolgroup-`id -gn`}
qfstoolmeta=${qfstoolmeta-'127.0.0.1:20000'}
qfstoolopts=${qfstoolopts-}
udir="/user/$qfstooluser"
dir="$udir/qfstool/`hostname`/`basename "$0" .sh`${1-}"
testdir="`basename "$0" .sh`${1-}"
kfstools=${kfstools-'src/cc/tools'}
kfsdevtools=${kfsdevtools-'src/cc/devtools'}
qfstool="qfs $qfstoolopts -fs qfs://$qfstoolmeta"
qfstoolrand='rand-sfmt'
qfstoolrandseed=1234
qfstoolsizes=${qfstoolsizes-'1 2 3 127 511 1024 65535 65536 65537 70300 1e5 67108864 67108865 100e6 250e6'}
qfstoolumask=${qfstoolumask-0022}
qfstooltrace=${qfstooltrace-no}
if [ x"$qfstoolrootauthcfg" = x ]; then
    qfstoolrootauthcfg='/dev/null'
fi

if [ x"$qfstooltrace" = x'yes' -o  x"$qfstooltrace" = x'on' ]; then
    set -x
fi

qfstoolchksum=${qfstoolchksum-sha1sum}
if [ x"`{ cat /dev/null | $qfstoolchksum ; } 2>/dev/null`" = x ]; then
    qfstoolchksum='openssl sha1'
    [ x"`{ cat /dev/null | $qfstoolchksum ; }`" = x ] && exit 1
fi

if [ -d "${kfstools}" ]; then
    kfstools=`cd "${kfstools}" && pwd`
fi
if [ -d "${kfsdevtools}" ]; then
    kfsdevtools=`cd "${kfsdevtools}" && pwd`
fi
PATH="`pwd`:${PATH}"
[ -d "${kfstools}" ] && PATH="${kfstools}:${PATH}"
[ -d "${kfsdevtools}" ] && PATH="${kfsdevtools}:${PATH}"
export PATH
trap 'echo "`basename "$0"`: test failed."' EXIT

set -e
umask $qfstoolumask
$qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -mkdir "$udir"
$qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -chown "$qfstooluser" "$udir"

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

$qfstool -test -e "$dir" && $qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -rmr "$dir"
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
$qfstool -ls "$dst/*" && exit 1
$qfstool -put "$dst" "$dst" && exit 1

echo "Creating test files"
rm -rf "$testdir"
mkdir "$testdir"
rseed=$qfstoolrandseed
ts=5 # size of echo "test"
for s in $qfstoolsizes; do
    mkdir "$testdir/$s"
    $qfstoolrand -g $s $rseed > "$testdir/$s/$s.dat"
    echo "test" > "$testdir/$s/$s.txt"
    rseed=`expr $rseed + 1`
done

dstbn=`basename "$testdir"`
testdircp="${testdir}-cp"
$qfstool -copyFromLocal "$testdir" "$dir/"

for pass in 1 2; do
    rm -rf "$testdircp"
    $qfstool -copyToLocal "$dir/$dstbn"  "$testdircp"

    for s in $qfstoolsizes; do
        test x"`$qfstoolchksum < "$testdir/$s/$s.dat"`" = \
            x"`$qfstoolchksum < "$testdircp/$s/$s.dat"`"
        test x"`$qfstoolchksum < "$testdir/$s/$s.txt"`" = \
            x"`$qfstoolchksum < "$testdircp/$s/$s.txt"`"
    done
done

$qfstool -cp "$dir/$dstbn" "$dir/$dstbn.cp"
for s in $qfstoolsizes; do
    test x"`$qfstool -cat "$dir/$dstbn/$s/$s.dat" | $qfstoolchksum`" = \
        x"`$qfstool -cat "$dir/$dstbn.cp/$s/$s.dat" | $qfstoolchksum`"
    test x"`$qfstool -cat "$dir/$dstbn/$s/$s.txt" | $qfstoolchksum`" = \
        x"`$qfstool -cat "$dir/$dstbn.cp/$s/$s.txt" | $qfstoolchksum`"
done

tmpout="$testdir/tmp.out"

$qfstool -du "$dir/$dstbn" > "$tmpout"
awk -v p="$dir/$dstbn/" -v s="$qfstoolsizes" -v ts=$ts '
BEGIN {
    ic = split(s, v)
    t = 0
    for (i in v) {
        t += v[i]
        t += ts
    }
    err = 0
}
{
    if (NR == 1) {
        if ($1 != "Found" || $3 != "items" || $2 != ic) {
            err = 1
            print $0
            exit(1)
        }
    } else if (index($2, p) != 1) {
        err = 1
        print $0
        exit(1)
    } else {
        tt += $1
        fz = substr($2, length(p) + 1) + ts
        if ($1 != fz) {
            err = 1
            exit(1)
        }
    }
}
END {
    if (tt != t && ! err) {
        print "total mismatch " t " " tt
        exit(1)
    }
}
' "$tmpout" 

$qfstool -duh "$dir/$dstbn" > "$tmpout"
awk -v p="$dir/$dstbn/" -v s="$qfstoolsizes" -v ts=$ts '
BEGIN {
    ic = split(s, v)
    m["KB"] = 1024
    m["MB"] = 1024 * 1024
    m["GB"] = 1024 * 1024 * 1024
    m["TB"] = 1024 * 1024 * 1024 * 1024
}
{
    if (NR == 1) {
        if ($1 != "Found" || $3 != "items" || $2 != ic) {
            print $0
            exit(1)
        }
    } else if (index($3, p) != 1) {
        print $0
        exit(1)
    } else {
        sz = $1 * m[$2]
        d = m[$2] * 5e-2
        fz = substr($3, length(p) + 1) + ts
        if (sz < fz - d || fz + d < sz) {
            print fz " " d
            print $0
            exit(1)
        }
    }
}
' "$tmpout" 

$qfstool -dus "$dir/$dstbn" > "$tmpout"
awk -v p="$dir/$dstbn" -v s="$qfstoolsizes" -v ts=$ts '
BEGIN {
    sz = split(s, v)
    t = 0
    for (i in v) {
        t += v[i]
        t += ts
    }
}
{
    if (p != $1 || t != $2 || NR != 1) {
        print $0
        exit(1)
    }
}
' "$tmpout" 

$qfstool -dush "$dir/$dstbn" > "$tmpout"
awk -v p="$dir/$dstbn" -v s="$qfstoolsizes" -v ts=$ts '
BEGIN {
    sz = split(s, v)
    t = 0
    for (i in v) {
        t += v[i]
        t += ts
    }
    m["KB"] = 1024
    m["MB"] = 1024 * 1024
    m["GB"] = 1024 * 1024 * 1024
    m["TB"] = 1024 * 1024 * 1024 * 1024
}
{
    sz = $2 * m[$3]
    d = m[$3] * 5e-2
    if (p != $1 || sz < t - d || t + d < sz || NR != 1) {
        print $0
        exit(1)
    }
}
' "$tmpout" 

$qfstool -lsr "$dir/$dstbn" > "$tmpout"
awk -v p="$dir/$dstbn/" \
    -v s="$qfstoolsizes" -v ts=$ts \
    -v usr="`echo "${qfstooluser}" | sed -e 's/\\\\/\\\\\\\\/g'`" '
BEGIN {
    sz = split(s, v)
    t = 0
    for (i in v) {
        t += v[i]
        t += ts
    }
    err = 0
    tt = 0
}
{
    if (index($NF, p) != 1) {
        err = 1
        print $0
        exit(1)
    }
    if (substr($1, 1, 1) == "-") {
        tt += $(NF-3)
    }
}
END {
    if (tt != t && ! err) {
        print "total mismatch " t " " tt
        exit(1)
    }
}
' "$tmpout" 

tdir="$dir/$dstbn/1"
tfile="$tdir/1.txt"
$qfstool -setrep -R -w 2 "$tdir"
$qfstool -chmod -R a-x "$tdir"
$qfstool -lsr "$tfile" && exit 1
$qfstool -chmod -R a+X "$tdir"
$qfstool -lsr "$tfile" > "$tmpout"
awk -v p="$tfile" '
{
    if ($1 != "-rw-r--r--" || $2 != 2 || $NF != p) {
        print "$0"
        exit(1)
    }
}
' "$tmpout" 
$qfstool -setrep 1 "$tfile"
$qfstool -setrep -R 1 "$tdir"
$qfstool -mv "$tdir" "$tfile" && exit 1

$qfstool -chmod o-r "$tfile"
$qfstool -tail "$tfile" > /dev/null
$qfstool -chmod ug-r "$tfile"
$qfstool -tail "$tfile" > /dev/null && exit 1
$qfstool -chmod a+r "$tfile"
$qfstool -tail "$tfile" > /dev/null
$qfstool -setModTime "$tfile" 3600000
$qfstool -astat "$tfile" > "$tmpout"
awk '
{
    if ($1 == "Modified:" && $2 != 3600) {
        print $0
        exit(1)
    }
}
' "$tmpout"
$qfstool -chown -R 0:0 "$tdir" && exit 1
$qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -chown -R 0:0 "$tdir"
$qfstool -astat "$tdir" > "$tmpout"
$qfstool -astat "$tdir/*.*" >> "$tmpout"
awk '
BEGIN {
    t = 0
    d = 0
}
{
    if (($1 == "Owner:" && $2 != 0) || ($1 == "Group:" && $2 != 0)) {
        print $0
        exit(1)
    }
    if ($1 == "Uri:") {
        t++
    }
    if ($1 == "Type:" && $2 == "dir") {
        d++
    }
}
END {
    if (t != 3 && d != 1) {
        exit(1)
    }
}
' "$tmpout"
$qfstool -chown -R "${qfstooluser}:${qfstoolgroup}" "$tdir" && exit 1
$qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -chown -R "${qfstooluser}" "$tdir"
$qfstool -D fs.euser=0 -cfg "$qfstoolrootauthcfg" -chgrp -R "${qfstoolgroup}" "$tdir"
$qfstool -ls "$tdir/*" > "$tmpout"
awk -v usr="`echo "${qfstooluser}" | sed -e 's/\\\\/\\\\\\\\/g'`" \
    -v grp="`echo "${qfstoolgroup}" | sed -e 's/\\\\/\\\\\\\\/g'`" \
    -v p="`echo "${tdir}" | sed -e 's/\\\\/\\\\\\\\/g'`/" '
{
    if (($1 != "Found" && $2 != "items") &&
            ($3 != usr || index($0, grp) == 0 || index($NF, p) == 0)) {
        print $0
        exit(1)
    }
}
' "$tmpout"

tfilegz="$tdir/testgz"
echo 'this is a test' | gzip -c | $qfstool -put - "$tfilegz"
test x"`$qfstool -text "$tfilegz"`" = x"`echo 'this is a test'`"

tfiletxt="$tdir/testtxt"
echo 'this is a test' | $qfstool -put - "$tfiletxt"
test x"`$qfstool -text "$tfilegz"`" = x"`echo 'this is a test'`"

$qfstool -rmr -skipTrash "$dir" "local://$testdir" "local://$testdircp"

echo "`basename "$0"`: passed all tests."
trap '' EXIT

exit 0
