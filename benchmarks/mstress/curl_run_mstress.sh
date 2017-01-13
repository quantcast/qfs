#!/bin/bash
#
# $Id$
#
# Created: 2013/02/08
# Author: Jeremy R. Fishman
#
# Copyright 2013-2017 Quantcast Corporation. All rights reserved.
#
# This file is part of Quantcast File System.
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

PORT=40000 # metaserver client port
CLIENTS=5
LEVELS=3
INODES=10
STATS=1000

url=$1
tarfile=${2:-"mstress.tgz"}
if [ -z "$url" ]
then
	echo >&2 "Usage: $0 url [tgz]"
	exit 64
fi

date=`date +%Y%m%d%H%M%S`
exec > >(tee mstress.log.$date)
exec 2>&1

echo "Downloading: $url"
tmpfile=`mktemp $tarfile.XXXXXX` || exit $?
if [ -f "$tarfile" ]
then
	code=`curl -sf "$url" -z "$tarfile" -o "$tmpfile" -w '%{http_code}'`
else
	code=`curl -sf "$url" -o "$tmpfile" -w '%{http_code}'`
fi

if [ $? -ne 0 ]
then
	echo >&2 "Failed to curl $url"
	rm "$tmpfile"
elif [ "$code" = 200 ]
then
	mv "$tmpfile" "$tarfile"
elif [ "$code" = 304 ]
then
	echo "  not modified"
	rm "$tmpfile"
fi

if [ ! -f "$tarfile" ]
then
	exit 1
fi

echo "Unpacking $tarfile"
contents=`tar xzfv "$tarfile"` || exit $?
echo -n "$contents" | sed 's/^/  /'

echo "Installing client files"
./mstress/mstress_install.sh localhost || exit $?

echo "Configuring meta and chunk servers"
cd mstress
cat >setup.cfg <<EOF
[metaserver]
hostname    = localhost
rundir      = ~/qfsbase/meta
clientport  = $PORT
chunkport   = `expr $PORT + 100`
clusterkey  = myTestCluster

[chunkserver1]
hostname    = localhost
rundir      = ~/qfsbase/chunk1
chunkport   = `expr $PORT + 1000`
# in practice, have a chunkdir per disk.
chunkdirs   = ~/qfsbase/chunk1/chunkdir11 ~/qfsbase/chunk1/chunkdir12

[chunkserver2]
hostname    = localhost
rundir      = ~/qfsbase/chunk2
chunkport   = `expr $PORT + 1001`
# in practice, have a chunkdir per disk.
chunkdirs   = ~/qfsbase/chunk2/chunkdir21

[webui]
hostname    = localhost
rundir      = ~/qfsbase/web
webport     = 42000
EOF
./setup.py -c setup.cfg -r . -s . -a install || exit $?

echo "Running benchmark"
(
	./mstress_plan.py -c localhost -n $CLIENTS -t file -l $LEVELS -i $INODES -s $STATS -o mstress.plan &&
	./mstress.py -f qfs -s localhost -p $PORT -a mstress.plan
	# disabled - unknown hanging error on create
	#./mstress.py -f hdfs -s localhost -p $PORT -a mstress.plan
	)
ret=$?

uid=`id -u`
pid=`pgrep -u $uid metaserver`
ctime=`awk '{print $14,$15}' /proc/$pid/stat`
rev=`strings bin/metaserver | grep -B1 KFS_BUILD_INFO_END | head -1 | cut -d@ -f2 | cut -c-16`

./setup.py -c setup.cfg -r . -s . -a uninstall || exit $?

if [ $ret -eq 0 ]
then
	echo "Metaserver cpu usage (date gitrev utime stime):"
	echo "$date $rev $ctime"
fi
exit $ret
