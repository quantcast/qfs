#!/bin/sh
#
# $Id$
#
# Created 2010
# Author: Mike Ovsiannikov
#
# Copyright 2010 Quantcast Corp.
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

# /mnt/data*/qmr_kfs/kfs-sns/chunk/data/

if [ $# -lt 1 ]; then
    echo "Usage: $0 <chunk directories pattern> [<chukserver log files>]"
    exit 1
fi

chunkDir=$1
shift

grep 'Checksum mismatch for chunk=' ${1+"$@"} | \
grep  ChunkManager.cc  | \
sed -e 's/^.* chunk=//' | \
tr ':=' '  ' | \
awk -v p="$chunkDir" \
'{
    printf("od -w4 -t u4 -j %d -N %d %s/*.%s.* | grep -n %s\n", \
        $3/65536*4+40, ($5+65535)/65536*4, p, $1, $7);
}' | \
sh -x

