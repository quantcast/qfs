#!/bin/sh
#
# $Id$
#
# Created 2009
# Author: Mike Ovsiannikov
#
# Copyright 2009 Quantcast Corp.
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

outd="./`basename "$0" .sh`.$$.out"
mkdir "$outd" || exit
awk '{
    print  $1
}' ${0+"$@"} \
| while read h; do
    {
    echo "========================== $h =================================";
    ssh -o StrictHostKeyChecking=no -l root "$h" sh -c \'\
        'hostname && ' \
        'cd /opt/kfs-sort && ls -ltr core.* bin/chunkserver && ' \
        'md5sum bin/chunkserver && ' \
        'gdbtmp=/tmp/$$tmp.gdb && ' \
        '{ echo bt && echo set print pretty && echo quit ; } > "$gdbtmp" && ' \
        'ls -tr core.* | tr " " "\n" | tail -n 2 ' \
        '| while read n; do ' \
            'echo ===================== "$n" =======================; ' \
            'gunzip "$n"; ' \
            'gdb bin/chunkserver `basename "$n" .gz` < "$gdbtmp"; ' \
            'echo ""; ' \
        'done; ' \
        'rm -rf "$gdbtmp"; ' \
    \';
    } > "$outd/$h.trace.gdb" 2>&1 &
done
wait
