#!/bin/sh
#
#
# Created 2023/01/222
# Author: Mike Ovsiannikov
#
# Copyright 2023 Quantcast Corporation. All rights reserved.
#
# This file is part of Quantcast File System (QFS).
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
# Scans QFS meta server checkpoint and computes number of "object store" files
# and corresponding space used by files accessed, created, or modified
# before and after given time.

days=30

if [ $# -ge 1 -a x"$1" = x'-h' ]; then
    echo "Usage $0: [-d <days> (default 30)] qfs-latest-checkpoint
Scans QFS meta server checkpoint and computes number of "object store" files
and corresponding space used by files accessed, created, or modified
before and after given time.
File access time update must be enabled in meta server configuration file
with metaServer.ATimeUpdateResolution parameter.
Only intended to work with modern checkpoint format.
"
    
    exit 1
fi

if [ $# -ge 2 -a x"$1" = x'-d' ] && expr $2 : '[0-9]' > /dev/null; then
    shift
    days=$1
    shift
fi

awk $(awk 'BEGIN {
    # Check if hex conversion works, and if it does not, add -n arg
    b = "abc";
    a = "0x" b;
    a += 0;
    if (a<=0) {
        print "-n"
    }
}') -F / '
BEGIN {
    secs_in_day = 60 * 60 * 24;
    start_date="'"$(date)"'";
    now = '"$(date '+%s')"';
    days='"$days"';
    th = now - days * secs_in_day;
}
/^a\/file\// {
    # Only for object store files (replication 0).
    if ("0" == $8) {
        # Get max seconds of mtime, ctime, and atime.
        at = 0;
        for (i = 10; i <= 16; i += 3) {
            t = "0x" $i;
            t += 0;
            if (at < t) {
                at = t;
            }
        }
        s = "0x" $19;
        s += 0;
        # print at " " s " " (int(s / (64 * 1024 * 1024)) + 1);
        if (0 < s) {
            # Add block headers overhead.
            s += (int(s / (64 * 1024 * 1024)) + 1) * 8192;
            i = at < th ? 0 : 1;
            space[i] += s;
            time[i] += at;
            count[i] += 1;
        }
    }
}
END {
    tb = 1. / (1024 * 1024 * 1024 * 1024);
    print start_date;
    for (i = 0; i < 2; i += 1) {
        c = count[i];
        t = time[i];
        s = size[i];
        printf("%s than %d days: %15.0f files %10.3f TiB %10.2f avg. days old\n",
            0 == i ? "older" : "newer", days, c, s * tb,
            0 < c ? (now - t / c) / secs_in_day : 0);
    }
}
' ${1+"$@"}
