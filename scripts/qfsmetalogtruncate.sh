#!/bin/sh
#
# $Id$
#
# Created 2022/04/25
# Author: Mike Ovsiannikov
#
# Copyright 2022 Quantcast Corporation. All rights reserved.
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

qfs_cp_dir=kfscp
qfs_log_dir=kfslog
qfs_log_seq=

print_usage_exit()
{
    echo "Usage $1:"
    echo "  -c <checkpoint dir> (default kfscp)"
    echo "  -l <log dir> (default kfslog)"
    echo "  -s <log sequence to truncate at>" \
        "(For example: -s '1 2 3abc')"
    echo "  -h|--help display help / usage."
    exit $2
}

while [ $# -gt 0 ]; do
    if [ x"$1" = x'-c'  -a $# -gt 1 ]; then
        shift
        qfs_cp_dir=$1
    elif [ x"$1" = x'-l'  -a $# -gt 1 ]; then
        shift
        qfs_log_dir=$1
    elif [ x"$1" = x'-s' -a $# -gt 1 ]; then
        shift
        qfs_log_seq=$1
    elif [ x"$1" = x'-h' -o x"$1" = x'--help' ]; then
        echo "
This program is intended to be used for debugging and possibly file system
recovery / repair / roll back by discarding transaction log RPC blocks with
sequence number equal or greater to the the specified log sequence number.
Typically the sequence number of offending RPC reported by the meta server or
log compactor.
Truncating log typically results in some meta data loss.
        "
        print_usage_exit $0 0
    else
        print_usage_exit $0 1 1>&2
    fi
    shift
done

[ -d "$qfs_log_dir" ] || {
    echo "Log is not a directory: $qfs_log_dir" 1>&2
    exit 1
}

[ -d "$qfs_cp_dir" ] || {
    echo "Checkpoint is not a directory: $qfs_log_dir" 1>&2
    exit 1
}

[ -f "$qfs_cp_dir"/latest ] || {
    echo "No latest file in checkpoint directory: $qfs_log_dir" 1>&2
    exit 1
}

expr "$qfs_log_seq" : \
    '^[0-9a-f][0-9a-f]* [0-9a-f][0-9a-f]* [0-9a-f][0-9a-f]*$' > /dev/null || {
    echo "Unexpected log sequence format: $qfs_log_seq" 1>&2
    echo "Expected 3 space separated hex numbers. For example: 1 2 3abc" 1>&2
    print_usage_exit $0 1 1>&2
}

awk '
/^c\// {
    commit_line_num = FNR
}
/z[=\/]'"$qfs_log_seq"';*$/ {
    print commit_line_num
    print FILENAME
    exit 0
}
END {
    print ""
    exit 1
}' "$qfs_log_dir"/log.*.*.*.* \
| {
    read commit_line_num
    [ x"$commit_line_num" = x ] && {
        echo "Error: no log sequence $qfs_log_seq found" 1>&2
        exit 1
    }
    read log_file || exit
    head -n $commit_line_num "$log_file" > "$log_file".tmp || exit
    mv "$log_file".tmp "$log_file" || exit
    log_seg_num=$(echo "$log_file" | sed -e 's/^.*\.//')
    log_dir=$(dirname "$log_file")

    # Remove "future" log segments.
    i=$log_seg_num
    while true; do
        i=$(expr $i + 1)
        rm "$log_dir"/log.*.*.*.$i 2>/dev/null || break
    done
    # Re-ceate last link
    last_link=$log_dir/last
    rm -f "$last_link"
    i=$(expr $log_seg_num - 1)
    prev_log=$(echo "$log_dir"/log.*.*.*.$i)
    [ -f "$prev_log" ] && ln "$prev_log" "$last_link"

    # Cleanup temp files, if any.
    rm -f "$qfs_cp_dir"/chkpt.*.*.*.tmp
    # Remove "future" checkpoints.
    awk -F '[.]' -v sn=$log_seg_num '
    BEGIN {
        mls = -1
        mf = ""
    }
    /^log\//{
        if(sn < $NF){
            print FILENAME
        } else {
            if (mls < $NF) {
                mls = $NF
                mf = FILENAME
            }
        }
        nextfile
    }
    END {
        print mf
    }' "$qfs_cp_dir"/chkpt.*.*.* \
    | {
        pf=
        while true; do
            read fn || {
                [ x"$pf" = x ] && {
                    echo "Error: no checkpoints left" 1>&2
                    exit 1
                }
                dn=$(dirname "$pf")
                # Create latest hard link.
                cp_latest=$dn/latest
                rm "$cp_latest" || exit
                ln "$pf" "$cp_latest" || exit
                break
            }
            [ x"$pf" = x ] || {
                rm "$pf" || exit
            }
            pf=$fn
        done
    }
}
