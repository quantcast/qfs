#!/bin/sh
#
# $Id: #1 $
#
# Created 2010/5/25
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

if [ ! -x rwtest ]; then
    gcc -Wall -g3 -DIOV_MAX=256 rwtest.c -o rwtest || exit
fi
tf="rwtest.$$.file"
rm -rf "$tf";
while date; do
    xfs_io -f -c 'resvsp 0 10m' -c 'truncate 10m' "$tf"
    ./rwtest "$tf" || break
    rm "$tf"
done > "rwtest.$$.log" 2>&1 &
