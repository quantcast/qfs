#!/bin/sh

#
# $Id$
#
# Copyright 2012 Quantcast Corp.
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

logd=${1-.}
if [ -f "$logd"/current ]; then
    true
else
    "Usage: $0 log-dir [file-count]"
    exit 1
fi

{

echo "$logd"/@*.s | tr ' ' '\n' | sort | tail -n ${2-20} | xargs gunzip -c 
cat "$logd"/current

} | sed -e 's/^@[0-9a-fA-F]* //'
