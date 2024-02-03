#!/bin/sh
#
# $Id$
#
# Copyright 2016-2017 Quantcast Corporation. All rights reserved.
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

if [ $# -ne 1 ]; then
    echo "usage: $0 source_dir"
    exit 1
fi

SRC="`cd "$1" > /dev/null && pwd`"

MYRAT_VERS=0.16.1
MYURL="https://downloads.apache.org/creadur/apache-rat-${MYRAT_VERS}/apache-rat-${MYRAT_VERS}-bin.tar.gz"
MYSHAURL="https://dlcdn.apache.org/creadur/apache-rat-${MYRAT_VERS}/apache-rat-${MYRAT_VERS}-bin.tar.gz.sha512"
MYTAR="`basename "$MYURL"`"
MYNAME="`basename "$MYTAR" -bin.tar.gz`"
MYJAR="$MYNAME/$MYNAME.jar"

if [ -f "$MYJAR" ]; then
    true
else
    rm -f "$MYTAR"
    if curl --retry 3 -Ss -o "$MYTAR" "$MYURL"; then
        MYTARSHA="`curl --retry 3 -Ss "$MYSHAURL" \
            | sed -e 's/^.*://' | tr -d ' \n' | tr ABCDEF abcdef`"
        MYACTSHA="`openssl sha512 < "$MYTAR" | sed -e 's/^.*)= *//'`"
        if [ x"$MYACTSHA" = x"$MYTARSHA" ]; then
            true
        else
            echo "$MYTAR: sha512 mismatch:" \
                "downloaded: $MYACTSHA, expected: $MYTARSHA"
            rm "$MYTAR"
            exit 1
        fi
    else
        rm -f "$MYTAR"
        exit 1
    fi
    tar -xf "$MYTAR"
    status=$?
    rm "$MYTAR"
    if [ $status -ne 0 ]; then
        exit
    fi
fi

java -jar "$MYJAR" --dir "$SRC" -E "$SRC/.ratignore" \
| awk '
    BEGIN { ret = 1; }
    /Unknown Licenses/ {
        if (0 == $1) {
            ret = 0
        }
        print;
    }
    /^==/{ print; }
    END { exit ret; }
'
