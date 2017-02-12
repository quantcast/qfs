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

MYURL='http://apache.mirrors.pair.com//creadur/apache-rat-0.12/apache-rat-0.12-bin.tar.gz'
MYSHA1URL='https://www.apache.org/dist/creadur/apache-rat-0.12/apache-rat-0.12-bin.tar.gz.sha1'
MYTAR="`basename "$MYURL"`"
MYNAME="`basename "$MYTAR" -bin.tar.gz`"
MYJAR="$MYNAME/$MYNAME.jar"

if [ -f "$MYJAR" ]; then
    true
else
    rm -f "$MYTAR"
    if curl --retry 3 -Ss -o "$MYTAR" "$MYURL"; then
        MYTARSHA1="`curl --retry 3 -Ss "$MYSHA1URL" | awk '{print $1}'`"
        MYACTSHA1="`openssl sha1 < "$MYTAR" | awk '{print $2}'`"
        if [ x"$MYACTSHA1" = x"$MYTARSHA1" ]; then
            true
        else
            echo "$MYTAR: sha1 mismatch:" \
                "downloaded: $MYACTSHA1, expected: $MYTARSHA1"
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
