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

SRC=$1
DIR=apache-rat-0.11
TAR=$DIR-bin.tar.gz
URL=http://mirror.cogentco.com/pub/apache/creadur/$DIR/$TAR

if [ ! -e $TAR ]; then
    curl --silent $URL > $TAR
fi

tar -xf $TAR
java -jar $DIR/$DIR.jar --dir $SRC -E $SRC/.ratignore | egrep '^==[^=]' | sed -e 's,==../../,,g'
