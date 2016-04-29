#!/bin/sh

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
