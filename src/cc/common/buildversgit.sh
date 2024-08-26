#!/bin/sh
#
# $Id$
#
# Copyright 2010-2016 Quantcast Corporation. All rights reserved.
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

# default version to use if git is not available
qfs_no_git_version="2.2.7"

usage() {
    echo "
usage: $0 <options>

    Options:
        --info | -i BINARY          get build information from the binary
        --head                      print the head hash of qfs
        --release                   print the release version or hash of qfs
        --remote                    print the remote we are tracking
        --branch                    print the branch we are on

        --generate                  generate build info -- if this option is
                                    specified, then --build-type, --source-dir,
                                    and --outfile are all required
        --build-type BUILD_TYPE     the build type we built with
        --source-dir SOURCE_DIR     the directory where qfs source lives
        --outfile OUTFILE           the file to write generate c++ code
        --extra EXTRA               extra information to include in the output
                                    this parameter can be used multiple times

        --help                      show this help
    "
    exit 1
}

git_not_available() {
    if ! git="$(command -v git)" || [ x"$git" = x ]; then
        return 0
    fi
    return 1
}

get_head() {
    if git_not_available; then
        echo "unknown"
        return 0
    fi

    echo $(git rev-parse HEAD)
}

get_release() {
    if git_not_available; then
        echo "$qfs_no_git_version"
        return 0
    fi

    # if the hash doesn't match any known ref, just report the hash itself
    HEAD=$(get_head)
    REF=$(git show-ref --tags --dereference | grep $HEAD)
    if [ x"$REF" = x ]; then
        # special case: master
        MASTER=$(git show-ref refs/heads/master | awk '{print $1}')
        if [ "$HEAD" = "$MASTER" ]; then
            RELEASE="master"
        else
            RELEASE=$(git rev-parse --short $HEAD)
        fi
    else
        REF=$(echo $REF | sed -e 's,refs/tags/,,g')
        REF=$(echo $REF | sed -e 's,\^{},,g')
        REF=$(echo $REF | awk '{print $2}')
        RELEASE=$REF
    fi

    echo $RELEASE
}

get_remote() {
    if git_not_available; then
        echo "unknown"
        return 0
    fi

    echo $(git remote -v show | grep origin | grep '(fetch)' | awk '{print $2}')
}

get_branch() {
    if git_not_available; then
        echo "unknown"
        return 0
    fi

    echo $(git rev-parse --abbrev-ref HEAD | sed -e 's/HEAD/unknown/g')
}

display_git_info() {
    git config -l
    echo "git status:"
    git status --porcelain
    echo "git branch:"
    git branch -v --no-abbrev --no-color
    echo "git remote:"
    git remote -v
    echo "version:"
}

MYACTION=
GENERATE=0
while true ; do
    case "$1" in
        --info|-i)
            INFO=true
            BINARY_PATH=$2 ; shift 2
            ;;
        --head)
            MYACTION=get_head
            break
            ;;
        --release)
            MYACTION=get_release
            break
            ;;
        --remote)
            MYACTION=get_remote
            break
            ;;
        --branch)
            MYACTION=get_branch
            break
            ;;
        --generate)
            GENERATE=1 ; shift 1
            ;;
        --build-type)
            BUILD_TYPE=$2 ; shift 2
            ;;
        --source-dir)
            SOURCE_DIR=$2 ; shift 2
            ;;
        --outfile)
            OUTFILE=$2 ; shift 2
            ;;
        --extra)
            EXTRA="$EXTRA\n$2" ; shift 2
            ;;
        --help)
            usage
            ;;
        --|'')
            break
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# query build information from the binary
if [ "$INFO" = "true" -a x"$MYACTION" = x ]; then
    strings -a "$BINARY_PATH" | awk '/KFS_BUILD_INFO_START/,/KFS_BUILD_INFO_END/'
    exit
fi

ORIGDIR=$(pwd)

# Change into qfs source directory, in order to handle the invocations from
# arbitrary directory.
MYDIR=$(dirname "$0")
QFS_SRC_DIR=$(cd "$MYDIR/../../.." > /dev/null && pwd)
if [ x"$QFS_SRC_DIR" = x ]; then
    exit 1
fi

[ x"$MYACTION" = x ] || {
    (
        cd "$QFS_SRC_DIR" || exit
        $MYACTION
    )
    exit
}

if [ $GENERATE -eq 0 ]; then
    exit
fi

# if we are generating, some required parameters must be present
for var in BUILD_TYPE SOURCE_DIR OUTFILE ; do
    if [ x"$(eval "echo \$$var")" = x ]; then
        echo "--generate specified, but missing required param: $var"
        usage
    fi
done

# generate build information for the binary to compile into itself
tmpfile=$(mktemp tmp.XXXXXXXXXX)

(
ABS_SRC_DIR=$(cd "$SOURCE_DIR" > /dev/null && pwd)
if cd $SOURCE_DIR 2>/dev/null; then
    HEAD=$(get_head)
    RELEASE=$(get_release)
    REMOTE=$(get_remote)
    BRANCH=$(get_branch)
    SRC_DIR_GIT_VERSION=${REMOTE}/${BRANCH}@$HEAD
    SRC_REV_STR=${RELEASE}-${REMOTE}/${BRANCH}@$HEAD
else
    SRC_DIR_GIT_VERSION=
fi
cd "$QFS_SRC_DIR" > /dev/null || exit 1
HEAD=$(get_head)
RELEASE=$(get_release)
REMOTE=$(get_remote)
BRANCH=$(get_branch)
QFS_DIR_GIT_VERSION=${REMOTE}/${BRANCH}@$HEAD
QFS_SOURCE_REVISION_STRING=${RELEASE}-${REMOTE}/${BRANCH}@$HEAD

if [ x"$SRC_DIR_GIT_VERSION" = x ]; then
    true
elif [ x"$SRC_DIR_GIT_VERSION" = x"$QFS_DIR_GIT_VERSION" ]; then
    SRC_DIR_GIT_VERSION=
else
    QFS_SOURCE_REVISION_STRING="${QFS_SOURCE_REVISION_STRING};${SRC_REV_STR}"
fi

echo '
// Generated by '"$0"'. Do not edit.

#include "Version.h"
#include "hsieh_hash.h"

namespace KFS {

const std::string KFS_BUILD_INFO_STRING='

{
echo KFS_BUILD_INFO_START
echo "host: `hostname`"
echo "user: $USER"
echo "date: `date`"
echo "build type: $BUILD_TYPE"
echo "release: $RELEASE"
echo "source dir: $SOURCE_DIR"
echo "qfs source dir: $QFS_SRC_DIR"
echo "$EXTRA"
if [ x"$HEAD" = x'unknown' ]; then
    echo 'git source build version not available'
else
    display_git_info
    echo "$QFS_DIR_GIT_VERSION"
fi
if [ x"$SRC_DIR_GIT_VERSION" = x ]; then
    true
else
    cd "$ABS_SRC_DIR" > /dev/null || exit 1
    echo "-------------- source directory:"
    display_git_info
    echo "$SRC_DIR_GIT_VERSION"
fi
echo KFS_BUILD_INFO_END
} | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/^/"/' -e 's/$/\\n"/'

echo ';

static std::string MakeVersionHash()
{
    Hsieh_hash_fcn f;
    unsigned int h = (unsigned int)f(KFS_BUILD_INFO_STRING);
    std::string ret(2 * sizeof(h), '"'0'"');
    for (size_t i = ret.length() - 1; h != 0; i--) {
        ret[i] = "0123456789ABCDEF"[h & 0xF];
        h >>= 4;
    }
    return ret;
}

const std::string KFS_BUILD_VERSION_STRING(
    std::string("'"${RELEASE}-${HEAD}-${BUILD_TYPE}"'-") +
    MakeVersionHash()
);

const std::string KFS_SOURCE_REVISION_STRING(
    "'"$QFS_SOURCE_REVISION_STRING"'"
);

}
'

) > "$tmpfile" || {
    rm -f "$tmpfile"
    exit 1
}

mv "$tmpfile" "$OUTFILE"
