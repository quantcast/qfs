#!/bin/sh

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
qfs_no_git_version="1.2.1"

usage() {
    echo "
usage: $0 <options>

    Options:
        --info BINARY               get build information from the binary
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

check_if_have_git() {
    if ! git="$(command -v git)" || [ -z "$git" ]; then
        return 0
    fi

    return 1
}

get_head() {
    check_if_have_git
    if [ $? -eq 0 ]; then
        echo "unknown"
        return 0
    fi

    echo $(git rev-parse HEAD)
}

get_release() {
    # override
    echo "$qfs_no_git_version"
    return 0

    check_if_have_git
    if [ $? -eq 0 ]; then
        echo "$qfs_no_git_version"
        return 0
    fi

    # if the hash doesn't match any known ref, just report the hash itself
    HEAD=$(get_head)
    REF=$(git show-ref --tags --dereference | grep $HEAD)
    if [ -z "$REF" ]; then
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
    check_if_have_git
    if [ $? -eq 0 ]; then
        echo "unknown"
        return 0
    fi

    echo $(git remote -v show | grep origin | grep '(fetch)' | awk '{print $2}')
}

get_branch() {
    check_if_have_git
    if [ $? -eq 0 ]; then
        echo "unknown"
        return 0
    fi

    echo $(git rev-parse --abbrev-ref HEAD | sed -e 's/HEAD/unknown/g')
}

OPTS=$(getopt \
    -n $0 \
    -o '' \
    -l 'info:' \
    -l 'head' \
    -l 'release' \
    -l 'remote' \
    -l 'branch' \
    -l 'generate' \
    -l 'build-type:' \
    -l 'source-dir:' \
    -l 'outfile:' \
    -l 'extra:' \
    -l 'help' \
    -- "$@")

if [ $? != 0 ] ; then
    usage
fi

while true ; do
    case "$1" in
        --info)
            INFO=true
            BINARY_PATH=$2 ; shift 2
            ;;
        --head)
            get_head
            exit
            ;;
        --release)
            get_release
            exit
            ;;
        --remote)
            get_remote
            exit
            ;;
        --branch)
            get_branch
            exit
            ;;
        --generate)
            GENERATE=true ; shift 1
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
if [ "$INFO" = "true" ]; then
    strings -a "$BINARY_PATH" | awk '/KFS_BUILD_INFO_START/,/KFS_BUILD_INFO_END/'
    exit
fi

# if we are generating, some required parameters must be present
if [ "$GENERATE" = "true" ]; then
    for var in BUILD_TYPE SOURCE_DIR OUTFILE ; do
        if [ -z "$(eval "echo \$$var")" ]; then
            echo "--generate specified, but missing required param: $var"
            usage
        fi
    done
fi

# generate build information for the binary to compile into itself
HEAD=$(get_head)
RELEASE=$(get_release)
REMOTE=$(get_remote)
BRANCH=$(get_branch)

tmpfile=$(mktemp tmp.XXXXXXXXXX)

{
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
echo "$EXTRA"
if [ "$HEAD" != "unknown" ]; then
    echo "git config:"
    git config -l
    echo "git status:"
    git status --porcelain -- "$SOURCE_DIR"
    echo "git branch:"
    git branch -v --no-abbrev --no-color
    echo "git remote:"
    git remote -v
    echo "version:"
    echo "${REMOTE}/${BRANCH}@$HEAD"
else
    echo 'git source build version not available'
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
    "'"${RELEASE}-${REMOTE}/${BRANCH}@$HEAD"'"
);

}
'

} > "$tmpfile"

mv "$tmpfile" $OUTFILE
