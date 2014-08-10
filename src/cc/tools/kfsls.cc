//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/10/28
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// \brief Tool for listing directory contents (ala ls -l).
//
//----------------------------------------------------------------------------

#include "tools/kfsshell.h"
#include "libclient/KfsClient.h"
#include "common/StdAllocator.h"

#include <iostream>
#include <fstream>
#include <cerrno>
#include <ostream>
#include <iomanip>
#include <algorithm>
#include <map>

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <sys/types.h>

namespace KFS
{
namespace tools
{
using std::cout;
using std::cerr;
using std::endl;
using std::fixed;
using std::setw;
using std::setprecision;
using std::right;
using std::ofstream;
using std::ostringstream;
using std::vector;
using std::map;
using std::less;
using std::pair;
using std::make_pair;

static int dirList(KfsClient *client,
    string kfsdirname, bool longMode, bool humanReadable,
    bool timeInSecs, bool recursive);
static int doDirList(KfsClient *client, string kfsdirname);
static int doDirListPlusAttr(KfsClient *client,
    string kfsdirname, bool humanReadable, bool timeInSecs,
    bool recursive, bool longMode, int level = 0);
static void printAttrInfo(KfsClient *client, const KfsFileAttr& attr,
    bool humanReadable, bool timeInSecs, bool longMode);
static void getTimeString(time_t time, char *buf, int bufLen = 256);

int
handleLs(KfsClient *client, const vector<string> &args)
{
    bool longMode = false, humanReadable = false,
        timeInSecs = false, recursive = false;
    vector<string>::size_type pathIndex = 0, i;

    if ((args.size() >= 1) && (args[0] == "--help")) {
        cout << "Usage: ls {-lhtr} {<dir>} " << endl;
        return 0;
    }

    if (args.size() >= 1) {
        if (args[0][0] == '-') {
            pathIndex = 1;
            for (i = 1; i < args[0].size(); i++) {
                switch (args[0][i]) {
                    case 'l':
                        longMode = true;
                        break;
                    case 'h':
                        humanReadable = true;
                        break;
                    case 't':
                        timeInSecs = true;
                        break;
                    case 'r':
                        recursive = true;
                        break;
                    default:
                        break;
                }
            }
        }
    }

    if (args.size() > pathIndex) {
        for (i = pathIndex; i < args.size(); i++) {
            const int ret = dirList(client, args[i], longMode,
                humanReadable, timeInSecs, recursive);
            if (ret) {
                return ret;
            }
        }
        return 0;
    }
    return dirList(client, ".", longMode, humanReadable, timeInSecs, recursive);
}

int
dirList(KfsClient *client,
    string kfsdirname, bool longMode, bool humanReadable,
    bool timeInSecs, bool recursive)
{
    if (longMode || recursive)
        return doDirListPlusAttr(client, kfsdirname, humanReadable,
            timeInSecs, recursive, longMode);
    else
        return doDirList(client, kfsdirname);
}

int
doDirList(KfsClient *kfsClient, string kfsdirname)
{
    string kfssubdir, subdir;
    int res;
    vector<string> entries;
    vector<string>::size_type i;

    if (kfsClient->IsFile(kfsdirname.c_str())) {
        cout << kfsdirname << "\n";
        return 0;
    }

    if ((res = kfsClient->Readdir(kfsdirname.c_str(), entries)) < 0) {
        cerr << kfsdirname << ": " << ErrorCodeToStr(res) << "\n";
        return res;
    }

    // we could provide info of whether the thing is a dir...but, later
    for (i = 0; i < entries.size(); ++i) {
        if (entries[i] == "." || entries[i] == "..") {
            continue;
        }
        cout << entries[i] << "\n";
    }
    return 0;
}

int
doDirListPlusAttr(KfsClient *kfsClient, string kfsdirname, bool humanReadable,
    bool timeInSecs, bool recursive, bool longMode, int level)
{
    string kfssubdir, subdir;
    int res;
    vector<KfsFileAttr> fileInfo;
    vector<KfsFileAttr>::size_type i;

    if (level == 0 && kfsClient->IsFile(kfsdirname.c_str())) {
        KfsFileAttr attr;
        kfsClient->Stat(kfsdirname.c_str(), attr);
        cout << kfsdirname;
        printAttrInfo(kfsClient, attr, humanReadable, timeInSecs, longMode);
        return 0;
    }
    if ((res = kfsClient->ReaddirPlus(kfsdirname.c_str(), fileInfo)) < 0) {
        cerr << kfsdirname << ": " << ErrorCodeToStr(res) << endl;
        return res;
    }

    string prefix;
    if (recursive) {
        prefix = kfsdirname;
        if (! prefix.empty() && prefix[prefix.length() - 1] != '/') {
            prefix += "/";
        }
    }
    for (i = 0; i < fileInfo.size(); ++i) {
        if (fileInfo[i].isDirectory &&
                (fileInfo[i].filename == "." ||
                fileInfo[i].filename == "..")) {
            continue;
        }
        cout << prefix << fileInfo[i].filename;
        printAttrInfo(kfsClient, fileInfo[i], humanReadable, timeInSecs, longMode);
        if (recursive && fileInfo[i].isDirectory) {
            const int ret = doDirListPlusAttr(
                kfsClient, prefix + fileInfo[i].filename, humanReadable,
                timeInSecs, recursive, longMode, ++level);
            if (ret) {
                return ret;
            }
        }
    }
    return 0;
}

typedef map<kfsUid_t, string, less<kfsUid_t>,
    StdFastAllocator<pair<const kfsUid_t, string> >
> UserNames;
typedef map<kfsGid_t, string, less<kfsGid_t>,
    StdFastAllocator<pair<const kfsGid_t, string> >
> GroupNames;

static UserNames  sUserNames;
static GroupNames sGroupNames;

void
printAttrInfo(KfsClient *client, const KfsFileAttr& attr,
    bool humanReadable, bool timeInSecs, bool longMode)
{
    if (! longMode) {
        cout << (attr.isDirectory ? "/\n" : "\n");
        return;
    }
    char timeBuf[256];
    if (timeInSecs) {
        snprintf(timeBuf, sizeof(timeBuf), "%lld",
            (long long)attr.mtime.tv_sec);
    } else {
        getTimeString(attr.mtime.tv_sec, timeBuf);
    }
    if (attr.isDirectory) {
        cout << "/\t<dir>\t";
    } else if (attr.striperType != KFS_STRIPED_FILE_TYPE_NONE) {
        cout << "\t<r" <<
            (attr.striperType != KFS_STRIPED_FILE_TYPE_UNKNOWN ? "s " : " ") <<
            attr.numReplicas << "," <<
            attr.numStripes << "+" << attr.numRecoveryStripes <<
        ">\t";
    } else {
        cout << "\t<r " << attr.numReplicas << ">\t";
    }
    if (attr.mode == kKfsModeUndef) {
        cout << "-\t";
    } else {
        for (int i = 8; i > 0; ) {
            const char* perm[2] = {"---", "rwx"};
            for (int k = 0; k < 3; k++) {
                cout << perm[(attr.mode >> i--) & 1][k];
            }
        }
        if (attr.isDirectory && attr.IsSticky()) {
            cout << "t";
        }
        cout << "\t";
    }
    if (attr.user == kKfsUserNone) {
        cout << "-\t";
    } else {
        UserNames::const_iterator it = sUserNames.find(attr.user);
        if (it == sUserNames.end()) {
            string user;
            string group;
            client->GetUserAndGroupNames(
                attr.user, attr.group, user, group);
            it = sUserNames.insert(make_pair(attr.user, user)).first;
            sGroupNames[attr.group] = group;
        }
        cout << it->second << "\t";
    }
    if (attr.group == kKfsGroupNone) {
        cout << "-\t";
    } else {
        GroupNames::const_iterator it = sGroupNames.find(attr.group);
        if (it == sGroupNames.end()) {
            string user;
            string group;
            client->GetUserAndGroupNames(
                attr.user, attr.group, user, group);
            it = sGroupNames.insert(make_pair(attr.group, group)).first;
            sUserNames[attr.user] = user;
        }
        cout << it->second << "\t";
    }
    if (! attr.isDirectory || attr.fileSize >= 0) {
        if (humanReadable) {
            cout << fixed << setprecision(1) << right << setw(5);
            if (attr.fileSize < (1 << 10)) {
                cout << attr.fileSize;
            } else if (attr.fileSize < (1 << 20)) {
                cout << (double)(attr.fileSize) / (1 << 10) << "K";
            } else if (attr.fileSize < (1 << 30)) {
                cout << (double)(attr.fileSize) / (1 << 20) << "M";
            } else if (attr.fileSize < (int64_t(1) << 40)) {
                cout << (double)(attr.fileSize) / (1 << 30) << "G";
            } else {
                cout << (double)(attr.fileSize) / (int64_t(1) << 40) << "T";
            }
        } else {
            cout << attr.fileSize;
        }
    }
    cout << '\t' << timeBuf << "\n";
}

void
getTimeString(time_t time, char *buf, int bufLen)
{
    struct tm locTime;

    localtime_r(&time, &locTime);
    strftime(buf, bufLen, "%b %e %H:%M", &locTime);
}
}} // KFS::tools
