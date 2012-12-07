//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/23
// Author: Sriram Rao
//         Mike Ovsiannikov
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
// \brief Tool that copies a file/directory from a KFS path to another
// KFS path.  This does the analogous of "cp -r".
//
//----------------------------------------------------------------------------

#include "kfsshell.h"
#include "libclient/KfsClient.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>

namespace KFS {
namespace tools {

using std::cout;
using std::vector;
using std::string;

//
// Guts of the work to copy the file.
//
static int
CopyFile(KfsClient* kfsClient, const KfsFileAttr& attr,
    const string& src, const string& dst)
{
    KfsFileAttr dstAttr;
    int res = kfsClient->Stat(dst.c_str(), dstAttr, false);
    if (res != -ENOENT) {
        if (res == 0 && attr.fileId == dstAttr.fileId) {
            // same file
            cout << src << " and " << dst << " are the same file\n";
            return 0;
        }
        if (res != 0) {
            cout << src << ": " << ErrorCodeToStr(res) << "\n";
            return res;
        }
    }
    const int srcfd = kfsClient->Open(src.c_str(), O_RDONLY);
    if (srcfd < 0) {
        cout << "open: " << src <<
            ": " << ErrorCodeToStr(srcfd) << "\n";
        return srcfd;
    }
    const bool kExclusiveFlag = false;
    const bool kForceTypeFlag = false;
    const int dstfd = kfsClient->Create(
        dst.c_str(),
        attr.numReplicas,
        kExclusiveFlag,
        attr.numStripes,
        attr.numRecoveryStripes,
        attr.stripeSize,
        attr.striperType,
        kForceTypeFlag,
        attr.mode
    );
    if (dstfd < 0) {
        cout << "create " << dst << ": " << ErrorCodeToStr(dstfd) <<
            "\n";
        kfsClient->Close(srcfd);
        return dstfd;
    }

    const int   bufsize = 6 << 20;
    static char kfsBuf[bufsize];
    for (; ;) {
        res = kfsClient->Read(srcfd, kfsBuf, bufsize);
        if (res == 0) {
            break;
        }
        if (res < 0) {
            cout << "read: " << src <<
                ": " << ErrorCodeToStr(res) << "\n";
            break;
        }
        // write it out
        const int wr = res;
        res = kfsClient->Write(dstfd, kfsBuf, wr);
        if (res < 0) {
            cout << "write: " << dst <<
                ": " << ErrorCodeToStr(res) << "\n";
            break;
        }
        if (wr != res) {
            cout << "short write: " << dst <<
                ": " << res << "<" << wr << "\n";
            res = -EIO;
            break;
        }
    }
    kfsClient->Close(srcfd);
    const int cres = kfsClient->Close(dstfd);

    return (res != 0 ? res : cres);
}

//
// Given a file defined by a KFS srcPath, copy it to KFS as defined by
// dirPath
//
static int
CopyFileIntoDir(KfsClient* kfsClient, const KfsFileAttr& srcAttr,
    const string& srcPath, const string& dirPath)
{
    const char* const b = srcPath.c_str();
    const char*       e = b + srcPath.length();
    if (b < e) {
        --e;
    }
    while (b < e && *e == '/') {
        --e;
    }
    const char* f = e;
    while (b < f && f[-1] != '/') {
        --f;
    }
    if (e <= f) {
        cout << srcPath << ": invalid source path\n";
        return  -EINVAL;
    }
    string filename(f, e - f + 1);
    string dst = dirPath;
    if (! dst.empty() && *dst.rbegin() != '/') {
        dst += "/";
    }
    dst += filename;
    cout << srcPath << " " << dst << " " << dirPath << "\n";
    return CopyFile(kfsClient, srcAttr, srcPath, dst);
}

// Given a srcDirname, copy it to dirname.  Dirname will be created
// if it doesn't exist.  
static int
CopyDir(KfsClient* kfsClient, const string& srcDirname, const string& dstDirname)
{
    vector<KfsFileAttr> dirEntries;
    int                 res;
    if ((res = kfsClient->ReaddirPlus(srcDirname.c_str(), dirEntries)) < 0) {
        cout << "Readdir plus failed: " << ErrorCodeToStr(res) << "\n";
        return res;
    }
    if ((res = kfsClient->Mkdirs(dstDirname.c_str())) != 0 &&
            res != -EEXIST) {
        cout << dstDirname << ": " << ErrorCodeToStr(res) << "\n";
        return res;
    }
    string src(srcDirname + "/");
    string dst(dstDirname + "/");
    const size_t srcLen = src.length();
    const size_t dstLen = dst.length();
    for (vector<KfsFileAttr>::const_iterator it = dirEntries.begin();
            it != dirEntries.end();
            ++it) {
        const string& name = it->filename;
        if (name == "." || name == "..") {
            continue;
        }
        src.erase(srcLen);
        dst.erase(dstLen);
        src += name;
        dst += name;
        if (it->isDirectory) {
            res = CopyDir(kfsClient, src, dst);
        } else {
            res = CopyFile(kfsClient, *it, src, dst);
        }
        if (res != 0) {
            return res;
        }
    }
    return res;
}

int
handleCopy(KfsClient *kfsClient, const vector<string> &args)
{
    const size_t cnt = args.size();
    if (cnt < 2 || args[0] == "--help" ||
            args[0].empty() || args[1].empty()) {
        cout << "Usage: cp <source path> <dst path>" << "\n";
        return -EINVAL;
    }

    const string& dst = args.back();
    KfsFileAttr dstAttr;
    int res = kfsClient->Stat(dst.c_str(), dstAttr, false);
    if (cnt > 2 && res == 0 && ! dstAttr.isDirectory) {
        res = -ENOTDIR;
    }
    if (cnt == 2 && res == -ENOENT) {
        dstAttr.isDirectory = false;
        res = 0;
    }
    if (res != 0) {
        cout << dst << ": " << ErrorCodeToStr(res) << "\n";
        return res;
    }
    for (size_t i = 0; i < cnt - 1; i++) {
        const string& src = args[i];
        KfsFileAttr srcAttr;
        int res = kfsClient->Stat(src.c_str(), srcAttr, false);
        if (res != 0) {
            cout << src << ": " << ErrorCodeToStr(res) << "\n";
            return res;
        }
        if ((res = srcAttr.isDirectory ?
                CopyDir(kfsClient, src, dst) :
                (dstAttr.isDirectory ?
                    CopyFileIntoDir(kfsClient, srcAttr, src, dst) :
                    CopyFile(kfsClient, srcAttr, src, dst)
                )) != 0) {
            return res;
        }
    }
    return 0;
}

}
}
