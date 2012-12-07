//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/09
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
//
// \file KfsAttr.h
// \brief Kfs i-node and chunk attribute classes.
//
//----------------------------------------------------------------------------

#ifndef LIBKFSCLIENT_KFSATTR_H
#define LIBKFSCLIENT_KFSATTR_H

#include <sys/time.h>

#include <string>
#include <vector>

#include "common/kfstypes.h"
#include "common/kfsdecls.h"

struct stat;

namespace KFS
{
namespace client
{
using std::string;
using std::vector;

///
/// \brief Server attributes + chunk attributes
///
struct FileAttr : public Permissions
{
    kfsFileId_t     fileId;      /// i-node number
    struct timeval  mtime;       /// modification time
    struct timeval  ctime;       /// attribute change time
    struct timeval  crtime;      /// creation time
    bool            isDirectory; /// is this a directory?
    chunkOff_t      fileSize;    /// logical eof
    int64_t         subCount1;   /// number of chunks in the file or files in directory
    int64_t         subCount2;   /// directories count
    int16_t         numReplicas;
    int16_t         numStripes;
    int16_t         numRecoveryStripes;
    StripedFileType striperType;
    int32_t         stripeSize;

    FileAttr()
        : Permissions(),
          fileId(-1),
          mtime(),
          ctime(),
          crtime(),
          isDirectory(false),
          fileSize(-1),
          subCount1(0),
          subCount2(-1),
          numReplicas(0),
          numStripes(0),
          numRecoveryStripes(0),
          striperType(KFS_STRIPED_FILE_TYPE_NONE),
          stripeSize(0)
        {}
    void Reset()
        { *this = FileAttr(); }
    void Init(bool isDir)
    {
        isDirectory = isDir;
        gettimeofday(&mtime, 0);
        ctime  = mtime;
        crtime = mtime;
    }
    int64_t chunkCount() const
        { return (isDirectory ? 0 : subCount1); }
    int64_t fileCount() const
        { return (isDirectory ? subCount1 : int64_t(0)); }
    int64_t dirCount() const
        { return (isDirectory ? subCount2 : int64_t(0)); }
    void ToStat(struct stat& outStat) const;
};

struct ChunkAttr
{
    vector<ServerLocation> chunkServerLoc; // servers hosting chunk replicas
    kfsChunkId_t           chunkId;
    int64_t                chunkVersion;
    chunkOff_t             chunkSize;
    chunkOff_t             chunkOffset;    // start position in the file

    ChunkAttr()
        : chunkServerLoc(),
          chunkId(-1),
          chunkVersion(-1),
          chunkSize(0),
          chunkOffset(-1)
        {}
};

} // namespace client

///
/// \brief File attributes as usable by applications.
///
///
struct KfsFileAttr : public client::FileAttr
{
    /// the name of this file
    string filename;

    KfsFileAttr()
        : client::FileAttr(),
          filename()
        {}
    void Clear()
    {
        Reset();
        filename.clear();
    }
    KfsFileAttr& operator= (const FileAttr &other)
    {
        FileAttr::operator=(other);
        return *this;
    }
    bool operator < (const KfsFileAttr & other) const
    {
        return filename < other.filename;
    }
};
} // namespace KFS

#endif // LIBKFSCLIENT_KFSATTR_H
