//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/06/25
// Author: Mike Ovsiannikov
//
// Copyright 2010 Quantcast Corp.
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
//----------------------------------------------------------------------------

#ifndef FILE_OPENER_H
#define FILE_OPENER_H

#include "KfsNetClient.h"
#include "common/kfstypes.h"
#include "kfsio/checksum.h"

#include <stdint.h>
#include <ostream>

namespace KFS
{
namespace client
{

// File open / create state machine creates file and all intermediate
// directories in the specified path if needed.
class FileOpener
{
public:
    class Completion
    {
    public:
        virtual void Done(
            FileOpener& inOpener,
            int     inStatusCode) = 0;
        virtual void Unregistered(
            FileOpener& /* inOpener */)
            {}
    protected:
        Completion()
            {}
        Completion(
            const Completion&)
            {}
        virtual ~Completion()
            {}
    };
    struct Stats
    {
        typedef int64_t Counter;
        Stats()
            : mMetaOpsQueuedCount(0),
              mMetaOpsCancelledCount(0)
            {}
        void Clear()
            { *this = Stats(); }
        Stats& Add(
            const Stats& inStats)
        {
            mMetaOpsQueuedCount    += inStats.mMetaOpsQueuedCount;
            mMetaOpsCancelledCount += inStats.mMetaOpsCancelledCount;
            return *this;
        }
        std::ostream& Display(
            std::ostream& inStream,
            const char*   inSeparatorPtr = 0,
            const char*   inDelimiterPtr = 0) const
        {
            const char* const theSeparatorPtr =
                inSeparatorPtr ? inSeparatorPtr : " ";
            const char* const theDelimiterPtr =
                inDelimiterPtr ? inDelimiterPtr : ": ";
            inStream <<
                "MetaOpsQueued"            << theDelimiterPtr <<
                    mMetaOpsQueuedCount    << theSeparatorPtr <<
                "MetaOpsCancelled"         << theDelimiterPtr <<
                    mMetaOpsCancelledCount
            ;
            return inStream;
        }
        Counter mMetaOpsQueuedCount;
        Counter mMetaOpsCancelledCount;
    };
    typedef KfsNetClient MetaServer;
    FileOpener(
        MetaServer& inMetaServer,
        Completion* inCompletionPtr = 0,
        const char* inLogPrefixPtr  = 0);
    virtual ~FileOpener();
    int Open(
        const char* inFileNamePtr,
        int         inNumReplicas  = 3,
        bool        inMakeDirsFlag = false);
    int Open(
        kfsFileId_t inFileId,
        const char* inFileNamePtr);
    void Shutdown();
    bool IsOpen()    const;
    bool IsOpening() const;
    bool IsActive()  const;
    int GetErrorCode() const;
    void Register(
        Completion* inCompletionPtr);
    bool Unregister(
        Completion* inCompletionPtr);
    void GetStats(
        Stats& outStats);
private:
    class Impl;
    Impl& mImpl;
private:
    FileOpener(
        const FileOpener& inFileOpener);
    FileOpener& operator=(
        const FileOpener& inFileOpener);
};
}}

#endif /* FILE_OPENER_H */
