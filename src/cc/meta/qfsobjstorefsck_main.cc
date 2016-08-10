//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/08/09
//
// Author: Mike Ovsiannikov
//
// Copyright 2009 Quantcast Corp.
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
// List files with object store blocks missing.
//
//----------------------------------------------------------------------------

#include "kfstree.h"
#include "Checkpoint.h"
#include "Restorer.h"
#include "Replay.h"
#include "util.h"

#include "common/MdStream.h"
#include "common/MsgLogger.h"
#include "common/RequestParser.h"

#include "kfsio/blockname.h"

#include "libclient/KfsClient.h"

#include <iostream>
#include <string>
#include <vector>

#include <boost/static_assert.hpp>
#include <boost/dynamic_bitset.hpp>

namespace KFS
{
using std::cout;
using std::cin;
using std::cerr;
using std::string;
using std::vector;
using boost::dynamic_bitset;

typedef dynamic_bitset<> BlocksBitmap;

    inline static int
RestoreCheckpoint(
    const string& inLockFileName)
{
    if (! inLockFileName.empty()) {
        acquire_lockfile(inLockFileName, 10);
    }
    Restorer theRestorer;
    return (theRestorer.rebuild(LASTCP) ? 0 : -EIO);
}

    inline static bool
HasBitmapSet(
    const MetaFattr& theFattr)
{
    const size_t kBits = 8 * sizeof(theFattr.subcount1);
    return ((int64_t)(kBits * CHUNKSIZE) <= theFattr.nextChunkOffset());
}

    inline static int
GetFileName(
    const MetaFattr& inFattr,
    string&          outFileName)
{
    outFileName = metatree.getPathname(&inFattr);
    return 0;
}

    inline static BlocksBitmap*
GetBitmapPtr(
    const MetaFattr& inFattr)
{
    BOOST_STATIC_ASSERT(sizeof(BlocksBitmap*) <= sizeof(inFattr.subcount1));
    char* const kNullPtr = 0;
    return reinterpret_cast<BlocksBitmap*>(kNullPtr + inFattr.chunkcount());
}

    inline static void
SetBitmapPtr(
    MetaFattr&    inFattr,
    BlocksBitmap* inPtr)
{
    const char* const kNullPtr = 0;
    inFattr.chunkcount() = reinterpret_cast<const char*>(inPtr) - kNullPtr;
}

    static int
ObjectStoreFsck(
    int    inArgCnt,
    char** inArgaPtr)
{
    int                 theOpt;
    string              theCpDir;
    string              theLockFile;
    ServerLocation      theMetaServer;
    const char*         theLogDirPtr         = 0;
    const char*         theConfigFileNamePtr = 0;
    MsgLogger::LogLevel theLogLevel          = MsgLogger::kLogLevelINFO;
    int                 theStatus            = 0;
    bool                theHelpFlag          = false;
    bool                theReplayLastLogFlag = false;
    const char*         thePtr;

    while ((theOpt = getopt(inArgCnt, inArgaPtr, "vhal:c:L:s:p:f:")) != -1) {
        switch (theOpt) {
            case 'a':
                theReplayLastLogFlag = true;
                break;
            case 'L':
                theLockFile = optarg;
                break;
            case 'l':
                theLogDirPtr = optarg;
                break;
            case 'c':
                theCpDir = optarg;
                break;
            case 's':
                theMetaServer.hostname = optarg;
                break;
            case 'p':
                thePtr = optarg;
                if (! DecIntParser::Parse(
                        thePtr, strlen(thePtr), theMetaServer.port)) {
                    theMetaServer.port = -1;
                }
                break;
            case 'v':
                theLogLevel = MsgLogger::kLogLevelDEBUG;
                break;
            case 'h':
                theHelpFlag = true;
                break;
            case 'f':
                theConfigFileNamePtr = optarg;
                break;
            default:
                theStatus = -EINVAL;
                break;
        }
    }
    if (theHelpFlag || 0 != theStatus ||
            (! theMetaServer.hostname.empty() && ! theMetaServer.IsValid())) {
        cerr <<
            "Usage: " << inArgaPtr[0] << "\n"
            "[-h <help>]\n"
            "[-v verbose]\n"
            "[-L <lock file>] default: no lock file\n"
            "[-l <transaction log directory>] default: kfslog\n"
            "[-c <checkpoint directory>] default: kfscp\n"
            "[-f <client configuration file>] default: none\n"
            "[-a replay last log segment] default: don't replay last segment\n"
            "[-s <meta server host>]\n"
            "[-p <meta server port>]\n"
            "\n"
            "Loads checkpoint, replays transaction logs, then"
            " reads object store block keys from standard in, one key per line,"
            " and outputs \"lost\" file names on standard out (files with keys"
            " that were not present in standard in), if any."
            "\n\n"
            "Note that the list of object store block keys must be"
            " more recent than checkpoint, and transaction logs, and valid"
            " meta server host and port must be specified in order for"
            " this work correctly (no false positives) if the file system is"
            " \"live\" / being modified."
            "\n\n"
            "In other words, the correct procedure to check \"live\" file system"
            " is to copy / save checkpoint, and transaction logs, then create"
            " list of object store blocks, then run this tool."
            "\n"
        ;
        return 1;
    }
    MdStream::Init();
    MsgLogger::Init(0, theLogLevel);

    if (! theCpDir.empty()) {
        checkpointer_setup_paths(theCpDir);
    }
    if (theLogDirPtr && theLogDirPtr[0]) {
        replayer.setLogDir(theLogDirPtr);
    }
    int64_t          theLostCount = 0;
    KfsClient* const theClientPtr =  theMetaServer.IsValid() ? 
        KfsClient::Connect(
            theMetaServer.hostname, theMetaServer.port, theConfigFileNamePtr)
        : 0;
    if (theClientPtr) {
        // Make sure that the client is configured correctly prior to checkpoint
        // load and replay, by retrieving root node info.
        const kfsChunkId_t     kChunkId   = -1;
        chunkOff_t             theOffset  = -1;
        int64_t                theVersion = 0;
        KfsFileAttr            theCliAttr;
        vector<ServerLocation> theServers;
        theStatus = theClientPtr->GetFileOrChunkInfo(
            ROOTFID,
            kChunkId,
            theCliAttr,
            theOffset,
            theVersion,
            theServers
        );
    } else if (theMetaServer.IsValid()) {
        theStatus = -ENOTCONN;
    }
    if (0 == theStatus &&
            (theStatus = RestoreCheckpoint(theLockFile)) == 0 &&
            (theStatus = replayer.playLogs(theReplayLastLogFlag)) == 0) {
        if (! theClientPtr) {
            // Setup back pointers, to get file names retrival working.
            metatree.setUpdatePathSpaceUsage(true);
            metatree.enableFidToPathname();
        }
        const int64_t theFileSystemId = metatree.GetFsId();
        string        theExpectedKey;
        string        theBlockKey;
        string        theFsIdSuffix;
        theExpectedKey.reserve(256);
        theBlockKey.reserve(256);
        while (getline(cin, theBlockKey)) {
            KFS_LOG_STREAM_DEBUG <<
                "key: " << theBlockKey <<
            KFS_LOG_EOM;
            const char*       thePtr    = theBlockKey.data();
            const char* const theEndPtr = thePtr + theBlockKey.size();
            if (theEndPtr <= thePtr) {
                continue;
            }
            const int kSeparator = '.';
            thePtr = reinterpret_cast<const char*>(
                memchr(thePtr, kSeparator, theEndPtr - thePtr));
            fid_t theFid     = -1;
            seq_t theVersion = 0;
            if (! thePtr ||
                    theEndPtr <= ++thePtr ||
                    ! DecIntParser::Parse(thePtr, theEndPtr - thePtr, theFid) ||
                    theFid < 0 ||
                    theEndPtr <= thePtr ||
                    kSeparator != (0xFF & *thePtr) ||
                    theEndPtr <= ++thePtr ||
                    ! DecIntParser::Parse(
                        thePtr, theEndPtr - thePtr, theVersion) ||
                    0 <= theVersion ||
                    theEndPtr <= thePtr ||
                    kSeparator != (0xFF & *thePtr)) {
                KFS_LOG_STREAM_ERROR <<
                    theBlockKey << ": malformed object store block key" <<
                KFS_LOG_EOM;
                continue;
            }
            theExpectedKey.clear();
            if (! AppendChunkFileNameOrObjectStoreBlockKey(
                    theExpectedKey,
                    theFileSystemId,
                    theFid,
                    theFid,
                    theVersion,
                    theFsIdSuffix)) {
                panic("block name generation failure");
                continue;
            }
            if (theExpectedKey != theBlockKey) {
                KFS_LOG_STREAM_ERROR <<
                    theBlockKey    << ": invalid object store block key"
                    " expected: "  << theExpectedKey <<
                KFS_LOG_EOM;
                continue;
            }
            MetaFattr* const theFattrPtr = metatree.getFattr(theFid);
            if (! theFattrPtr) {
                KFS_LOG_STREAM_DEBUG <<
                    theBlockKey << ": invalid key: no such file" <<
                KFS_LOG_EOM;
                continue;
            }
            if (KFS_FILE != theFattrPtr->type) {
                KFS_LOG_STREAM_ERROR <<
                    theBlockKey << ": invalid key:"
                    " attribute type: " << theFattrPtr->type <<
                KFS_LOG_EOM;
                continue;
            }
            if (0 != theFattrPtr->numReplicas) {
                KFS_LOG_STREAM_ERROR <<
                    theBlockKey << ": invalid key:"
                    " replication: " << theFattrPtr->numReplicas <<
                KFS_LOG_EOM;
                continue;
            }
            if (theFattrPtr->filesize <= 0) {
                KFS_LOG_STREAM_DEBUG <<
                    theBlockKey << ": skipping 0 size file" <<
                KFS_LOG_EOM;
                continue;
            }
            const chunkOff_t thePos = -theVersion - 1 - theFattrPtr->minSTier;
            if (thePos < 0 || 0 != (thePos % (chunkOff_t)CHUNKSIZE)) {
                KFS_LOG_STREAM_ERROR <<
                    theBlockKey << ": invalid key:"
                    " position: " << thePos <<
                    " tier: "     << theFattrPtr->minSTier <<
                    " / "         << theFattrPtr->maxSTier <<
                KFS_LOG_EOM;
                continue;
            }
            if (theFattrPtr->nextChunkOffset() < thePos) {
                KFS_LOG_STREAM(
                        theFattrPtr->nextChunkOffset() +
                            (chunkOff_t)CHUNKSIZE < thePos ?
                        MsgLogger::kLogLevelERROR :
                        MsgLogger::kLogLevelDEBUG) <<
                    theBlockKey << ": block past last file block"
                        " position: "   << thePos <<
                        " last block: " << theFattrPtr->nextChunkOffset()  <<
                KFS_LOG_EOM;
                continue;
            }
            // Chunk count must be 0 for object store files. Use this field to
            // store bitmap of the blocks that are present in the input. If the
            // file has more blocks that fits into the chunk count field, then
            // allocate bit vector and store pointer to it.
            const size_t theIdx = thePos / CHUNKSIZE;
            if (HasBitmapSet(*theFattrPtr)) {
                BlocksBitmap* thePtr = GetBitmapPtr(*theFattrPtr);
                if (! thePtr) {
                    thePtr = new BlocksBitmap(
                        1 + theFattrPtr->nextChunkOffset() / CHUNKSIZE);
                    SetBitmapPtr(*theFattrPtr, thePtr);
                } else if ((*thePtr)[theIdx]) {
                    KFS_LOG_STREAM_DEBUG <<
                        theBlockKey << ": duplicate input key" <<
                    KFS_LOG_EOM;
                    continue;
                }
                (*thePtr)[theIdx] = true;
            } else {
                const int64_t theBit = int64_t(1) << theIdx;
                if (0 != (theFattrPtr->chunkcount() & theBit)) {
                    KFS_LOG_STREAM_DEBUG <<
                        theBlockKey << ": duplicate input key" <<
                    KFS_LOG_EOM;
                    continue;
                }
                theFattrPtr->chunkcount() |= theBit;
            }
        }
        // Traverse leaf nodes and query the the status for files with missing
        // blocks.
        KfsFileAttr            theCliAttr;
        vector<ServerLocation> theServers;
        LeafIter theIt(metatree.firstLeaf(), 0);
        for (const Meta* theNPtr = 0;
                theIt.parent() && (theNPtr = theIt.current());
                theIt.next()) {
            if (KFS_FATTR != theNPtr->metaType()) {
                continue;
            }
            const MetaFattr& theFattr = *static_cast<const MetaFattr*>(theNPtr);
            if (KFS_FILE != theFattr.type || 0 != theFattr.numReplicas ||
                   theFattr.filesize <= 0) {
                continue;
            }
            chunkOff_t theMissingIdx = 0;
            if (HasBitmapSet(theFattr)) {
                const BlocksBitmap* const thePtr = GetBitmapPtr(theFattr);
                if (thePtr) {
                    for (theMissingIdx = 0;
                            theMissingIdx < (chunkOff_t)thePtr->size() &&
                                (*thePtr)[theMissingIdx];
                            ++theMissingIdx)
                        {}
                }
            } else {
                const int64_t theBits = theFattr.chunkcount();
                const int64_t theEnd  = theFattr.nextChunkOffset() / CHUNKSIZE;
                int64_t       theBit  = 1;
                for (theMissingIdx = 0;
                        theMissingIdx <= theEnd && 0 != (theBits & theBit);
                        theMissingIdx++, theBit <<= 1)
                    {}
            }
            if (theMissingIdx * (chunkOff_t)CHUNKSIZE < theFattr.filesize) {
                const kfsChunkId_t kChunkId   = -1;
                chunkOff_t         theOffset  = -1;
                int64_t            theVersion = 0;
                const int theRet = theClientPtr ?
                    theClientPtr->GetFileOrChunkInfo(
                        theFattr.id(),
                        kChunkId,
                        theCliAttr,
                        theOffset,
                        theVersion,
                        theServers
                    ) : GetFileName(theFattr, theCliAttr.filename);
                if (0 == theRet) {
                    cout << theCliAttr.filename << "\n";
                    theLostCount++;
                } else if (-ENOENT != theRet) {
                    KFS_LOG_STREAM_ERROR <<
                        "failed to get file info:"
                        " fid: "  << theFattr.id() <<
                        " "       << ErrorCodeToStr(theRet) <<
                    KFS_LOG_EOM;
                    if (0 == theStatus) {
                        theStatus = theRet;
                    }
                }
            }
        }
    }
    if (0 != theStatus) {
        KFS_LOG_STREAM_ERROR <<
            ErrorCodeToStr(theStatus) <<
        KFS_LOG_EOM;
    } else {
        KFS_LOG_STREAM_INFO <<
            "lost files: " << theLostCount <<
        KFS_LOG_EOM;
    }
    delete theClientPtr;
    MsgLogger::Stop();
    MdStream::Cleanup();
    return ((0 == theStatus && 0 == theLostCount) ? 0 : 1);
}

} // namespace KFS

int
main(int argc, char **argv)
{
    return KFS::ObjectStoreFsck(argc, argv);
}
