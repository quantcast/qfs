//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/12/15
// Author: Mike Ovsiannikov
//
// Copyright 2015 Quantcast Corp.
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
// Transaction log and checkpoint storage.
//
//
//----------------------------------------------------------------------------

#ifndef KFS_META_DATA_READER_H
#define KFS_META_DATA_READER_H

#include "common/kfstypes.h"

namespace KFS
{

struct MetaReadMetaData;
class  Properties;
class  NetManager;
class  MetaVrLogSeq;

class MetaDataStore
{
public:
    MetaDataStore(
        NetManager& inNetManager);
    ~MetaDataStore();
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters);
    void Handle(
        MetaReadMetaData& inReadOp);
    void RegisterCheckpoint(
        const char*         inFileNamePtr,
        const MetaVrLogSeq& inStartSeq,
        seq_t               inLogSegmentNumber);
    void RegisterLogSegment(
        const char*         inFileNamePtr,
        const MetaVrLogSeq& inStartSeq,
        seq_t               inLogSegmentNumber);
    int Load(
        const char* inCheckpointDirPtr,
        const char* inLogDirPtr,
        bool        inRemoveTmpFilesFlag,
        bool        inIgnoreMissingSegmentsFlag);
    int Start();
    void Shutdown();
    void PrepareToFork();
    void ForkDone();
    void ChildAtFork();
    static bool GetLogSequenceFromFileName(
        const char*   inFileNamePtr,
        size_t        inFileNameLen,
        MetaVrLogSeq& outLogSeq,
        seq_t*        outSegNumPtr);
    static const char* GetCheckpointFileNamePrefixPtr()
        { return "chkpt."; }
    static const char* GetLogSegmentFileNamePrefixPtr()
        { return "log."; }
    static const char* GetCheckpointLatestFileNamePtr()
        { return "latest"; }
    static const char* GetLogSegmentLastFileNamePtr()
        { return "last"; }
private:
    class Impl;
    Impl& mImpl;
private:
    MetaDataStore(
        const MetaDataStore& inReader);
    MetaDataStore& operator=(
        const MetaDataStore& inReader);
};

} // namespace KFS

#endif /* KFS_META_DATA_READER_H */
