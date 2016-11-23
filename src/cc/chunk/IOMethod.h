//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/11
// Author: Mike Ovsiannikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unss required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// "Storage" IO abstract class.
//
//----------------------------------------------------------------------------

#ifndef KFS_CHUNK_IO_METHOD_H
#define KFS_CHUNK_IO_METHOD_H

#include "qcdio/QCDiskQueue.h"

namespace KFS
{

class Properties;

class IOMethod : public QCDiskQueue::RequestProcessor
{
public:
    static IOMethod* Create(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters);
    virtual ~IOMethod()
        {}
    virtual bool Init(
        QCDiskQueue& inQueue,
        int          inBlockSize,
        int64_t      inMinWriteBlkSize,
        int64_t      inMaxFileSize,
        bool&        outCanEnforceIoTimeoutFlag) = 0;
    virtual void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inParameters) = 0;
protected:
    IOMethod(
        bool inAllocatesReadBuffersFlag = false)
        : QCDiskQueue::RequestProcessor(inAllocatesReadBuffersFlag)
        {}
private:
    IOMethod(
        const IOMethod& inMethod);
    IOMethod& operator=(
        const IOMethod& inMethod);
};

} // namespace KFS

#endif /* KFS_CHUNK_IO_METHOD_H */
