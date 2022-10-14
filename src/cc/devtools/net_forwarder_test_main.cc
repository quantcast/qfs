//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/04/19
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
// Network forwarder / tcp proxy unit test.
//
//----------------------------------------------------------------------------

#include "common/Properties.h"
#include "common/MsgLogger.h"

#include "kfsio/NetManager.h"
#include "kfsio/NetForwarder.h"
#include "kfsio/IOBuffer.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCIoBufferPool.h"

#include <iostream>
#include <string>
#include <signal.h>

namespace KFS
{
using std::cerr;
using libkfsio::IOBufferAllocator;

class NetForwarderTest : private IOBufferAllocator
{
public:
    static void Stop(
        int /* inSignal */)
    {
        if (sInstancePtr) {
            sInstancePtr->mNetManager.Shutdown();
        }
    }
    NetForwarderTest()
        : IOBufferAllocator(),
          mBufferPool(),
          mParameters(),
          mNetManager(),
          mNetFowarder(mNetManager)
        {}
    ~NetForwarderTest()
        {}
    int Run(
        int    inArgCount,
        char** inArgsPtr)
    {
        if (inArgCount < 2) {
            cerr << "Usage:\n" <<
                (inArgCount < 1 ? "net_forwarder_test" : inArgsPtr[0]) << " \\\n"
                "    log.logLevel=DEBUG \\\n"
                "    fwd.listenOn=0.0.0.0\\ 2222 \\\n"
                "    fwd.connectTo=127.0.0.1\\ 80"
            "\n";
            return 1;
        }
        if (inArgCount == 2) {
            const char* const thePropsFileNamePtr = inArgsPtr[1];
            const int         theStatus           =
                mParameters.loadProperties(thePropsFileNamePtr, '=');
            if (theStatus != 0) {
                cerr << thePropsFileNamePtr << ": " <<
                    QCUtils::SysError(theStatus) << "\n";
                return theStatus;
            }
        } else {
            string theProps;
            for (int i = 1; i < inArgCount; i++) {
                theProps += inArgsPtr[i];
                theProps += "\n";
            }
            const int theStatus = mParameters.loadProperties(
                theProps.data(), theProps.size(), '=');
            if (theStatus != 0) {
                cerr << "failed to parse properties: " << theProps <<
                    "error: " << QCUtils::SysError(theStatus) << "\n";
                return theStatus;
            }
        }
        MsgLogger::Init(mParameters, "log.");
        if (MsgLogger::GetLogger()) {
            MsgLogger::GetLogger()->SetLogLevel(mParameters.getValue(
                "log.logLevel", MsgLogger::GetLogLevelNamePtr(
                    MsgLogger::GetLogger()->GetLogLevel()))
            );
        }
        int theStatus = mBufferPool.Create(
            mParameters.getValue("bufferPool.partitions",             1),
            mParameters.getValue("bufferPool.partitionBuffers", 4 << 10),
            mParameters.getValue("bufferPool.bufferSize",      16 << 10),
            mParameters.getValue("bufferPool.lockMemory",             0) != 0
        );
        if (theStatus != 0) {
            KFS_LOG_STREAM_ERROR <<
                "failed to create buffer pool: " <<
                    QCUtils::SysError(-theStatus) <<
            KFS_LOG_EOM;
            return theStatus;
        }
        if (! SetIOBufferAllocator(this)) {
            KFS_LOG_STREAM_ERROR <<
                "failed to set buffer allocator" <<
            KFS_LOG_EOM;
            return -1;
        }
        theStatus = mNetFowarder.Start("fwd.", mParameters);
        if (theStatus != 0) {
            return theStatus;
        }
        sInstancePtr = this;
        signal(SIGINT,  &NetForwarderTest::Stop);
        signal(SIGTERM, &NetForwarderTest::Stop);
        signal(SIGPIPE, SIG_IGN);
        mNetManager.MainLoop();
        sInstancePtr = 0;
        mNetFowarder.Shutdown();
        return 0;
    }
    virtual size_t GetBufferSize() const
        { return mBufferPool.GetBufferSize(); }
    virtual char* Allocate()
    {
        char* const theBufPtr = mBufferPool.Get();
        if (! theBufPtr) {
            QCUtils::FatalError("out of io buffers", 0);
        }
        return theBufPtr;
    }
    virtual void Deallocate(
        char* inBufferPtr)
        { mBufferPool.Put(inBufferPtr); }
private:
    QCIoBufferPool mBufferPool;
    Properties     mParameters;
    NetManager     mNetManager;
    NetForwarder   mNetFowarder;

    static NetForwarderTest* sInstancePtr;
};
NetForwarderTest* NetForwarderTest::sInstancePtr = 0;

}

int
main(
    int    inArgCount,
    char** inArgsPtr)
{
    KFS::NetForwarderTest theTest;
    return (theTest.Run(inArgCount, inArgsPtr) == 0 ? 0 : 1);
}
