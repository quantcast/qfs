//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/06/09
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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
// Kerberos 5 service / client authentication test.
//
//----------------------------------------------------------------------------

#include "KrbService.h"
#include "KrbClient.h"

#include "qcdio/QCThread.h"

#include <string.h>
#include <stdlib.h>

#include <time.h>
#include <iostream>
#include <vector>
#include <string>

namespace
{
using KFS::KrbClient;
using KFS::KrbService;

using std::cerr;
using std::cout;
using std::ostream;
using std::string;

class QfsKrbTest
{
public:
    QfsKrbTest()
        : mClient(),
          mService(),
          mServiceClock(0),
          mClientClock()
        {}
    int Init(
        int         inArgsCount,
        char**      inArgsPtr,
        const char* inInMemoryKeyTabNamePtr)
    {
        const bool theDetectReplayFlag = inArgsCount >= 5 &&
            strchr(inArgsPtr[4], 'R');
        const char* theErrMsgPtr = mClient.Init(inArgsPtr[1], inArgsPtr[2]);
        if (theErrMsgPtr) {
            cerr <<
                "client: init error: " << theErrMsgPtr << "\n";
            return 1;
        }
        if ((theErrMsgPtr = mService.Init(
                inArgsPtr[1],
                inArgsPtr[2],
                inArgsCount <= 3 ? 0 : inArgsPtr[3],
                inInMemoryKeyTabNamePtr,
                theDetectReplayFlag))) {
            cerr <<
                "service init error: " << theErrMsgPtr << "\n";
            return 1;
        }
        return 0;
    }
    int Run(
        int    inArgsCount,
        char** inArgsPtr)
    {
        if (inArgsCount < 3 || 6 < inArgsCount) {
            cerr <<
                "Usage: " << (inArgsCount >= 1 ? inArgsPtr[0] : "") <<
                " <service host> <service name> [<keytab name>]"
                " [-d|-p|-r<num>] [<num>]\n"
            ;
            return 1;
        }
        const int theCnt = inArgsCount >= 6 ? atoi(inArgsPtr[5]) : 1;
        int       theRet = 0;
        for (int i = 0; i < theCnt; i++) {
            const int theErr = RunSelf(inArgsCount, inArgsPtr);
            if (theErr && ! theRet) {
                theRet = theErr;
            }
        }
        if (inArgsCount >= 5 && strchr(inArgsPtr[4], 'p')) {
            cerr << (void*)this << "\n" <<
                "client  cpu: " <<  (double)mClientClock / CLOCKS_PER_SEC <<
                    " ops/sec: " << (mClientClock != 0 ?
                        (double)theCnt * CLOCKS_PER_SEC /  mClientClock
                        : 0.0) <<
                "\n"
                "service cpu: " << (double)mServiceClock / CLOCKS_PER_SEC <<
                    " ops/sec: " << (mServiceClock != 0 ?
                        (double)theCnt * CLOCKS_PER_SEC /  mServiceClock
                        : 0.0) <<
            "\n";
        }
        return theRet;
    }
private:
    KrbClient  mClient;
    KrbService mService;
    clock_t    mServiceClock;
    clock_t    mClientClock;

    int RunSelf(
        int    inArgsCount,
        char** inArgsPtr)
    {
        const int theReplayCnt =
            (inArgsCount >= 5 && strncmp(inArgsPtr[4], "-r", 2) == 0) ?
            atoi(inArgsPtr[4] + 2) : 1;
        const bool theDebugFlag = inArgsCount >= 5 &&
            strchr(inArgsPtr[4], 'd');
        const char* theDataPtr   = 0;
        int         theDataLen   = 0;
        clock_t     theStart     = clock();
        const char* theCliKeyPtr = 0;
        int         theCliKeyLen = 0;
        const char* theErrMsgPtr;
        if ((theErrMsgPtr = mClient.Request(theDataPtr, theDataLen,
                theCliKeyPtr, theCliKeyLen))) {
            cerr <<
                "client: request create error: " << theErrMsgPtr << "\n";
            return 1;
        }
        mClientClock += clock() - theStart;
        if (theDebugFlag) {
            cout <<
                "client:"
                " request:"
                " length: " << theDataLen <<
                " key:"
                " length: " << theCliKeyLen <<
                " data: "
            ;
            ShowAsHexString(theCliKeyPtr, theCliKeyLen, cout) << "\n";
        }
        const char* theSrvKeyPtr  = 0;
        int         theSrvKeyLen  = 0;
        const char* theReqDataPtr = theDataPtr;
        int         theReqDataLen = theDataLen;
        theStart = clock();
        for (int i = 0; i < theReplayCnt; i++) {
            if ((theErrMsgPtr = mService.Request(
                    theReqDataPtr, theReqDataLen))) {
                cerr <<
                    "service: request process error: " << theErrMsgPtr << "\n";
                return 1;
            }
            if (theDebugFlag) {
                cout << "service: request: processed\n";
            }
            theDataPtr   = 0;
            theDataLen   = 0;
            theSrvKeyPtr = 0;
            theSrvKeyLen = 0;
            const char* thePrincipalPtr = 0;
            if ((theErrMsgPtr = mService.Reply(
                    KrbService::kPrincipalUnparseShort,
                    theDataPtr,
                    theDataLen,
                    theSrvKeyPtr,
                    theSrvKeyLen,
                    thePrincipalPtr))) {
                cerr <<
                    "service: reply create error: " << theErrMsgPtr << "\n";
                return 1;
            }
            if (theDebugFlag) {
                cout <<
                    "service:"
                    " reply:"
                    " length: " << theDataLen <<
                    " principal: " << thePrincipalPtr <<
                    " key:"
                    " length: " << theSrvKeyLen <<
                    " data: "
                ;
                ShowAsHexString(theSrvKeyPtr, theSrvKeyLen, cout) << "\n";
            }
        }
        const clock_t theEnd = clock();
        mServiceClock += theEnd - theStart;
        theStart = theEnd;
        if ((theErrMsgPtr = mClient.Reply(theDataPtr, theDataLen))) {
            cerr <<
                "client: reply: process error: " << theErrMsgPtr << "\n";
            return 1;
        }
        mClientClock += clock() - theStart;
        if (theDebugFlag) {
            cout <<
                "client:"
                " reply: processed"
            "\n";
        }
        if (theCliKeyLen != theSrvKeyLen ||
                memcmp(theCliKeyPtr, theSrvKeyPtr, theCliKeyLen) != 0) {
            cerr << (void*)this << " " <<
                "service / client key mismatch:" <<
                " key length:"
                " service: " << theSrvKeyLen <<
                " client: "  << theCliKeyLen <<
            "\n";
            return 1;
        }
        return 0;
    }

    ostream& ShowAsHexString(
        const char* inDataPtr,
        int         inLength,
        ostream&    inOutStream)
    {
        const char* const kHexSyms = "0123456789ABCDEF";
        for (const char* thePtr = inDataPtr,
                * const theEndPtr = inDataPtr + inLength;
                thePtr < theEndPtr;
                ++thePtr) {
            inOutStream <<
                kHexSyms[(*thePtr >> 4) & 0xF] <<
                kHexSyms[*thePtr & 0xF];
        }
        return inOutStream;
    }
private:
    QfsKrbTest(
        const QfsKrbTest& inTest);
    QfsKrbTest& operator=(
        const QfsKrbTest& inTest);
};

class KrbTestThread : public QCThread, private QfsKrbTest
{
public:
    KrbTestThread()
        : mArgsCount(0),
          mArgsPtr(0),
          mStatus(0)
        {}
    ~KrbTestThread()
        {}
    void Start(
        int         inArgsCount,
        char**      inArgsPtr,
        const char* inInMemoryKeyTabNamePtr)
    {
        mArgsCount = inArgsCount;
        mArgsPtr   = inArgsPtr;
        mStatus    = Init(mArgsCount, mArgsPtr, inInMemoryKeyTabNamePtr);
        // Test that the init works and doesn't leak memory.
        for (int i = 0; i < 5 && ! mStatus; i++) {
            mStatus = Init(mArgsCount, mArgsPtr, inInMemoryKeyTabNamePtr);
        }
        QCThread::Start();
    }
    virtual void Run()
    {
        if (mStatus) {
            return;
        }
        mStatus = QfsKrbTest::Run(mArgsCount, mArgsPtr);
    }
    int Stop()
    {
        QCThread::Join();
        return mStatus;
    }
private:
    int    mArgsCount;
    char** mArgsPtr;
    int    mStatus;
};

}

    int
main(
    int    inArgsCount,
    char** inArgsPtr)
{
    const char* thePtr       = 0;
    int         theThreadCnt = 0;
    string      theInMemoryKeyTabName("_qfs_keytab");
    if (inArgsCount >= 5 && (thePtr = strchr(inArgsPtr[4], 't')) &&
            (theThreadCnt = atoi(thePtr + 1)) > 1) {
        KrbTestThread* const theThreadsPtr = new KrbTestThread[theThreadCnt];
        for (int i = 0; i < theThreadCnt; i++) {
            theThreadsPtr[i].Start(inArgsCount, inArgsPtr,
                theInMemoryKeyTabName.c_str());
             // The key tab names should be unique, each tread.must have its
             // own.
            theInMemoryKeyTabName += "X";
        }
        int theRet = 0;
        for (int i = 0; i < theThreadCnt; i++) {
            const int theErr = theThreadsPtr[i].Stop();
            if (theErr) {
                theRet = theErr;
            }
        }
        delete [] theThreadsPtr;
        return theRet;
    } else {
        QfsKrbTest theTest;
        int        theStatus = theTest.Init(inArgsCount, inArgsPtr,
            theInMemoryKeyTabName.c_str());
        // Test that the init works and doesn't leak memory.
        for (int i = 0; i < 5 && ! theStatus; i++) {
            theStatus = theTest.Init(inArgsCount, inArgsPtr,
                theInMemoryKeyTabName.c_str());
        }
        return (theStatus ? theStatus : theTest.Run(inArgsCount, inArgsPtr));
    }
}
