//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/10/03
// Author: Mike Ovsiannikov
//
// Copyright 2009-2011 Quantcast Corp.
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
// Network error simulator implementation.
//
//----------------------------------------------------------------------------

#include "NetErrorSimulator.h"

#ifdef KFS_DONT_USE_BOOST_REGEX_LIB
// Paper over missing boost regex lib

#include <iostream>

namespace KFS
{
bool
NetErrorSimulatorConfigure(
    NetManager& /* inNetManager */,
    const char* inConfigPtr)
{
    if (inConfigPtr && *inConfigPtr) {
        std::cerr <<
            "NetErrorSimulatorConfigure is not supported" <<
        std::endl;
    }
    return true;
}

}

#else

#include "common/MsgLogger.h"
#include "common/StdAllocator.h"
#include "common/Properties.h"
#include "qcdio/QCFdPoll.h"
#include "NetManager.h"
#include "Globals.h"

#include <inttypes.h>
#include <time.h>
#include <errno.h>

#include <vector>
#include <map>
#include <string>
#include <sstream>
#include <boost/regex.hpp>
#include <boost/random/mersenne_twister.hpp>

namespace KFS
{

class NetErrorSimulator : public NetManager::PollEventHook
{
public:
    NetErrorSimulator(
        NetManager& inNetManager)
        : NetManager::PollEventHook(),
          mSpecs(),
          mConnMap(),
          mRandom(/* seed */),
          mRandMax(mRandom.max()),
          mNetManager(inNetManager)
        {}
    virtual ~NetErrorSimulator()
    {
        PollEventHook* const theHookPtr = mNetManager.SetPollEventHook();
        if (theHookPtr && theHookPtr != this) {
            mNetManager.SetPollEventHook(theHookPtr);
        }
    }
    static bool Set(
        NetManager& inNetManager,
        std::string inSpecs)
    {
        NetErrorSimulator* theSimPtr = inSpecs.empty() ?
            0 : new NetErrorSimulator(inNetManager);
        if (theSimPtr && ! theSimPtr->SetParameters(inSpecs)) {
            delete theSimPtr;
            theSimPtr = 0;
        }
        PollEventHook* const thePrevHookPtr =
            inNetManager.SetPollEventHook(theSimPtr);
        if (! thePrevHookPtr) {
            return true;
        }
        NetErrorSimulator* const thePrevSimPtr =
            dynamic_cast<NetErrorSimulator*>(thePrevHookPtr);
        if (thePrevSimPtr) {
            delete thePrevSimPtr;
            return true;
        }
        inNetManager.SetPollEventHook(thePrevHookPtr);
        delete theSimPtr;
        return false;
    }
    bool SetParameters(
        std::string inSpecs)
    {
        mSpecs.clear();
        mConnMap.clear();
        // sn=^[^:]*:30000$,pn=^[^:]*:20000$,a=rn+rd+wd+err+rst+rand+log,int=1;
        for (size_t theNextPos = 0; ;) {
            const size_t theEndPos = inSpecs.find(';', theNextPos);
            std::string theSpec = inSpecs.substr(
                theNextPos,
                theEndPos == std::string::npos ?
                    theEndPos : theEndPos - theNextPos
            );
            for (size_t thePos = 0;
                    (thePos = theSpec.find(',', thePos)) !=
                    std::string::npos; ) {
                theSpec[thePos] = '\n';
            }
            std::istringstream theInStream(theSpec);
            Properties         theProps;
            theProps.loadProperties(theInStream, '=', false);
            int theActionFlags = 0;
            std::string theActions = theProps.getValue("a", "");
            if (theActions.find("rn") != std::string::npos) {
                theActionFlags |= SimSpec::kActionDisableRead;
            }
            if (theActions.find("rd") != std::string::npos) {
                theActionFlags |= SimSpec::kActionDiscardRead;
            }
            if (theActions.find("wd") != std::string::npos) {
                theActionFlags |= SimSpec::kActionDiscardWrite;
            }
            if (theActions.find("err") != std::string::npos) {
                theActionFlags |= SimSpec::kActionSetError;
            }
            if (theActions.find("rst") != std::string::npos) {
                theActionFlags |= SimSpec::kActionClose;
            }
            if (theActions.find("rand") != std::string::npos) {
                theActionFlags |= SimSpec::kActionRandomInterval;
            }
            if (theActions.find("log") != std::string::npos) {
                theActionFlags |= SimSpec::kActionLog;
            }
            if (theActions.find("exit") != std::string::npos) {
                theActionFlags |= SimSpec::kActionExit;
            }
            if (theActions.find("abort") != std::string::npos) {
                theActionFlags |= SimSpec::kActionAbort;
            }
            if (theActions.find("erd") != std::string::npos) {
                theActionFlags |= SimSpec::kActionSetErrorOnRead;
            }
            if (theActions.find("ewr") != std::string::npos) {
                theActionFlags |= SimSpec::kActionSetErrorOnWrite;
            }
            double theSleepSec = theProps.getValue("sleep", (double)-1);
            if (theSleepSec > 0) {
                theActionFlags |= SimSpec::kActionSleep;
            } else {
                if ((theSleepSec =
                        theProps.getValue("rsleep", (double)-1)) > 0) {
                    theActionFlags |= SimSpec::kActionRandomSleep;
               }
            }
            if (theActionFlags != 0) {
                mSpecs.push_back(SimSpec(
                    theProps.getValue("sn", ""),
                    theProps.getValue("pn", ""),
                    theActionFlags,
                    (uint32_t)theProps.getValue("int",   (uint64_t)0),
                    (float)theSleepSec
                ));
            }
            if (theEndPos == std::string::npos) {
                break;
            }
            theNextPos = theEndPos + 1;
        }
        return (! mSpecs.empty());
    }
    virtual void Remove(
        NetManager&    inMgr,
        NetConnection& inConn)
        { mConnMap.erase(&inConn); }
    virtual void Event(
        NetManager&    inMgr,
        NetConnection& inConn,
        int&           ioPollEvent)
    {
        if (! inConn.IsGood()) {
            return;
        }
        const NetConnection* const theConnPtr = &inConn;
        std::pair<ConnMap::iterator, ConnMap::iterator>
            theRange = mConnMap.equal_range(theConnPtr);
        if (theRange.first == theRange.second) {
            bool              theInsertedFlag = false;
            const std::string theSockName     = inConn.GetSockName();
            const std::string thePeerName     = inConn.GetPeerName();
            for (SimSpecs::const_iterator theIt = mSpecs.begin();
                    theIt != mSpecs.end();
                    ++theIt) {
                const SimSpec& theSpec = *theIt;
                if ((theSpec.mSockNameRegex.empty() || regex_match(
                        theSockName,
                        theSpec.mSockNameRegex)) &&
                    (theSpec.mPeerNameRegex.empty() || regex_match(
                        thePeerName,
                        theSpec.mPeerNameRegex))) {
                    mConnMap.insert(std::make_pair(theConnPtr, ConnEntry(
                        theIt,
                        theSockName + "/" + thePeerName,
                        GetCount(*theIt)
                    )));
                    theInsertedFlag = true;
                }
            }
            if (! theInsertedFlag) {
                mConnMap.insert(std::make_pair(theConnPtr, mSpecs.end()));
            }
            theRange = mConnMap.equal_range(theConnPtr);
        }
        for ( ; theRange.first != theRange.second; ++theRange.first) {
            if (theRange.first->second.mSpecIt == mSpecs.end()) {
                continue;
            }
            ConnEntry& theEntry = theRange.first->second;
            if (theEntry.mCount > 0) {
                theEntry.mCount--;
                continue;
            }
            const SimSpec& theSpec = *theEntry.mSpecIt;
            if (theSpec.mActionFlags == SimSpec::kActionNone) {
                continue;
            }
            theEntry.mCount = GetCount(theSpec);
            const int theOrigPollEvent = ioPollEvent;
            std::string theActions;
            if ((theSpec.mActionFlags & SimSpec::kActionDisableRead) != 0) {
                ioPollEvent &= ~int(
                    QCFdPoll::kOpTypeIn  |
                    QCFdPoll::kOpTypePri |
                    QCFdPoll::kOpTypeHup
                );
                inConn.SetMaxReadAhead(0);
                ListAdd(theActions, "rn");
            }
            int theRdDiscarded = 0;
            if ((theSpec.mActionFlags & SimSpec::kActionDiscardRead) != 0) {
                theRdDiscarded = inConn.GetNumBytesToRead();
                inConn.DiscardRead();
                ListAdd(theActions, "rd");
            }
            int theWrDiscarded = 0;
            if ((theSpec.mActionFlags &
                    SimSpec::kActionDiscardWrite) != 0) {
                ioPollEvent &= ~int(QCFdPoll::kOpTypeOut);
                theWrDiscarded = inConn.GetNumBytesToWrite();
                inConn.DiscardWrite();
                ListAdd(theActions, "wd");
            }
            if ((theSpec.mActionFlags & SimSpec::kActionSetError) != 0) {
                ioPollEvent = int(QCFdPoll::kOpTypeError);
                ListAdd(theActions, "err");
            }
            if ((theSpec.mActionFlags & SimSpec::kActionSetErrorOnRead) != 0 &&
                    (ioPollEvent & QCFdPoll::kOpTypeIn) != 0) {
                ioPollEvent = int(QCFdPoll::kOpTypeError);
                ListAdd(theActions, "erd");
            }
            if ((theSpec.mActionFlags & SimSpec::kActionSetErrorOnWrite) != 0 &&
                    (ioPollEvent & QCFdPoll::kOpTypeOut) != 0) {
                ioPollEvent = int(QCFdPoll::kOpTypeError);
                ListAdd(theActions, "ewr");
            }
            if ((theSpec.mActionFlags & SimSpec::kActionClose) != 0) {
                inConn.Close();
                ioPollEvent = 0;
                ListAdd(theActions, "rst");
            }
            if ((theSpec.mActionFlags & SimSpec::kActionLog) != 0) {
                ListAdd(theActions, "log");
                KFS_LOG_STREAM_DEBUG << theEntry.mConnId <<
                    " " << theActions <<
                    " poll: " << DisplayPollFlags(theOrigPollEvent) <<
                    " -> "    << DisplayPollFlags(ioPollEvent) <<
                    " discarded:"
                    " rd: "   << theRdDiscarded <<
                    " wr: "   << theWrDiscarded <<
                KFS_LOG_EOM;
            }
            if ((theSpec.mActionFlags & SimSpec::kActionSleep) != 0) {
                Sleep(theSpec.mSleepSec);
            }
            if ((theSpec.mActionFlags & SimSpec::kActionRandomSleep) != 0) {
                RandomSleep(theSpec.mSleepSec);
            }
            if ((theSpec.mActionFlags & SimSpec::kActionAbort) != 0) {
                abort();
            }
            if ((theSpec.mActionFlags & SimSpec::kActionExit) != 0) {
                _exit(1);
            }
        }
    }
private:
    typedef boost::regex Regex;
    struct SimSpec
    {
        enum
        {
            kActionNone            = 0,
            kActionDisableRead     = 1,
            kActionDiscardRead     = 1 << 1,
            kActionDiscardWrite    = 1 << 2,
            kActionSetError        = 1 << 3,
            kActionClose           = 1 << 4,
            kActionRandomInterval  = 1 << 5,
            kActionLog             = 1 << 6,
            kActionSleep           = 1 << 7,
            kActionRandomSleep     = 1 << 8,
            kActionExit            = 1 << 9,
            kActionAbort           = 1 << 10,
            kActionSetErrorOnRead  = 1 << 11,
            kActionSetErrorOnWrite = 1 << 12
        };
        SimSpec()
            : mSockNameRegex(),
              mPeerNameRegex(),
              mActionFlags(kActionNone),
              mInterval(0),
              mSleepSec(0)
            {}
        SimSpec(
            const std::string& inSockNameRegexStr,
            const std::string& inPeerNameRegexStr,
            int                inActionFlags,
            uint32_t           inInterval,
            float              inSleepSec)
            : mSockNameRegex(inSockNameRegexStr,
                Regex::perl + Regex::icase + Regex::no_except),
              mPeerNameRegex(inPeerNameRegexStr,
                Regex::perl + Regex::icase + Regex::no_except),
              mActionFlags(inActionFlags),
              mInterval(inInterval),
              mSleepSec(inSleepSec)
            {}
        Regex    mSockNameRegex;
        Regex    mPeerNameRegex;
        int      mActionFlags;
        uint32_t mInterval;
        float    mSleepSec;
    };
    typedef std::vector<SimSpec> SimSpecs;
    typedef boost::mt19937       Random;
    struct ConnEntry
    {
        ConnEntry(
            SimSpecs::const_iterator inSpecIt = SimSpecs::const_iterator(),
            std::string              inConnId = std::string(),
            Random::result_type      inCount  = 0)
            : mSpecIt(inSpecIt),
              mConnId(inConnId),
              mCount(inCount)
            {}
        SimSpecs::const_iterator mSpecIt;
        std::string              mConnId;
        Random::result_type      mCount;
    };
    typedef std::multimap<
        const NetConnection*,
        ConnEntry,
        std::less<const NetConnection*>,
        StdFastAllocator<
            std::pair<const NetConnection* const, ConnEntry>
        >
    > ConnMap;

    SimSpecs                  mSpecs;
    ConnMap                   mConnMap;
    Random                    mRandom;
    const Random::result_type mRandMax;
    NetManager&               mNetManager;

    static void ListAdd(
        std::string& inList,
        const char*  inElemPtr,
        const char*  inDelimPtr = "+")
    {
        if (! inList.empty()) {
            inList += inDelimPtr;
        }
        inList += inElemPtr;
    }
    static std::string DisplayPollFlags(
        int inFlags)
    {
        std::string theRet;
        if ((inFlags & QCFdPoll::kOpTypeIn) != 0) {
            ListAdd(theRet, "in");
        }
        if ((inFlags & QCFdPoll::kOpTypeOut) != 0) {
            ListAdd(theRet, "out");
        }
        if ((inFlags & QCFdPoll::kOpTypePri) != 0) {
            ListAdd(theRet, "pri");
        }
        if ((inFlags & QCFdPoll::kOpTypeHup) != 0) {
            ListAdd(theRet, "hup");
        }
        if ((inFlags & QCFdPoll::kOpTypeError) != 0) {
            ListAdd(theRet, "err");
        }
        return theRet;
    }
    Random::result_type GetCount(
        const SimSpec& inSpec)
    {
        // Don't use modulo, low order bits might be "less random".
        // Though this shouldn't be a problem with Mersenne twister.
        const uint64_t theInterval = inSpec.mInterval;
        return Random::result_type(
            ((inSpec.mActionFlags ==
                SimSpec::kActionRandomInterval) != 0 &&
            theInterval > 0) ?
                (uint64_t)mRandom() * theInterval / mRandMax  :
                theInterval
        );
    }
    void Sleep(
        float inSec)
    {
        if (inSec <= 0) {
            return;
        }
        struct timespec theTs;
        theTs.tv_sec  = time_t(inSec);
        long kMaxNsec = 999999999;
        theTs.tv_nsec = std::min(kMaxNsec, long((inSec - theTs.tv_sec) * 1e9));
        while (
                (theTs.tv_sec > 0 || theTs.tv_nsec > 0) &&
                nanosleep(&theTs, &theTs) != 0 &&
                errno == EINTR)
            {}
    }
    void RandomSleep(
        float inSec)
        { Sleep(mRandom() * inSec / mRandMax); }

private:
    NetErrorSimulator(
        const NetErrorSimulator&);
    NetErrorSimulator& operator=(
        const NetErrorSimulator&);
};

bool
NetErrorSimulatorConfigure(
    NetManager& inNetManager,
    const char* inConfigPtr)
{
    return NetErrorSimulator::Set(inNetManager, inConfigPtr ? inConfigPtr : "");
}

} /* namespace KFS */
#endif
