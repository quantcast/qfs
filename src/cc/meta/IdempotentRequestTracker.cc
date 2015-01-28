//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/01/27
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
// Class to keep track of idempotent requests.
//
//
//----------------------------------------------------------------------------

#include "IdempotentRequestTracker.h"
#include "MetaRequest.h"

#include "common/Properties.h"
#include "common/RequestParser.h"
#include "common/StdAllocator.h"
#include "common/LinearHash.h"

#include "kfsio/CryptoKeys.h"
#include "kfsio/ITimeout.h"
#include "kfsio/Globals.h"

namespace KFS
{

using libkfsio::globalNetManager;

class IdempotentRequestTracker::Impl : public ITimeout
{
public:
    Impl()
        : mExpirationTimeMicroSec((5 * 60) * 1000 * 1000),
          mAckId(0)
    {
        CryptoKeys::PseudoRand(&mAckId, sizeof(mAckId));
        if (mAckId < 0) {
            mAckId = -mAckId;
        }
        for (size_t i = 0; i < sizeof(mTables) / sizeof(mTables[0]); i++) {
            mTables[i] = 0;
        }
        globalNetManager().RegisterTimeoutHandler(this);
    };
    ~Impl()
    {
        for (size_t i = 0; i < sizeof(mTables) / sizeof(mTables[0]); i++) {
            delete mTables[i];
        }
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    void SetParameters(
        const Properties& inProps)
    {
    }
    bool Handle(
        MetaIdempotentRequest& inRequest)
    {
        if (inRequest.reqId < 0) {
            return false;
        }
        Entry theEntry(inRequest);
        bool  theInsertedFlag = false;
        Entry* theEntryPtr = Get(inRequest.op).Insert(
            theEntry, theEntry, theInsertedFlag);
        if (theInsertedFlag) {
            inRequest.SetReq(&inRequest);
            Lru::Insert(*theEntryPtr, mLru);
            inRequest.ackType = inRequest.op;
            inRequest.ackId   = inRequest.reqId ^ mAckId;
        } else {
            inRequest.SetReq(theEntryPtr->mReqPtr);
        }
        return true;
    }
    void Handle(
        MetaAck& inAck)
    {
        seq_t theAck                = -1;
        int   theAckType            = -1;
        const char*       thePtr    = inAck.ack.data();
        const char* const theEndPtr = thePtr + inAck.ack.size();
        if (inAck.shortRpcFormatFlag) {
            if (! HexIntParser::Parse(thePtr, theEndPtr - thePtr, theAck) ||
                    (*thePtr++ & 0xFF) != '/' ||
                    ! HexIntParser::Parse(
                        thePtr, theEndPtr - thePtr, theAckType)) {
                return;
            }
        } else {
            if (! DecIntParser::Parse(thePtr, theEndPtr - thePtr, theAck) ||
                    (*thePtr++ & 0xFF) != '/' ||
                    ! DecIntParser::Parse(
                        thePtr, theEndPtr - thePtr, theAckType)) {
                return;
            }
        }
        if (theAckType < 0 ||
                sizeof(mTables) / sizeof(mTables[0]) < (size_t)theAckType) {
            return;
        }
        Table* const theTablePtr = mTables[theAckType];
        if (! theTablePtr) {
            return;
        }
        SearchKey theKey(theAckType, theAck ^ mAckId, inAck);
        Entry     theEntry(theKey);
        theTablePtr->Erase(theEntry);
    }
    virtual void Timeout()
    {
        if (! Lru::IsInList(mLru)) {
            return;
        }
        int64_t const theExpirationTime =
            GetLastCallTimeMs() * 1000 - mExpirationTimeMicroSec;
        Entry* thePtr = &Lru::GetPrev(mLru);
        while (thePtr != &mLru &&
                thePtr->mReqPtr->submitTime < theExpirationTime) {
            mTables[thePtr->mReqPtr->op]->Erase(*thePtr);
            thePtr = &Lru::GetPrev(mLru);
        }
    }
private:
    class Entry
    {
    public:
        typedef QCDLListOp<Entry> Lru;
        Entry()
            : mReqPtr(0)
            { Lru::Init(*this); }
        Entry(
            MetaIdempotentRequest& inReq)
            : mReqPtr(&inReq)
            { Lru::Init(*this); }
        Entry(
            const Entry& inEntry)
            : mReqPtr(inEntry.mReqPtr)
            { Lru::Init(*this); }
        ~Entry()
            { Lru::Remove(*this); }
        bool operator==(
            const Entry& inRhs) const
        {
            return (
                mReqPtr->reqId == inRhs.mReqPtr->reqId &&
                (kKfsUserNone == mReqPtr->authUid ?
                    kKfsUserNone == inRhs.mReqPtr->authUid &&
                        mReqPtr->euser == inRhs.mReqPtr->euser :
                    mReqPtr->authUid == inRhs.mReqPtr->authUid)
            );
        }
        bool operator<(
            const Entry& inRhs) const
        {
            return (
                mReqPtr->reqId < inRhs.mReqPtr->reqId ||
                (mReqPtr->reqId == inRhs.mReqPtr->reqId &&
                (kKfsUserNone == mReqPtr->authUid ?
                    kKfsUserNone == inRhs.mReqPtr->authUid &&
                    mReqPtr->euser   < inRhs.mReqPtr->euser :
                    mReqPtr->authUid < inRhs.mReqPtr->authUid))
            );
        }
        MetaIdempotentRequest* const mReqPtr;
    private:
        Entry*                       mPrevPtr[1];
        Entry*                       mNextPtr[1];

        friend class QCDLListOp<Entry>;
    private:
        Entry& operator=(
            const Entry& inEntry);
    };
    class KeyHash
    {
    public:
        static size_t Hash(
            const Entry& inEntry)
            { return inEntry.mReqPtr->reqId; }
    };
    typedef KeyOnly<Entry> TableEntry;
    typedef Entry::Lru     Lru;
    typedef LinearHash<
        TableEntry,
        KeyCompare<Entry, KeyHash>,
        DynamicArray<SingleLinkedList<TableEntry>*, 10>,
        StdFastAllocator<TableEntry>,
        Impl
    > Table;

    struct SearchKey : public MetaIdempotentRequest
    {
        SearchKey(
                int            inOpType,
                seq_t          inReqId,
                const MetaAck& inAck)
            : MetaIdempotentRequest(
                MetaOp(inOpType), false)
        {
            reqId   = inReqId;
            authUid = inAck.authUid;
            euser   = inAck.euser;
        }
        virtual int log(
            ostream& /* inStream */) const
            { return 0; }
        virtual ostream& ShowSelf(
            ostream& inStream) const
            { return (inStream << "search: " << ackId); }
    };

    int64_t mExpirationTimeMicroSec;
    seq_t   mAckId;
    Entry   mLru;
    Table*  mTables[META_NUM_OPS_COUNT];
    
    Table& Get(
        MetaOp inOp)
    {
        Table*& thePtr = mTables[inOp];
        if (! thePtr) {
            thePtr = new Table;
            thePtr->SetDeleteObserver(this);
        }
        return *thePtr;
    }
public:
    // Delete observer.
    void operator()(
        TableEntry& inEntry)
        { inEntry.GetVal().mReqPtr->SetReq(0); }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

IdempotentRequestTracker::


IdempotentRequestTracker::IdempotentRequestTracker()
    : mImpl(*(new Impl()))
{
}

IdempotentRequestTracker::~IdempotentRequestTracker()
{
    delete &mImpl;
}

    void
IdempotentRequestTracker::SetParameters(
    const Properties& inProps)
{
    mImpl.SetParameters(inProps);
}

    bool
IdempotentRequestTracker::Handle(
    MetaIdempotentRequest& inRequest)
{
    return mImpl.Handle(inRequest);
}

    void
IdempotentRequestTracker::Handle(
    MetaAck& inAck)
{
    return mImpl.Handle(inAck);
}

} // namespace KFS
