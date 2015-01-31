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
#include "common/time.h"

#include "kfsio/CryptoKeys.h"
#include "kfsio/ITimeout.h"
#include "kfsio/Globals.h"

#include "qcdio/qcdebug.h"

#include <ostream>
#include <istream>

namespace KFS
{

using std::istream;
using std::ostream;
using libkfsio::globalNetManager;

class IdempotentRequestTracker::Impl : public ITimeout
{
public:
    Impl()
        : mExpirationTimeMicroSec((6 * 60) * 1000 * 1000),
          mSize(0),
          mMaxSize(64 << 10),
          mLru()
    {
        for (size_t i = 0; i < sizeof(mTables) / sizeof(mTables[0]); i++) {
            mTables[i] = 0;
        }
        globalNetManager().RegisterTimeoutHandler(this);
    };
    ~Impl()
    {
        Impl::Clear();
        globalNetManager().UnRegisterTimeoutHandler(this);
    }
    void Clear()
    {
        for (size_t i = 0; i < sizeof(mTables) / sizeof(mTables[0]); i++) {
            delete mTables[i];
            mTables[i] = 0;
        }
        QCASSERT(mSize == 0 && ! Lru::IsInList(mLru));
    }
    void SetParameters(
        const char*       inParamNamePrefixPtr,
        const Properties& inParameters)
    {
        Properties::String theParamName;
        if (inParamNamePrefixPtr) {
            theParamName.Append(inParamNamePrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        mMaxSize = (size_t)inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append("maxSize"),
            (double)mMaxSize);
        mExpirationTimeMicroSec = (int64_t)(inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append("timeout"),
            (double)mExpirationTimeMicroSec * 1e-6) * 1e6);
        Expire(microseconds());
    }
    bool Handle(
        MetaIdempotentRequest& inRequest)
    {
        if (inRequest.reqId < 0 || mMaxSize <= 0) {
            return false;
        }
        if (! Validate(inRequest.op)) {
            panic("IdempotentRequestTracker: invalid request type");
            return false;
        }
        Entry  theEntry(inRequest);
        bool   theInsertedFlag = false;
        Table& theTable        = Get(inRequest.op);
        Entry* theEntryPtr     = theTable.Insert(
            theEntry, theEntry, theInsertedFlag);
        inRequest.SetReq(theEntryPtr->mReqPtr);
        if (theInsertedFlag) {
            Lru::Insert(*theEntryPtr, mLru);
            mSize++;
            if (mMaxSize < mSize) {
                Expire(microseconds());
                if (mMaxSize < mSize) {
                    theTable.Erase(*theEntryPtr);
                    inRequest.status    = -ESERVERBUSY;
                    inRequest.statusMsg = "out of idempotent request entries"; 
                    return true;
                }
            }
            inRequest.ackId = inRequest.reqId;
            return false;
        }
        if (! theEntryPtr->mReqPtr ||
                theEntryPtr->mReqPtr->suspended) {
            // Presently "resume" is not supported.
            panic("IdempotentRequestTracker: invalid idempotent request entry");
            return false;
        }
        return true;
    }
    void Handle(
        MetaAck& inAck)
    {
        inAck.status = HandleAck(inAck.ack.data(), inAck.ack.size(),
                inAck.euser, inAck.authUid) ? 0 :
            -EINVAL; // Don't log invalid or duplicate ack.
    }
    bool HandleAck(
        const char* inPtr,
        size_t      inLen,
        kfsUid_t    inUid,
        kfsUid_t    inAuthUid)
    {
        const char*       thePtr    = inPtr;
        const char* const theEndPtr = thePtr + inLen;
        seq_t             theAck    = -1;
        if (! HexIntParser::Parse(thePtr, theEndPtr - thePtr, theAck)) {
            return false;
        }
        if (theAck < 0 || theEndPtr <= thePtr || (*thePtr & 0xFF) != '_') {
            return false;
        }
        if (theEndPtr <= ++thePtr) {
            return false;
        }
        const int theAckType = MetaRequest::GetId(
            TokenValue(thePtr, theEndPtr - thePtr));
        if (theAckType < 0 || ! Validate(theAckType)) {
            return false;
        }
        Table* const theTablePtr = mTables[theAckType];
        if (! theTablePtr) {
            return false;
        }
        SearchKey theKey(theAckType, theAck, inUid, inAuthUid);
        Entry     theEntry(theKey);
        return (0 < theTablePtr->Erase(theEntry));
    }
    virtual void Timeout()
    {
        if (mSize <= 0) {
            return;
        }
        Expire(GetLastCallTimeMs() * 1000);
    }
    int Write(
        ostream& inStream) const
    {
        const Entry* thePtr = &mLru;
        while (&mLru != (thePtr = &Lru::GetPrev(*thePtr)) && inStream) {
            thePtr->mReqPtr->WriteLog(inStream);
        }
        return (inStream ? 0 : -EIO);
    }
    int Read(
        const char* inPtr,
        size_t      inLen)
    {
        if (mMaxSize <= 0) {
            return 0;
        }
        MetaRequest* const theReqPtr = MetaRequest::Read(inPtr, inLen);
        if (! theReqPtr) {
            return -EINVAL;
        }
        // This is used for checkpoint load -- assign the "start" time.
        static const int sStartTime = microseconds();
        theReqPtr->submitTime  = sStartTime;
        theReqPtr->processTime = theReqPtr->submitTime;
        // Keep the most recent requests.
        Entry* thePtr;
        while (mMaxSize <= mSize && &mLru != (thePtr = &Lru::GetPrev(mLru))) {
            mTables[thePtr->mReqPtr->op]->Erase(*thePtr);
        }
        const bool theHandledFlag = Handle(
            *static_cast<MetaIdempotentRequest*>(theReqPtr));
        MetaRequest::Release(theReqPtr);
        return (theHandledFlag ? -EINVAL : 0);
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
            { QCASSERT(! Lru::IsInList(*this)); }
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
                int      inOpType,
                seq_t    inReqId,
                kfsUid_t inUid,
                kfsUid_t inAuthUid)
            : MetaIdempotentRequest(
                MetaOp(inOpType), false)
        {
            reqId   = inReqId;
            authUid = inAuthUid;
            euser   = inUid;
        }
        virtual int log(
            ostream& /* inStream */) const
            { return 0; }
        virtual ostream& ShowSelf(
            ostream& inStream) const
            { return (inStream << "search: " << ackId); }
    };

    int64_t mExpirationTimeMicroSec;
    size_t  mSize;
    size_t  mMaxSize;
    Entry   mLru;
    Table*  mTables[META_NUM_OPS_COUNT];

    bool Validate(
        int inOp) const
    {
        return (0 <= inOp &&
                (size_t)inOp < sizeof(mTables) / sizeof(mTables[0]));
    }
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
    void Expire(
        int64_t inNow)
    {
        int64_t const theExpirationTime = inNow - mExpirationTimeMicroSec;
        Entry* thePtr;
        while (&mLru != (thePtr = &Lru::GetPrev(mLru)) &&
                thePtr->mReqPtr->submitTime < theExpirationTime) {
            mTables[thePtr->mReqPtr->op]->Erase(*thePtr);
        }
    }
public:
    // Delete observer.
    void operator()(
        TableEntry& inEntry)
    {
        Entry& theEntry = inEntry.GetVal();
        if (mSize <= 0 ||
                ! theEntry.mReqPtr ||
                ! Lru::IsInList(theEntry) ||
                ! Validate(theEntry.mReqPtr->op)) {
            panic("IdempotentRequestTracker: invalid idempotent request erase");
        }
        theEntry.mReqPtr->SetReq(0);
        Lru::Remove(theEntry);
        mSize--;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

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
    const char*       inParamNamePrefixPtr,
    const Properties& inParameters)
{
    mImpl.SetParameters(inParamNamePrefixPtr, inParameters);
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

    bool
IdempotentRequestTracker::HandleAck(
    const char* inPtr,
    size_t      inLen,
    kfsUid_t    inUid,
    kfsUid_t    inAuthUid)
{
    return mImpl.HandleAck(inPtr, inLen, inUid, inAuthUid);
}

    int
IdempotentRequestTracker::Write(
    ostream& inStream) const
{
    return mImpl.Write(inStream);
}

    int
IdempotentRequestTracker::Read(
    const char* inPtr,
    size_t      inLen)
{
    return mImpl.Read(inPtr, inLen);
}

    void
IdempotentRequestTracker::Clear()
{
    mImpl.Clear();
}

} // namespace KFS
