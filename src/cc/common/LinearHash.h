//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2011/05/18
// Author: Mike Ovsainnikov
//
// Copyright 2011 Quantcast Corp.
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
// [Sorted] linear hash table implementation. The insertion and removal cost is
// constant (assuming "good" hash -- low number of collisions) as adding or
// removing item always re-hashes single bucket. The overhead per item is 2
// pointers. Lookup cost (with "good" hash) is ~2 dram cache misses. Suitable
// for implementing map/set with large dynamic size ranges.
//
//----------------------------------------------------------------------------

#ifndef LINEAR_HASH_H
#define LINEAR_HASH_H

#include "DynamicArray.h"
#include "SingleLinkedList.h"

#include <cstddef>
#include <memory>
#include <algorithm>

namespace KFS
{

template<typename T>
struct IntegerHash
{
    typedef std::size_t size_t;

    static size_t Hash(
        const T& inVal)
        { return size_t(inVal); }
};

template<typename T, typename H = IntegerHash<T> >
struct KeyCompare
{
    typedef std::size_t size_t;

    static bool Equals(
        const T& inLhs,
        const T& inRhs)
        { return inLhs == inRhs; }
    static bool Less(
        const T& inLhs,
        const T& inRhs)
        { return inLhs < inRhs; }
    static size_t Hash(
        const T& inVal)
        { return H::Hash(inVal); }
};

template <typename KeyT, typename ValT>
class KVPair
{
public:
    typedef KeyT Key;
    typedef ValT Val;

    KVPair(
        const Key& inKey,
        const Val& inVal)
        : mKey(inKey),
          mVal(inVal)
        {}
    Key& GetKey()             { return mKey; }
    const Key& GetKey() const { return mKey; }
    Val& GetVal()             { return mVal; }
    const Val& GetVal() const { return mVal; }
private:
    Key mKey;
    Val mVal;
};

template <typename KeyT>
class KeyOnly
{
public:
    typedef KeyT Key;
    typedef KeyT Val;

    KeyOnly(
        const Key& inKey,
        const Val& /* inVal */)
        : mKey(inKey)
        {}
    Key& GetKey()             { return mKey; }
    const Key& GetKey() const { return mKey; }
    Val& GetVal()             { return mKey; }
    const Val& GetVal() const { return mKey; }
private:
    Key mKey;
};

template<typename T>
class DeleteObserver
{
public:
    void operator()(T&) {}
};

template<
  typename KVPairT,
  typename KeyIdT          = KeyCompare<typename KVPairT::Key>,
  typename DArrayT         = DynamicArray<SingleLinkedList<KVPairT>*>,
  typename AllocT          = std::allocator<KVPairT>,
  typename DeleteObserverT = DeleteObserver<KVPairT>
>
class LinearHash
{
public:
    typedef typename KVPairT::Key                          Key;
    typedef typename KVPairT::Val                          Val;
    typedef SingleLinkedList<KVPairT>                      Entry;
    typedef typename AllocT::template rebind<Entry>::other Allocator;
    typedef std::size_t                                    size_t;

    static inline size_t MaxSize()
        { return DArrayT::MaxSize(); }

    LinearHash()
        : mSplitIdx(0),
          mMaxSplitIdx(1),
          mNextBucketIdx(0),
          mNextEntryPtr(0),
          mBuckets(),
          mKeyId(),
          mAlloc(),
          mDelObserverPtr(0)
        {}
    LinearHash(
        const LinearHash& inHash)
        : mSplitIdx(0),
          mMaxSplitIdx(1),
          mNextBucketIdx(0),
          mNextEntryPtr(0),
          mBuckets(),
          mKeyId(),
          mAlloc(),
          mDelObserverPtr(0)
        { *this = inHash; }
    ~LinearHash()
        { LinearHash::Clear(); }
    LinearHash& operator=(
        const LinearHash& inHash)
    {
        if (this == &inHash) {
            return *this;
        }
        Clear();
        for (size_t i = 0; i < inHash.GetSize(); i++) {
            const Entry* thePtr = inHash.mBuckets[i];
            while (thePtr) {
                bool theInsertedFlag;
                Insert(thePtr->GetData().GetKey(), thePtr->GetData().GetVal(),
                    theInsertedFlag);
                thePtr = thePtr->GetNextPtr();
            }
        }
        return *this;
    }
    void SetDeleteObserver(
        DeleteObserverT* inObserverPtr)
        { mDelObserverPtr = inObserverPtr; }
    const Allocator& GetAllocator() const
        { return mAlloc; }
    size_t GetSize() const
        { return mBuckets.GetSize(); }
    size_t IsEmpty() const
        { return mBuckets.IsEmpty(); }
    void Clear()
    {
        mNextBucketIdx = 0;
        mNextEntryPtr  = 0;
        while (! mBuckets.IsEmpty()) {
            Entry*& theBackPtr = mBuckets.Back();
            Entry*  thePtr     = theBackPtr;
            theBackPtr = 0;
            while (thePtr) {
                Entry* const theNextPtr = thePtr->GetNextPtr();
                Delete(*thePtr);
                thePtr = theNextPtr;
            }
            mBuckets.PopBack();
        }
        mSplitIdx    = 0;
        mMaxSplitIdx = 1;
    }
    Val* Find(
        const Key& inKey) const
    {
        if (IsEmpty()) {
            return 0;
        }
        Entry* thePtr = GetBucket(inKey);
        while (thePtr) {
            if (mKeyId.Equals(inKey, thePtr->GetData().GetKey())) {
                return &(thePtr->GetData().GetVal());
            }
            thePtr = thePtr->GetNextPtr();
        }
        return 0;
    }
    Val* Insert(
        const Key& inKey,
        const Val& inVal,
        bool&      outInsertedFlag)
    {
        if (IsEmpty()) {
            Entry& theEntry = New(Entry(KVPairT(inKey, inVal), 0));
            mBuckets.PushBack(&theEntry);
            outInsertedFlag = true;
            return &(theEntry.GetData().GetVal());
        }
        Entry*& theBucketPtr = GetBucket(inKey);
        Entry*  thePtr       = theBucketPtr;
        if (! thePtr || mKeyId.Less(inKey, thePtr->GetData().GetKey())) {
            Entry& theEntry = New(Entry(KVPairT(inKey, inVal), thePtr));
            theBucketPtr = &theEntry;
            Split();
            outInsertedFlag = true;
            return &(theEntry.GetData().GetVal());
        }
        while (! mKeyId.Equals(inKey, thePtr->GetData().GetKey())) {
            Entry* const thePrevPtr = thePtr;
            thePtr = thePtr->GetNextPtr();
            if (! thePtr || mKeyId.Less(inKey, thePtr->GetData().GetKey())) {
                Entry& theEntry = New(Entry(KVPairT(inKey, inVal), thePtr));
                thePrevPtr->GetNextPtr() = &theEntry;
                Split();
                outInsertedFlag = true;
                return &(theEntry.GetData().GetVal());
            }
        }
        outInsertedFlag = false;
        return &(thePtr->GetData().GetVal());
    }
    size_t Erase(
        const Key& inKey)
    {
        if (IsEmpty()) {
            return 0;
        }
        Entry*& theBucketPtr = GetBucket(inKey);
        Entry*  thePtr       = theBucketPtr;
        if (! thePtr) {
            return 0;
        }
        if (mKeyId.Equals(inKey, thePtr->GetData().GetKey())) {
            theBucketPtr = thePtr->GetNextPtr();
            Delete(*thePtr);
            Merge();
            mBuckets.PopBack();
            return 1;
        }
        // With good hash function the lists should be short enough.
        // Early termination using Less() wouldn't get much with short
        // lists.
        for (; ;) {
            Entry* const thePrevPtr = thePtr;
            if (! (thePtr = thePtr->GetNextPtr())) {
                break;
            }
            if (mKeyId.Equals(inKey, thePtr->GetData().GetKey())) {
                thePrevPtr->GetNextPtr() = thePtr->GetNextPtr();
                Delete(*thePtr);
                Merge();
                mBuckets.PopBack();
                return 1;
            }
        }
        return 0;
    }
    void First()
    {
        mNextEntryPtr  = 0;
        mNextBucketIdx = 0;
    }
    const KVPairT* Next()
    {
        if (! mNextEntryPtr) {
            const size_t theSize = GetSize();
            while (mNextBucketIdx < theSize &&
                    ! (mNextEntryPtr = mBuckets[mNextBucketIdx])) {
                mNextBucketIdx++;
            }
            if (! mNextEntryPtr) {
                return 0;
            }
        }
        KVPairT& theRet = mNextEntryPtr->GetData();
        if (! (mNextEntryPtr = mNextEntryPtr->GetNextPtr())) {
            mNextBucketIdx++;
        }
        return &theRet;
    }
    void Swap(LinearHash& inHash)
    {
        if (this == &inHash) {
            return;
        }
        mBuckets.Swap(inHash.mBuckets);
        std::swap(mSplitIdx,       inHash.mSplitIdx);
        std::swap(mMaxSplitIdx,    inHash.mMaxSplitIdx);
        std::swap(mNextBucketIdx,  inHash.mNextBucketIdx);
        std::swap(mNextEntryPtr,   inHash.mNextEntryPtr);
        std::swap(mKeyId,          inHash.mKeyId);
        std::swap(mAlloc,          inHash.mAlloc);
        std::swap(mDelObserverPtr, inHash.mDelObserverPtr);
    }

private:
    size_t           mSplitIdx;
    size_t           mMaxSplitIdx;   // Split upper bound during this expansion.
    size_t           mNextBucketIdx; // Cursor.
    Entry*           mNextEntryPtr;  // Cursor.
    DArrayT          mBuckets;       // Hash table buckets.
    KeyIdT           mKeyId;
    Allocator        mAlloc;
    DeleteObserverT* mDelObserverPtr;

    Entry& New(
        const Entry& inEntry)
    {
        Entry& theEntry = *mAlloc.allocate(1);
        mAlloc.construct(&theEntry, inEntry);
        return theEntry;
    }
    void Delete(
        Entry& inEntry)
    {
        if (&inEntry == mNextEntryPtr &&
                ! (mNextEntryPtr = mNextEntryPtr->GetNextPtr())) {
            mNextBucketIdx++;
        }
        if (mDelObserverPtr) {
            (*mDelObserverPtr)(inEntry.GetData());
        }
        mAlloc.destroy(&inEntry);
        mAlloc.deallocate(&inEntry, 1);
    }
    static size_t BucketIdx(
        size_t inMaxSplitIdx,
        size_t inSplitIdx,
        size_t inHash)
    {
        // maxSplit always power of 2, thus:
        // hash % maxSplit == hash & (maxSplit - 1)
        const size_t theIdx = inHash & (inMaxSplitIdx - 1);
        return (theIdx < inSplitIdx ?
            (inHash & (inMaxSplitIdx + inMaxSplitIdx - 1)) :
            theIdx);
    }
    Entry*& GetBucket(
        const Key& inKey) const
    {
        return mBuckets[BucketIdx(mMaxSplitIdx, mSplitIdx, mKeyId.Hash(inKey))];
    }
    void Split()
    {
        if (IsEmpty()) {
            return; // Nothing to split.
        }
        Entry*&      theBucketPtr = mBuckets.PushBack(0);
        const size_t thePrevIdx   = mSplitIdx;
        if (++mSplitIdx >= mMaxSplitIdx) {
            // Start new expansion round.
            mSplitIdx = 0;
            mMaxSplitIdx += mMaxSplitIdx;
        }
        // Split into prev and new buckets, preserving the order.
        Entry*  theTailPtr       = 0;
        Entry*& thePrevBucketPtr = mBuckets[thePrevIdx];
        Entry*  thePtr           = thePrevBucketPtr;
        Entry*  thePrevPtr       = 0;
        while (thePtr) {
            Entry* const theNextPtr = thePtr->GetNextPtr();
            if (BucketIdx(mMaxSplitIdx, mSplitIdx,
                    mKeyId.Hash(thePtr->GetData().GetKey())) == thePrevIdx) {
                thePrevPtr = thePtr;
            } else {
                // Move the entry into new bucket.
                if (thePrevPtr) {
                    thePrevPtr->GetNextPtr() = theNextPtr;
                } else {
                    thePrevBucketPtr = theNextPtr;
                }
                if (theTailPtr) {
                    theTailPtr->GetNextPtr() = thePtr;
                } else {
                    theBucketPtr = thePtr;
                }
                theTailPtr = thePtr;
                thePtr->GetNextPtr() = 0;
            }
            thePtr = theNextPtr;
        }
    }
    void Merge()
    {
        if (mSplitIdx == 0) {
            // Start new collapse round.
            // The size here is +1: the bucket removal happens after Merge().
            if (GetSize() <= 1) {
                return;
            }
            mMaxSplitIdx >>= 1;
            mSplitIdx = mMaxSplitIdx - 1;
        } else {
            --mSplitIdx;
        }
        Entry*& thePrevBucketPtr = mBuckets.Back();
        Entry*  thePtr           = thePrevBucketPtr;
        if (! thePtr) {
            return;
        }
        thePrevBucketPtr = 0;
        Entry*& theBucketPtr = mBuckets[mSplitIdx];
        Entry*  theInsertPtr = theBucketPtr;
        if (! theInsertPtr) {
            theBucketPtr = thePtr;
            return;
        }
        Entry* theInsertPrevPtr = 0;
        while (thePtr) {
            Entry* const theNextPtr = thePtr->GetNextPtr();
            while (theInsertPtr && mKeyId.Less(theInsertPtr->GetData().GetKey(),
                    thePtr->GetData().GetKey())) {
                theInsertPrevPtr = theInsertPtr;
                theInsertPtr     = theInsertPtr->GetNextPtr();
            }
            if (! theInsertPtr) {
                theInsertPrevPtr->GetNextPtr() = thePtr;
                return;
            }
            thePtr->GetNextPtr() = theInsertPtr;
            if (theInsertPrevPtr) {
                theInsertPrevPtr->GetNextPtr() = thePtr;
            } else {
                theBucketPtr = thePtr;
            }
            theInsertPrevPtr = thePtr;
            thePtr           = theNextPtr;
        }
    }
};

}

#endif /* LINEAR_HASH_H */
