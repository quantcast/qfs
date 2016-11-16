//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/12/15
// Author: Mike Ovsainnikov
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
// Singly linked queue.
//
//----------------------------------------------------------------------------

#ifndef KFS_COMMON_SINGLE_LINKED_QUEUE_H
#define KFS_COMMON_SINGLE_LINKED_QUEUE_H

#include "qcdio/QCUtils.h"

namespace KFS
{

template<typename T, typename AccessorT>
class SingleLinkedQueue
{
public:
    typedef T         Entry;
    typedef AccessorT Accessor;

    SingleLinkedQueue()
        : mHeadPtr(0),
          mTailPtr(0)
        {}
    SingleLinkedQueue(
        Entry* inHeadPtr,
        Entry* inTailPtr)
        : mHeadPtr(inHeadPtr),
          mTailPtr(inTailPtr)
        {}
    void Set(
        Entry* inHeadPtr,
        Entry* inTailPtr)
    {
        mHeadPtr = inHeadPtr;
        mTailPtr = inTailPtr;
    }
    void Swap(
        SingleLinkedQueue& inQueue)
    {
        Entry* thePtr = mHeadPtr;
        mHeadPtr = inQueue.mHeadPtr;
        inQueue.mHeadPtr = thePtr;
        thePtr = mTailPtr;
        mTailPtr = inQueue.mTailPtr;
        inQueue.mTailPtr = thePtr;
    }
    void Reset()
    {
        mHeadPtr = 0;
        mTailPtr = 0;
    }
    bool IsEmpty() const
        { return (! mTailPtr); }
    Entry* Front() const
        { return mHeadPtr; }
    Entry* Back() const
        { return mTailPtr; }
    Entry* PopFront()
    {
        if (! mTailPtr) {
            return 0;
        }
        Entry& theCur = *mHeadPtr;
        mHeadPtr = Accessor::Next(theCur);
        if (! mHeadPtr) {
            mTailPtr = 0;
        }
        Accessor::Next(theCur) = 0;
        return &theCur;
    }
    void PushBack(
        Entry& inNode)
    {
        QCRTASSERT(! Accessor::Next(inNode));
        if (mTailPtr) {
            Accessor::Next(*mTailPtr) = &inNode;
        } else {
            mHeadPtr = &inNode;
        }
        mTailPtr = &inNode;
    }
    void PushBack(
        SingleLinkedQueue& inQueue)
    {
        if (inQueue.mTailPtr) {
            if (mTailPtr) {
                Accessor::Next(*mTailPtr) = inQueue.mHeadPtr;
            } else {
                mHeadPtr = inQueue.mHeadPtr;
            }
            mTailPtr = inQueue.mTailPtr;
            inQueue.mHeadPtr = 0;
            inQueue.mTailPtr = 0;
        }
    }
    void PopFront(
        SingleLinkedQueue& inQueue)
        { inQueue.PushBack(*this); }
    void PutFront(
        Entry& inNode)
    {
        QCRTASSERT(! Accessor::Next(inNode));
        if (mTailPtr) {
            Accessor::Next(inNode) = mHeadPtr;
        } else {
            mTailPtr = &inNode;
        }
        mHeadPtr = &inNode;
    }
    static Entry* GetNext(
        Entry& inNode)
        { return Accessor::Next(inNode); }
    static const Entry* GetNext(
        const Entry& inNode)
        { return Accessor::Next(inNode); }
private:
    Entry* mHeadPtr;
    Entry* mTailPtr;
private:
    SingleLinkedQueue(
        const SingleLinkedQueue& inQueue);
    SingleLinkedQueue& operator=(
        const SingleLinkedQueue& inQueue);
};

} // namespace KFS

#endif /* KFS_COMMON_SINGLE_LINKED_QUEUE_H */

