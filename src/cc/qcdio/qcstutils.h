//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/01
// Author: Mike Ovsiannikov
//
// Copyright 2008-2011 Quantcast Corp.
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
// Miscellaneous "stack / scope based" classes [RAII]. Not all of then have
// to do with "resource" allocation though, so calling all these RAII wouldn't
// be accurate.
//
//----------------------------------------------------------------------------

#ifndef QCSTUTILS_H
#define QCSTUTILS_H

#include "QCMutex.h"
#include "qcdebug.h"

class QCStMutexLocker
{
public:
    QCStMutexLocker(
        QCMutex& inMutex)
        : mMutexPtr(&inMutex)
        { Lock(); }

    QCStMutexLocker(
        QCMutex* inMutexPtr = 0)
        : mMutexPtr(inMutexPtr)
        { Lock(); }

    ~QCStMutexLocker()
        { Unlock(); }

    void Lock()
    {
        if (mMutexPtr) {
            mMutexPtr->Lock();
        }
    }

    void Unlock()
    {
        if (mMutexPtr) {
            mMutexPtr->Unlock();
            mMutexPtr = 0;
        }
    }

    void Attach(
        QCMutex* inMutexPtr)
    {
        Unlock();
        mMutexPtr = inMutexPtr;
        Lock();
    }

    void Detach()
        { mMutexPtr = 0; }

private:
    QCMutex* mMutexPtr;

    QCStMutexLocker(const QCStMutexLocker& inLocker);
    QCStMutexLocker& operator=(const QCStMutexLocker& inLocker);
};

class QCStMutexUnlocker
{
public:
    QCStMutexUnlocker(
        QCMutex& inMutex)
        : mMutexPtr(&inMutex)
        { Unlock(); }

    QCStMutexUnlocker(
        QCMutex* inMutexPtr = 0)
        : mMutexPtr(inMutexPtr)
        { Unlock(); }

    ~QCStMutexUnlocker()
        { Lock(); }

    void Lock()
    {
        if (mMutexPtr) {
            mMutexPtr->Lock();
            mMutexPtr = 0;
        }
    }

    void Unlock()
    {
        if (mMutexPtr) {
            mMutexPtr->Unlock();
        }
    }

    void Attach(
        QCMutex* inMutexPtr)
    {
        Lock();
        mMutexPtr = inMutexPtr;
        Unlock();
    }

    void Detach()
        { mMutexPtr = 0; }

private:
    QCMutex* mMutexPtr;

private:
    QCStMutexUnlocker(const QCStMutexUnlocker& inUnlocker);
    QCStMutexUnlocker& operator=(const QCStMutexUnlocker& inUnlocker);
};

template<typename T>
class QCStValueChanger
{
public:
    QCStValueChanger(
        T& inValRef,
        T  inNewVal)
        : mValPtr(&inValRef),
          mOrigVal(inValRef)
        { *mValPtr = inNewVal; }

    QCStValueChanger(
        T* inValPtr,
        T  inNewVal)
        : mValPtr(inValPtr),
          mOrigVal(inValPtr ? *inValPtr : T())
    {
        if (mValPtr) {
            mValPtr = inNewVal;
        }
    }

    ~QCStValueChanger()
        { QCStValueChanger::Restore(); }

    void Restore()
    {
        if (mValPtr) {
            *mValPtr = mOrigVal;
            mValPtr = 0;
        }
    }

    void Cancel()
        { mValPtr = 0; }

private:
    T*       mValPtr;
    T  const mOrigVal;

private:
    QCStValueChanger(const QCStValueChanger& inChanger);
    QCStValueChanger& operator=(const QCStValueChanger& inChanger);
};

template<typename T>
class QCStValueIncrementor
{
public:
    QCStValueIncrementor(
        T& inValRef,
        T  inIncrement)
        : mValPtr(&inValRef),
          mIncrement(inIncrement)
        { *mValPtr += inIncrement; }

    QCStValueIncrementor(
        T* inValPtr,
        T  inIncrement)
        : mValPtr(inValPtr),
          mIncrement(inIncrement)
    {
        if (mValPtr) {
            mValPtr += mIncrement;
        }
    }

    ~QCStValueIncrementor()
        { QCStValueIncrementor::Decrement(); }

    void Decrement()
    {
        if (mValPtr) {
            *mValPtr -= mIncrement;
            mValPtr = 0;
        }
    }

    void Cancel()
        { mValPtr = 0; }
private:
    T*       mValPtr;
    T  const mIncrement;

private:
    QCStValueIncrementor(const QCStValueIncrementor& inIncrementor);
    QCStValueIncrementor& operator=(const QCStValueIncrementor& inIncrementor);
};

class QCStDeleteNotifier
{
public:
    QCStDeleteNotifier(
        bool*& inTargetDeleteFlagPtr)
        : mDeletedFlag(false),
          mDeletedFlagPtr(inTargetDeleteFlagPtr ?
            inTargetDeleteFlagPtr : &mDeletedFlag),
          mTargetDeleteFlagPtr(inTargetDeleteFlagPtr)
    {
        if (! inTargetDeleteFlagPtr) {
            inTargetDeleteFlagPtr = &mDeletedFlag;
        }
    }

    ~QCStDeleteNotifier()
    {
        if (! IsDeleted() && mDeletedFlagPtr == &mDeletedFlag) {
            QCASSERT(mDeletedFlagPtr == mTargetDeleteFlagPtr);
            mTargetDeleteFlagPtr = 0;
        }
    }

    bool IsDeleted() const
        { return (*mDeletedFlagPtr); }

private:
    bool              mDeletedFlag;
    const bool* const mDeletedFlagPtr;
    bool*&            mTargetDeleteFlagPtr;

private:
    QCStDeleteNotifier(const QCStDeleteNotifier& inNotifier);
    QCStDeleteNotifier& operator=(const QCStDeleteNotifier& inNotifier);
};

class QCRefCountedObj
{
public:
    class StRef
    {
    public:
        StRef(
            const QCRefCountedObj& inObj)
            : mObj(inObj)
            { mObj.Ref(); }
        ~StRef()
            { mObj.UnRef(); }

    private:
        const QCRefCountedObj& mObj;
    private:
        StRef(
            StRef& inRef);
        StRef& operator=(
            StRef& inRef);
    };

    QCRefCountedObj()
        : mRefCount(0)
        {}

    void Ref() const
    {
        ++Mutable(mRefCount);
        QCASSERT(mRefCount > 0);
    }

    void UnRef() const
    {
        QCASSERT(mRefCount > 0);
        if (--Mutable(mRefCount) == 0) {
            delete this;
        }
    }

    int GetRefCount() const
        { return mRefCount; }

protected:
    virtual ~QCRefCountedObj()
        { mRefCount = -12345; }

private:
    int mRefCount;

    template<typename T>
    static T& Mutable(
        const T& inVar)
        { return const_cast<T&>(inVar); }
private:
    QCRefCountedObj(const QCRefCountedObj& inObj);
    QCRefCountedObj& operator=(const QCRefCountedObj& inObj);
};

#endif /* QCSTUTILS_H */
