//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/01
// Author: Mike Ovsiannikov
//
// Copyright 2008-2010 Quantcast Corp.
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
// System agnostic pthread wrapper. Global thread list can be used for
// for debugging. All threads, except the main thread must not be started before
// entering main, and must be joined before exiting main before static / global
// destructors run.
// Thread affinity support only implemented on linux platform.
//
//----------------------------------------------------------------------------

#ifndef QCTHREAD_H
#define QCTHREAD_H

#include <pthread.h>
#include <string>
#include <inttypes.h>

class QCRunnable
{
public:
    virtual void Run() = 0;

protected:
    QCRunnable()
        {}
    virtual ~QCRunnable()
        {}
};

class QCThread : public QCRunnable
{
public:
    class CpuAffinity
    {
    public:
        CpuAffinity()
            : mCpus(0)
            {}
        CpuAffinity(
            int inCpuIndex)
            : mCpus(inCpuIndex >= 0 ? (Cpus(1) << inCpuIndex) : ~Cpus(0))
            {}
        CpuAffinity(
            const CpuAffinity& inAffinity)
            : mCpus(inAffinity.mCpus)
            {}
        CpuAffinity& operator=(
            const CpuAffinity& inAffinity)
        {
            mCpus = inAffinity.mCpus;
            return *this;
        }
        bool operator==(
            const CpuAffinity& inAffinity) const
            { return (mCpus == inAffinity.mCpus); }
        bool operator!=(
            const CpuAffinity& inAffinity) const
            { return (mCpus != inAffinity.mCpus); }
        CpuAffinity& Clear()
        {
            mCpus = 0;
            return *this;
        }
        CpuAffinity& Clear(
            int inCpuIndex)
        {
            mCpus &= ~(Cpus(1) << inCpuIndex);
            return *this;
        }
        CpuAffinity& Set(
            int inCpuIndex)
        {
            mCpus |= (Cpus(1) << inCpuIndex);
            return *this;
        }
        bool IsSet(
            int inCpuIndex) const
            { return (((mCpus >> inCpuIndex) & Cpus(1)) != 0); }
        static CpuAffinity None()
            { return CpuAffinity(-1); }
    private:
        typedef uint64_t Cpus;
        Cpus mCpus;
    };

    QCThread(
        QCRunnable* inRunnablePtr = 0,
        const char* inNamePtr     = 0);
    virtual ~QCThread();
    void Start(
        QCRunnable* inRunnablePtr = 0,
        int         inStackSize   = -1,
        const char* inNamePtr     = 0,
        CpuAffinity inAffinity    = CpuAffinity::None())
    {
        const int theErr = TryToStart(
            inRunnablePtr, inStackSize, inNamePtr, inAffinity);
        if (theErr) {
            FatalError("TryToStart", theErr);
        }
    }
    int TryToStart(
        QCRunnable* inRunnablePtr = 0,
        int         inStackSize   = -1,
        const char* inNamePtr     = 0,
        CpuAffinity inAffinity    = CpuAffinity::None());
    void Join();
    virtual void Run()
        {}
    bool IsStarted() const
        { return mStartedFlag; }
    std::string GetName() const
        { return mName; }
    bool IsCurrentThread() const
        { return (::pthread_equal(mThread, ::pthread_self()) != 0); }
    static std::string GetErrorMsg(
        int inErrorCode);
    static int GetThreadCount();
    static int SetCurrentThreadAffinity(CpuAffinity inAffinity);

private:
    bool        mStartedFlag;
    pthread_t   mThread;
    QCRunnable* mRunnablePtr;
    CpuAffinity mAffinity;
    std::string mName;
    QCThread*   mPrevPtr[1];
    QCThread*   mNextPtr[1];
    template<typename, unsigned int> friend class QCDLListOp;

    static void* Runner(
        void* inArgPtr);
    void RunnerSelf();
    void FatalError(
        const char* inErrMsgPtr,
        int         inSysError);
    void Insert(
        QCThread& inAfter);
    void Remove();

    // No copies.
    QCThread(const QCThread& inThread);
    QCThread& operator=(const QCThread& inThread);
};

#endif /* QCTHREAD_H */
