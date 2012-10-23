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
// Thread wrapper implementation.
// 
//----------------------------------------------------------------------------

#include "QCThread.h"
#include "QCMutex.h"
#include "qcstutils.h"
#include "QCUtils.h"
#include "QCDLList.h"
#include "qcdebug.h"

#ifdef QC_OS_NAME_LINUX
#include <sys/types.h>
#include <sys/syscall.h>
#include <sched.h>
#include <unistd.h>
#endif

class QCStartedThreadList
{
public:
    typedef QCDLListOp<QCThread, 0> ThreadList;

    QCStartedThreadList()
        : mMutex(),
          mHead(0, "QCThread list head"),
          mMainThread(pthread_self()),
          mCount(0)
        {}
    ~QCStartedThreadList()
        { QCASSERT(mCount == 0); }
    void Insert(
        QCThread& inThread)
    {
        QCStMutexLocker theLock(mMutex);
        ThreadList::Insert(inThread, ThreadList::GetPrev(mHead));
        mCount++;
    }
    void Remove(
        QCThread& inThread)
    {
        QCStMutexLocker theLock(mMutex);
        ThreadList::Remove(inThread);
        mCount--;
    }
    int GetThreadCount()
    {
        QCStMutexLocker theLock(mMutex);
        return mCount;
    }

private:
    QCMutex   mMutex;
    QCThread  mHead;
    pthread_t mMainThread;
    int       mCount;
};
static QCStartedThreadList sThreadList;


QCThread::QCThread(
    QCRunnable* inRunnablePtr /* = 0 */,
    const char* inNamePtr     /* = 0 */)
    : QCRunnable(),
      mStartedFlag(false),
      mThread(),
      mRunnablePtr(inRunnablePtr),
      mAffinity(CpuAffinity::None()),
      mName(inNamePtr ? inNamePtr : "")
{
    QCStartedThreadList::ThreadList::Init(*this);
}

    /* virtual */QCThread::
QCThread::~QCThread()
{
    QCThread::Join();
}

    int
QCThread::TryToStart(
    QCRunnable*           inRunnablePtr /* = 0 */,
    int                   inStackSize   /* = -1 */,
    const char*           inNamePtr     /* = 0 */,
    QCThread::CpuAffinity inAffinity    /* = CpuAffinity::None() */)
{
    if (mStartedFlag) {
        return EINVAL;
    }
    mStartedFlag = true;
    pthread_attr_t theStackSizeAttr;
    int theErr = pthread_attr_init(&theStackSizeAttr);
    if (theErr != 0) {
        return theErr;
    }
    if (inStackSize > 0 && (theErr = pthread_attr_setstacksize(
            &theStackSizeAttr, inStackSize)) != 0) {
        pthread_attr_destroy(&theStackSizeAttr);
        return theErr;
    }
    if (inNamePtr) {
        mName = inNamePtr;
    }
    if (inRunnablePtr) {
        mRunnablePtr = inRunnablePtr;
    }
    if (! mRunnablePtr) {
        mRunnablePtr = this;
    }
    mAffinity = inAffinity;
    theErr = pthread_create(
        &mThread, &theStackSizeAttr, &QCThread::Runner, this);
    pthread_attr_destroy(&theStackSizeAttr);
    if (theErr != 0) {
        mStartedFlag = false;
        return theErr;
    }
    sThreadList.Insert(*this);
    return theErr;
}

   void
QCThread::Join()
{
    if (! mStartedFlag) {
        return;
    }
    const int theErr = pthread_join(mThread, 0);
    if (theErr) {
        FatalError("pthread_join", theErr);
    }
    mStartedFlag = false;
    sThreadList.Remove(*this);
}

    void
QCThread::FatalError(
    const char* inErrMsgPtr,
    int         inSysError)
{
    QCUtils::FatalError(inErrMsgPtr, inSysError);
}

    void
QCThread::RunnerSelf()
{
    const int theError = SetCurrentThreadAffinity(mAffinity);
    if (theError) {
        FatalError("sched_setaffinity", theError);
    }
    mRunnablePtr->Run();
}
    /* static */ void*
QCThread::Runner(
    void* inArgPtr)
{
    reinterpret_cast<QCThread*>(inArgPtr)->RunnerSelf();
    return 0;
}

    /* static */ std::string
QCThread::GetErrorMsg(
    int inErrorCode)
{
    return QCUtils::SysError(inErrorCode);
}

    /* static */ int
QCThread::GetThreadCount()
{
    return sThreadList.GetThreadCount();
}

/* static */ int
QCThread::SetCurrentThreadAffinity(CpuAffinity inAffinity)
{
#ifdef QC_OS_NAME_LINUX
    long theNumCpus = sysconf(_SC_NPROCESSORS_CONF);
    if (theNumCpus > CPU_SETSIZE) {
        theNumCpus = CPU_SETSIZE;
    }
    if (theNumCpus > 1 && inAffinity != CpuAffinity::None()) {
        cpu_set_t theSet;
        CPU_ZERO(&theSet);
        int theNumSet = 0;
        for (int i = 0; i < theNumCpus; i++) {
            if (inAffinity.IsSet(i)) {
                CPU_SET(i, &theSet);
                theNumSet++;
            }
        }
        if (theNumSet <= 0) {
            // Run on last cpu.
            CPU_SET(theNumCpus - 1, &theSet);
        }
        // Use syscall until glibc catches up.
        if (sched_setaffinity(syscall(SYS_gettid), sizeof(theSet), &theSet)) {
            return errno;
        }
    }
#endif
    return 0;
}
