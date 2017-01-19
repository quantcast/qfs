//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/09/14
// Author: Mike Ovsiannikov
//
// Copyright 2016 Quantcast Corp.
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
// Restart process by issuing exec with saved args, environment, and working
// directory.
//
//----------------------------------------------------------------------------

#include "ProcessRestarter.h"

#include "common/Properties.h"

#include "qcdio/QCUtils.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/resource.h>

#include <string>

extern char **environ;

namespace KFS
{
using std::string;

class ProcessRestarter::Impl
{
public:
    Impl(
        bool inCloseFdsAtInitFlag,
        bool inSaveRestoreEnvFlag,
        bool inExitOnRestartFlag,
        bool inCloseFdsBeforeExecFlag,
        int  inMaxGracefulRestartSeconds)
        : mCwd(0),
          mArgs(0),
          mEnv(0),
          mMaxGracefulRestartSeconds(inMaxGracefulRestartSeconds),
          mExitOnRestartFlag(inExitOnRestartFlag),
          mCloseFdsAtInitFlag(inCloseFdsAtInitFlag),
          mCloseFdsBeforeExecFlag(inCloseFdsBeforeExecFlag),
          mSaveRestoreEnvFlag(inSaveRestoreEnvFlag)
        {}
    ~Impl()
        { Cleanup(); }
    int Init(
        int    inArgCnt,
        char** inArgsPtr)
    {
        ::alarm(0);
        if (::signal(SIGALRM, &Impl::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        Cleanup();
        if (inArgCnt < 1 || ! inArgsPtr) {
            return EINVAL;
        }
        int theErr = 0;
        for (int theLen = 4 << 10; theLen < (128 << 10); theLen += theLen) {
            mCwd = (char*)::malloc(theLen);
            if (! mCwd || ::getcwd(mCwd, theLen)) {
                break;
            }
            theErr = errno;
            ::free(mCwd);
            mCwd = 0;
            if (theErr != ERANGE) {
                break;
            }
        }
        if (! mCwd) {
            return (0 == theErr ? EINVAL : theErr);
        }
        mArgs = new char*[inArgCnt + 1];
        int i;
        for (i = 0; i < inArgCnt; i++) {
            if (! (mArgs[i] = ::strdup(inArgsPtr[i]))) {
                theErr = errno;
                Cleanup();
                return (0 == theErr ? ENOMEM : theErr);
            }
        }
        mArgs[i] = 0;
        if (mSaveRestoreEnvFlag) {
            if (environ) {
                char** thePtr = environ;
                for (i = 0; *thePtr; i++, thePtr++)
                    {}
                mEnv = new char*[i + 1];
                for (i = 0, thePtr = environ; *thePtr; ) {
                    if (! (mEnv[i++] = ::strdup(*thePtr++))) {
                        theErr = errno;
                        Cleanup();
                        return (0 == theErr ? ENOMEM : theErr);
                    }
                }
            } else {
                i = 0;
                mEnv = new char*[i + 1];
            }
            mEnv[i] = 0;
        }
        if (mCloseFdsAtInitFlag) {
            CloseFds();
        }
        return 0;
    }
    void SetParameters(
        const char*       inPrefixPtr,
        const Properties& inProps)
    {
        Properties::String theName(inPrefixPtr ? inPrefixPtr : "");
        const size_t       theLen = theName.GetSize();
        mMaxGracefulRestartSeconds = inProps.getValue(
            theName.Truncate(theLen).Append("maxGracefulRestartSeconds"),
            mMaxGracefulRestartSeconds
        );
        // Prior name for backward compatibility.
        mExitOnRestartFlag = inProps.getValue(
            theName.Truncate(theLen).Append("exitOnRestart"),
            mExitOnRestartFlag ? 1 : 0
        ) != 0;
        mExitOnRestartFlag = inProps.getValue(
            theName.Truncate(theLen).Append("exitOnRestart"),
            mExitOnRestartFlag ? 1 : 0
        ) != 0;
        mCloseFdsAtInitFlag = inProps.getValue(
            theName.Truncate(theLen).Append("closeFdsAtInit"),
            mCloseFdsAtInitFlag ? 1 : 0
        ) != 0;
        mCloseFdsBeforeExecFlag = inProps.getValue(
            theName.Truncate(theLen).Append("closeFdsBeforeExec"),
            mCloseFdsBeforeExecFlag ? 1 : 0
        ) != 0;
    }
    string Restart()
    {
        if (! mCwd || ! mArgs || ! mArgs[0] || ! mArgs[0][0] ||
                (! mEnv && mSaveRestoreEnvFlag)) {
            return string("not initialized");
        }
        if (! mExitOnRestartFlag) {
            struct stat theRes = {0};
            if (::stat(mCwd, &theRes) != 0) {
                return QCUtils::SysError(errno, mCwd);
            }
            if (! S_ISDIR(theRes.st_mode)) {
                return (mCwd + string(": not a directory"));
            }
            string theExecpath(mArgs[0][0] == '/' ? mArgs[0] : mCwd);
            if (mArgs[0][0] != '/') {
                if (! theExecpath.empty() &&
                        theExecpath.at(theExecpath.length() - 1) != '/') {
                    theExecpath += "/";
                }
                theExecpath += mArgs[0];
            }
            if (::stat(theExecpath.c_str(), &theRes) != 0) {
                return QCUtils::SysError(errno, theExecpath.c_str());
            }
            if (! S_ISREG(theRes.st_mode)) {
                return (theExecpath + string(": not a file"));
            }
        }
        if (::signal(SIGALRM, &Impl::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        if (0 < mMaxGracefulRestartSeconds) {
            if (sInstancePtr) {
                return string("restart in progress");
            }
            sInstancePtr = this;
            if (::atexit(&Impl::RestartSelf)) {
                sInstancePtr = 0;
                return QCUtils::SysError(errno, "atexit");
            }
            ::alarm((unsigned int)mMaxGracefulRestartSeconds);
        } else {
            ::alarm((unsigned int)-mMaxGracefulRestartSeconds);
            Exec();
        }
        return string();
    }
    static void CloseFds(
        int inFrstFd = 3)
    {
        int theMaxFds = 16 << 10;
        struct rlimit theLimt = { 0 };
        if (0 == getrlimit(RLIMIT_NOFILE, &theLimt)) {
            if (0 < theLimt.rlim_cur) {
                theMaxFds = (int)theLimt.rlim_cur;
            }
        }
        for (int i = inFrstFd; i < theMaxFds; i++) {
            close(i);
        }
    }
private:
    char*  mCwd;
    char** mArgs;
    char** mEnv;
    int    mMaxGracefulRestartSeconds;
    bool   mExitOnRestartFlag;
    bool   mCloseFdsAtInitFlag;
    bool   mCloseFdsBeforeExecFlag;
    bool   mSaveRestoreEnvFlag;

    static Impl* sInstancePtr;

    static void FreeArgs(
        char** inArgsPtr)
    {
        if (! inArgsPtr) {
            return;
        }
        char** thePtr = inArgsPtr;
        while (*thePtr) {
            ::free(*thePtr++);
        }
        delete [] inArgsPtr;
    }
    void Cleanup()
    {
        free(mCwd);
        mCwd = 0;
        FreeArgs(mArgs);
        mArgs = 0;
        FreeArgs(mEnv);
        mEnv = 0;
    }
    void Exec()
    {
        if (mExitOnRestartFlag) {
            _exit(0);
        }
        if (mEnv && mSaveRestoreEnvFlag) {
#ifdef KFS_OS_NAME_LINUX
            ::clearenv();
#else
            environ = 0;
#endif
            for (char** thePtr = mEnv; *thePtr; thePtr++) {
                if (::putenv(*thePtr)) {
                    QCUtils::FatalError("putenv", errno);
                }
            }
        }
        if (::chdir(mCwd) != 0) {
            QCUtils::FatalError(mCwd, errno);
        }
        if (mCloseFdsBeforeExecFlag) {
            CloseFds();
        }
        execvp(mArgs[0], mArgs);
        QCUtils::FatalError(mArgs[0], errno);
    }
    static void RestartSelf()
    {
        if (! sInstancePtr) {
            ::abort();
        }
        sInstancePtr->Exec();
    }
    static void SigAlrmHandler(
        int /* inSig */)
    {
        if (write(2, "SIGALRM\n", 8) < 0) {
            QCUtils::SetLastIgnoredError(errno);
        }
        ::abort();
    }
};
ProcessRestarter::Impl* ProcessRestarter::Impl::sInstancePtr = 0;

ProcessRestarter::ProcessRestarter(
    bool inCloseFdsAtInitFlag,
    bool inSaveRestoreEnvFlag,
    bool inExitOnRestartFlag,
    bool inCloseFdsBeforeExecFlag,
    int  inMaxGracefulRestartSeconds)
    : mImpl(*(new Impl(
            inCloseFdsAtInitFlag,
            inSaveRestoreEnvFlag,
            inExitOnRestartFlag,
            inCloseFdsBeforeExecFlag,
            inMaxGracefulRestartSeconds)))
{}

ProcessRestarter::~ProcessRestarter()
{
    delete &mImpl;
}

    int
ProcessRestarter::Init(
    int    inArgCnt,
    char** inArgsPtr)
{
    return mImpl.Init(inArgCnt, inArgsPtr);
}

    void
ProcessRestarter::SetParameters(
    const char*       inPrefixPtr,
    const Properties& inProps)
{
    mImpl.SetParameters(inPrefixPtr, inProps);
}

    string
ProcessRestarter::Restart()
{
    return mImpl.Restart();
}

    /* static */ void
ProcessRestarter::CloseFds(
    int inFrstFd)
{
    Impl::CloseFds(inFrstFd);
}

} // namespace KFS
