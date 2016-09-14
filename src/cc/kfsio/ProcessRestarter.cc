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
    Impl()
        : mCwd(0),
          mArgs(0),
          mEnv(0),
          mMaxGracefulRestartSeconds(60 * 6),
          mExitOnRestartFlag(false)
        {}
    ~Impl()
        { Cleanup(); }
    bool Init(
        int    inArgCnt,
        char** inArgsPtr)
    {
        ::alarm(0);
        if (::signal(SIGALRM, &Impl::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        Cleanup();
        if (inArgCnt < 1 || ! inArgsPtr) {
            return false;
        }
        for (int len = PATH_MAX; len < PATH_MAX * 1000; len += PATH_MAX) {
            mCwd = (char*)::malloc(len);
            if (! mCwd || ::getcwd(mCwd, len)) {
                break;
            }
            const int err = errno;
            ::free(mCwd);
            mCwd = 0;
            if (err != ERANGE) {
                break;
            }
        }
        if (! mCwd) {
            return false;
        }
        mArgs = new char*[inArgCnt + 1];
        int i;
        for (i = 0; i < inArgCnt; i++) {
            if (! (mArgs[i] = ::strdup(inArgsPtr[i]))) {
                Cleanup();
                return false;
            }
        }
        mArgs[i] = 0;
        char** ptr = environ;
        for (i = 0; *ptr; i++, ptr++)
            {}
        mEnv = new char*[i + 1];
        for (i = 0, ptr = environ; *ptr; ) {
            if (! (mEnv[i++] = ::strdup(*ptr++))) {
                Cleanup();
                return false;
            }
        }
        mEnv[i] = 0;
        return true;
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
        mExitOnRestartFlag = inProps.getValue(
            theName.Truncate(theLen).Append("exitOnRestartFlag"),
            mExitOnRestartFlag
        );
    }
    string Restart()
    {
        if (! mCwd || ! mArgs || ! mEnv || ! mArgs[0] || ! mArgs[0][0]) {
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
            string execpath(mArgs[0][0] == '/' ? mArgs[0] : mCwd);
            if (mArgs[0][0] != '/') {
                if (! execpath.empty() &&
                        execpath.at(execpath.length() - 1) != '/') {
                    execpath += "/";
                }
                execpath += mArgs[0];
            }
            if (::stat(execpath.c_str(), &theRes) != 0) {
                return QCUtils::SysError(errno, execpath.c_str());
            }
            if (! S_ISREG(theRes.st_mode)) {
                return (execpath + string(": not a file"));
            }
        }
        if (::signal(SIGALRM, &Impl::SigAlrmHandler) == SIG_ERR) {
            QCUtils::FatalError("signal(SIGALRM)", errno);
        }
        if (mMaxGracefulRestartSeconds > 0) {
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
private:
    char*  mCwd;
    char** mArgs;
    char** mEnv;
    int    mMaxGracefulRestartSeconds;
    bool   mExitOnRestartFlag;

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
#ifdef KFS_OS_NAME_LINUX
        ::clearenv();
#else
        environ = 0;
#endif
        if (mEnv) {
            for (char** thePtr = mEnv; *thePtr; thePtr++) {
                if (::putenv(*thePtr)) {
                    QCUtils::FatalError("putenv", errno);
                }
            }
        }
        if (::chdir(mCwd) != 0) {
            QCUtils::FatalError(mCwd, errno);
        }
        int theMaxFds = 16 << 10;
        struct rlimit theLimt = { 0 };
        if (0 == getrlimit(RLIMIT_NOFILE, &theLimt)) {
            if (0 < theLimt.rlim_cur) {
                theMaxFds = (int)theLimt.rlim_cur;
            }
        }
        for (int i = 3; i < theMaxFds; i++) {
            close(i);
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

ProcessRestarter::ProcessRestarter()
    : mImpl(*(new Impl()))
{}

ProcessRestarter::~ProcessRestarter()
{
    delete &mImpl;
}

    bool
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

} // namespace KFS
