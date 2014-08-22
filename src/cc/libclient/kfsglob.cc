//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/18
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \brief Kfs glob() equivalent.
//
//----------------------------------------------------------------------------

#include "kfsglob.h"

#include "KfsClient.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <string.h>
#include <stdlib.h>
#include <glob.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>

#include <string>
#include <vector>
#include <algorithm>

// Enabling thread local assumes that the libc glob() implementation is
// re-entrant.
#if ! defined(KFS_GLOB_USE_THREAD_LOCAL) && defined(__GNUC__) && \
        (! defined(QC_OS_NAME_DARWIN) || ((defined(__SIZEOF_POINTER__) && \
        __SIZEOF_POINTER__ == 8)))
#   define KFS_GLOB_USE_THREAD_LOCAL
#endif

namespace KFS
{
namespace client
{
using std::vector;
using std::string;
using std::find;

class KfsOpenDir
{
private:
    typedef struct stat StatBuf;
public:
    class Glob : private QCRefCountedObj
    {
    public:
        static int Expand(
            KfsClient&  inClient,
            const char* inGlobPtr,
            int         inGlobFlags,
            int (*inErrorHandlerPtr)(const char* inErrPathPtr, int inError),
            glob_t*     inResultPtr)
        {
            if (! inResultPtr) {
                errno = EFAULT;
                return GLOB_ABORTED;
            }
            if ((inGlobFlags & GLOB_ALTDIRFUNC) != 0) {
                errno = EINVAL;
                return GLOB_ABORTED;
            }

            Glob&           theInstance = Instance();
            QCStMutexLocker theLock(GetMutexPtr());
            StRef           theRef(theInstance);
            // The mutex doesn't exists or cannot be deleted when "theRef"
            // destructor is invoked as the condition in the following
            // assertion shows, therefore no problem exists with "theLock"
            // destructor releasing mutex after de-referencing.
            // Such order of constructors and destructors ensures the
            // reference counter access serialization with thread local
            // disabled.
            // With thread local disabled the instance must be constructed
            // prior to entering this method, therefore this method must not
            // be invoked before entering the "main()", otherwise the following
            // assertion will fail.
            // The assertion assumes no recursion, as GLOB_ALTDIRFUNC is not
            // allowed.
            QCRTASSERT(theInstance.GetRefCount() == (GetMutexPtr() ? 2 : 1));
            return theInstance.ExpandSelf(inClient, inGlobPtr, inGlobFlags,
                inErrorHandlerPtr, inResultPtr);
        }
    private:
        Glob()
            : mClientPtr(0),
              mDirToReusePtr(0),
              mOpenDirs(),
              mError(0),
              mCwd(),
              mTmpName()
        {
            // Insure that the mutex constructor is invoked.
            GetMutexPtr();
        }
        virtual ~Glob()
        {
            QCStMutexLocker theLock(GetMutexPtr());
            if (this == sInstancePtr) {
                sInstancePtr = 0;
            }
            QCASSERT(mOpenDirs.empty());
            Reset();
            delete mDirToReusePtr;
        }
        static Glob& Instance()
        {
            if (! sInstancePtr) {
                sInstancePtr = new Glob();
            }
            return *sInstancePtr;
        }
        bool IsMutexOwner() const
        {
            QCMutex* const theMutexPtr = GetMutexPtr();
            return (! theMutexPtr || theMutexPtr->IsOwned());
        }
        int ExpandSelf(
            KfsClient&  inClient,
            const char* inGlobPtr,
            int         inGlobFlags,
            int (*inErrorHandlerPtr)(const char* inErrPathPtr, int inError),
            glob_t*     inResultPtr)
        {
            QCASSERT(IsMutexOwner() && mOpenDirs.empty());
            Reset();

            mClientPtr = &inClient;
            mCwd = mClientPtr->GetCwd();
            if (*mCwd.rbegin() != '/') {
                mCwd += "/";
            }
            inResultPtr->gl_closedir = &Glob::CloseDir;
            inResultPtr->gl_readdir  = &Glob::ReadDir;
            inResultPtr->gl_opendir  = &Glob::OpenDir;
            inResultPtr->gl_lstat    = &Glob::LStat;
            inResultPtr->gl_stat     = &Glob::Stat;
            return glob(inGlobPtr, inGlobFlags | GLOB_ALTDIRFUNC,
                inErrorHandlerPtr, inResultPtr);
        }
        static void* OpenDir(
            const char* inPathNamePtr)
            { return Instance().OpenDirSelf(inPathNamePtr); }
        static void CloseDir(
            void* inDirPtr)
            { Instance().CloseDirSelf(inDirPtr); }
        static struct dirent* ReadDir(
            void* inDirPtr)
            { return Instance().ReadDirSelf(inDirPtr); }
        static int Stat(
            const char* inPathNamePtr,
            StatBuf*    inStatPtr)
            { return Instance().StatSelf(inPathNamePtr, inStatPtr); }
        // For now same as stat as sym links aren't supported.
        static int LStat(
            const char* inPathNamePtr,
            StatBuf*    inStatPtr)
            { return Instance().StatSelf(inPathNamePtr, inStatPtr); }
        void* OpenDirSelf(
            const char* inPathNamePtr)
        {
            QCASSERT(mClientPtr && IsMutexOwner());

            if (! inPathNamePtr || ! *inPathNamePtr) {
                errno = EINVAL;
                return 0;
            }
            // Use absolute path names as concurrent threads can change
            // client's current working directory.
            const char* const theDirNamePtr = GetAbsPathName(inPathNamePtr);
            KfsOpenDir& theDir = mDirToReusePtr ?
                mDirToReusePtr->Clear() : *(new KfsOpenDir());
            mDirToReusePtr = 0;
            const bool kComputeFileSizeFlag   = false;
            const bool kUpdateClientCacheFlag = true; // Cache dirs i-nodes.
            const bool kFileIdAndTypeOnlyFalg = true;
            mError = mClientPtr->ReaddirPlus(
                theDirNamePtr,
                theDir.mDirContent,
                kComputeFileSizeFlag,
                kUpdateClientCacheFlag,
                kFileIdAndTypeOnlyFalg
            );
            if (mError != 0) {
                mDirToReusePtr = &(theDir.Clear());
                errno = mError < 0 ? -mError : mError;
                return 0;
            }
            theDir.Reset(theDirNamePtr);
            if (mOpenDirs.capacity() == 0) {
                mOpenDirs.reserve(16);
            }
            mOpenDirs.push_back(&theDir);
            return &theDir;
        }
        void CloseDirSelf(
            void* inDirPtr)
        {
            QCASSERT(mClientPtr && IsMutexOwner() && ! mOpenDirs.empty());

            if (! inDirPtr) {
                return;
            }
            KfsOpenDir* const theDirPtr = reinterpret_cast<KfsOpenDir*>(inDirPtr);
            if (theDirPtr == mOpenDirs.back()) {
                mOpenDirs.pop_back();
            } else {
                OpenDirs::iterator const theIt = find(
                    mOpenDirs.begin(), mOpenDirs.end(), theDirPtr);
                if (theIt == mOpenDirs.end()) {
                    QCRTASSERT(! "invalid CloseDir argument");
                }
                mOpenDirs.erase(theIt);
            }
            if (mDirToReusePtr) {
                delete theDirPtr;
            } else {
                mDirToReusePtr = &(theDirPtr->Clear());
            }
        }
        struct dirent* ReadDirSelf(
            void* inDirPtr)
        {
            QCASSERT(mClientPtr && IsMutexOwner());

            if (! inDirPtr) {
                return 0;
            }
            return reinterpret_cast<KfsOpenDir*>(inDirPtr)->ReadDirSelf();
        }
        int StatSelf(
            const char* inPathNamePtr,
            StatBuf*    inStatBufPtr)
        {
            QCASSERT(mClientPtr && IsMutexOwner());

            if (! inPathNamePtr || ! *inPathNamePtr || ! inStatBufPtr) {
                errno = EINVAL;
                return -1;
            }
            const char* const theAbsPathNamePtr = GetAbsPathName(inPathNamePtr);
            if (mOpenDirs.empty() || ! mOpenDirs.back()->Stat(
                    theAbsPathNamePtr, *inStatBufPtr)) {
                KfsFileAttr theAttr;
                const bool  kComputeFileSizesFlag = false;
                mError = mClientPtr->Stat(
                    theAbsPathNamePtr, theAttr, kComputeFileSizesFlag);
                if (mError != 0) {
                    errno = mError < 0 ? -mError : mError;
                    return -1;
                }
                theAttr.ToStat(*inStatBufPtr);
            }
            return 0;
        }
    private:
        typedef vector<KfsOpenDir*> OpenDirs;

        KfsClient*  mClientPtr;
        KfsOpenDir* mDirToReusePtr;
        OpenDirs    mOpenDirs;
        int         mError;
        string      mCwd;
        string      mTmpName;

#ifdef KFS_GLOB_USE_THREAD_LOCAL
        static __thread Glob* sInstancePtr;
        static QCMutex* GetMutexPtr()
            { return 0; }
#else
        static Glob* sInstancePtr;
        static StRef sInstanceRef;
        static QCMutex* GetMutexPtr()
        {
            static QCMutex sMutex;
            return &sMutex;
        }
#endif
        void Reset()
        {
            for (OpenDirs::const_iterator theIt = mOpenDirs.begin();
                    theIt != mOpenDirs.end();
                    ++theIt) {
                delete *theIt;
            }
            mOpenDirs.clear();
            mError = 0;
            mCwd.clear();
            mTmpName.clear();
            mClientPtr = 0;
        }
        const char* GetAbsPathName(
            const char* inPathNamePtr)
        {
            const char* theAbsPathNamePtr = inPathNamePtr;
            if (*theAbsPathNamePtr != '/') {
                mTmpName.reserve(mCwd.length() + 1024);
                mTmpName = mCwd;
                mTmpName += theAbsPathNamePtr;
                theAbsPathNamePtr = mTmpName.c_str();
            }
            return theAbsPathNamePtr;
        }
    private:
        Glob(const Glob& inGlob);
        Glob& operator=(const Glob& inGlob);
    };
    friend class Glob;
private:
    typedef vector<KfsFileAttr> DirConent;
    class DirEntry : public dirent
    {
    public:
        DirEntry()
            : dirent()
            { d_name[0] = 0; }
    private:
        DirEntry(const DirEntry& inDirEntry);
        DirEntry& operator=(const DirEntry& inDirEntry);
    };

    DirConent                 mDirContent;
    DirEntry                  mCurEntry;
    const KfsFileAttr*        mCurPtr;
    DirConent::const_iterator mNextIt;
    string                    mDirName;

    KfsOpenDir()
        : mDirContent(),
          mCurPtr(0),
          mNextIt(mDirContent.begin()),
          mDirName()
    {
        mDirContent.reserve(256);
        mNextIt = mDirContent.begin();
        mDirName.reserve(1024);
    }
    void Reset(
        const char* inDirNamePtr)
    {
        mNextIt  = mDirContent.begin();
        mDirName = inDirNamePtr;
        mCurPtr  = 0;
        if (mDirName.empty()) {
            mDirName = "./";
        } else if (*mDirName.rbegin() != '/') {
            mDirName += "/";
        }
    }
    KfsOpenDir& Clear()
    {
        mDirName.clear();
        mDirContent.clear();
        mNextIt  = mDirContent.begin();
        mCurPtr  = 0;
        return *this;
    }
    DirEntry* ReadDirSelf()
    {
        for (; ;) {
            if (mNextIt == mDirContent.end()) {
                mCurPtr = 0;
                return 0;
            }
            mCurPtr = &(*mNextIt);
            ++mNextIt;
            const size_t theLen  = mCurPtr->filename.length() + 1;
            const size_t kMaxLen = sizeof(mCurEntry.d_name);
            if (theLen > kMaxLen) {
                // Skip / hide entries that exceed max length for now.
                // Other way to handle this is to scan all entries in OpenDir or
                // modify ReadDirplus to do this, and return ENAMETOOLONG.
                // Not adequate name length is not expected be a problem with
                // the modern os / libc / glibc. There seems to be no reliable
                // way to let glob know about the problem at this exact point.
                continue;
            }
            memcpy(mCurEntry.d_name, mCurPtr->filename.c_str(), theLen);
            mCurEntry.d_ino = (ino_t)mCurPtr->fileId;
            if (mCurEntry.d_ino == 0) {
                // I-node must be non 0 for glob to treat as directory entry.
                mCurEntry.d_ino = 1;
            }
#ifndef QC_OS_NAME_CYGWIN
            // Cygwin has no d_type, all other supported platforms have the file
            // type, and glob uses this field instead of invoking stat.
            mCurEntry.d_type = mCurPtr->isDirectory ? DT_DIR : DT_REG;
#endif
            break;
        }
        return &mCurEntry;
    }
    bool Stat(
        const char* inPathNamePtr,
        StatBuf&    outStatBuf)
    {
        if (! mCurPtr) {
            return false;
        }
        const size_t theLen = mDirName.length();
        if (mDirName.compare(0, theLen, inPathNamePtr) != 0 ||
                mCurPtr->filename.compare(inPathNamePtr + theLen) != 0) {
            return false;
        }
        mCurPtr->ToStat(outStatBuf);
        return true;
    }
private:
    KfsOpenDir(
        const KfsOpenDir& inOpenDir);
    KfsOpenDir& operator=(
        const KfsOpenDir& inOpenDir);
};
#ifdef KFS_GLOB_USE_THREAD_LOCAL
__thread KfsOpenDir::Glob* KfsOpenDir::Glob::sInstancePtr = 0;
#else
KfsOpenDir::Glob* KfsOpenDir::Glob::sInstancePtr = 0;
KfsOpenDir::Glob::StRef KfsOpenDir::Glob::sInstanceRef(
    KfsOpenDir::Glob::Instance());
#endif

} // namespace client

int
KfsGlob(
    KfsClient&  inClient,
    const char* inGlobPtr,
    int         inGlobFlags,
    int (*inErrorHandlerPtr)(const char* inErrPathPtr, int inError),
    glob_t*     inResultPtr)
{
    return client::KfsOpenDir::Glob::Expand(
        inClient, inGlobPtr, inGlobFlags, inErrorHandlerPtr, inResultPtr);
}

}
