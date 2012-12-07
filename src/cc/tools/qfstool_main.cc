//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/08/20
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
// \brief Kfs tool.
//
//----------------------------------------------------------------------------

#include "FileSystem.h"
#include "common/MsgLogger.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include <string>
#include <vector>
#include <deque>
#include <map>
#include <ostream>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <iomanip>

namespace KFS
{
namespace tools
{

using std::string;
using std::cin;
using std::cout;
using std::cerr;
using std::vector;
using std::deque;
using std::map;
using std::pair;
using std::max;
using std::ostream;
using std::ostringstream;
using std::setw;
using std::left;
using std::right;
using std::make_pair;

const string kDefaultCreateParams("S"); // RS 6+3 64K stripe

class KfsTool
{
public:
    KfsTool()
        : mIoBufferSize(6 << 20),
          mIoBufferPtr(new char[mIoBufferSize])
        {}
    ~KfsTool()
    {
        delete [] mIoBufferPtr;
    }
    int Run(
        int    inArgCount,
        char** inArgsPtr)
    {
        MsgLogger::LogLevel theLogLevel = MsgLogger::kLogLevelINFO;

        int theArgIndex = 1;
        if (theArgIndex < inArgCount &&
                strcmp(inArgsPtr[theArgIndex], "-v") == 0) {
            theLogLevel = MsgLogger::kLogLevelDEBUG;
            theArgIndex++;
        }
        if (inArgCount <= theArgIndex) {
            ShortHelp(cerr);
            return 1;
        }
        MsgLogger::Init(0, theLogLevel);
        if (strcmp(inArgsPtr[theArgIndex], "-fs") == 0) {
            theArgIndex++;
            if (inArgCount <= theArgIndex) {
                ShortHelp(cerr);
                return 1;
            }
            const string theUri(inArgsPtr[theArgIndex]);
            const int theErr = FileSystem::SetDefault(theUri);
            if (theErr != 0) {
                cerr << theUri << ": " <<
                    FileSystem::GetStrError(theErr) << "\n";
                return 1;
            }
            theArgIndex++;
        }
        if (inArgCount <= theArgIndex) {
            ShortHelp(cerr);
            return 1;
        }
        const char* const theCmdPtr  = inArgsPtr[theArgIndex++] + 1;
        if (theCmdPtr[-1] != '-') {
            ShortHelp(cerr);
            return 1;
        }
        int       theArgCnt    = inArgCount - theArgIndex;
        char**    theArgsPtr   = inArgsPtr + theArgIndex;
        int       theErr       = 0;
        bool      theChgrpFlag = false;
        bool      theMoveFlag  = false;
        if (strcmp(theCmdPtr, "cat") == 0) {
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                theErr = Cat(theArgsPtr, theArgCnt);
            }
        } else if (strcmp(theCmdPtr, "ls") == 0) {
            const bool kRecursiveFlag = false;
            theErr = List(theArgsPtr, theArgCnt, kRecursiveFlag);
        } else if (strcmp(theCmdPtr, "lsr") == 0) {
            const bool kRecursiveFlag = true;
            theErr = List(theArgsPtr, theArgCnt, kRecursiveFlag);
        } else if (strcmp(theCmdPtr, "mkdir") == 0) {
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                const kfsMode_t kCreateMode    = 0777;
                const bool      kCreateAllFlag = true;
                theErr = Mkdir(
                    theArgsPtr, theArgCnt,
                    kCreateMode, kCreateAllFlag);
            }
        } else if (strcmp(theCmdPtr, "cp") == 0) {
            theErr = Copy(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "put") == 0 ||
                strcmp(theCmdPtr, "copyFromLocal") == 0 ||
                (theMoveFlag = strcmp(theCmdPtr, "moveFromLocal") == 0)) {
            if (theArgCnt == 2 && strcmp(theArgsPtr[0], "-") == 0) {
                theErr = CopyFromStream(theArgsPtr[1], cin, cerr);
            } else {
                if (theArgCnt < 2) {
                    theErr = EINVAL;
                    ShortHelp(cerr, "Usage: ", theCmdPtr);
                } else {
                    theErr = CopyFromLocal(theArgsPtr, theArgCnt, theMoveFlag,
                        cerr);
                }
            }
        } else if (strcmp(theCmdPtr, "get") == 0 ||
                strcmp(theCmdPtr, "copyToLocal") == 0) {
            if (theArgCnt >=2 && strcmp(theArgsPtr[theArgCnt - 1], "-") == 0) {
                theErr = Cat(theArgsPtr, theArgCnt - 1);
            } else {
                theErr = CopyToLocal(theArgsPtr, theArgCnt, theMoveFlag, cerr);
            }
        } else if (strcmp(theCmdPtr, "mv") == 0) {
            theErr = Move(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "du") == 0) {
            theErr = DiskUtilizationBytes( theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "duh") == 0) {
            theErr = DiskUtilizationHumanReadable(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "dus") == 0) {
            theErr = DiskUtilizationSummary(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "dush") == 0) {
            theErr = DiskUtilizationSummaryHumanReadable(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "count") == 0) {
            const bool theShowQuotaFlag =
                theArgCnt > 0 && strcmp(theArgsPtr[0], "-q") == 0;
            if (theShowQuotaFlag) {
                theArgCnt--;
                theArgsPtr++;
            }
            theErr = Count(theArgsPtr, theArgCnt, theShowQuotaFlag);
        } else if (strcmp(theCmdPtr, "chmod") == 0) {
            const bool theRecursiveFlag =
                theArgCnt > 0 && strcmp(theArgsPtr[0], "-R") == 0;
            if (theRecursiveFlag) {
                theArgCnt--;
                theArgsPtr++;
            }
            if (theArgCnt < 2) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                const char* const theModePtr = *theArgsPtr++;
                theArgCnt--;
                theErr = Chmod(theArgsPtr, theArgCnt, theModePtr,
                    theRecursiveFlag);
            }
        } else if (strcmp(theCmdPtr, "chown") == 0 ||
                (theChgrpFlag = strcmp(theCmdPtr, "chgrp") == 0)) {
            const bool theRecursiveFlag =
                theArgCnt > 0 && strcmp(theArgsPtr[0], "-R") == 0;
            if (theRecursiveFlag) {
                theArgCnt--;
                theArgsPtr++;
            }
            if (theArgCnt < 2) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                const char* const theOwnerGroupPtr = *theArgsPtr++;
                theArgCnt--;
                const char* theUserNamePtr  = 0;
                const char* theGroupNamePtr = 0;
                string      theUserGroupName;
                if (theChgrpFlag) {
                    theGroupNamePtr = theOwnerGroupPtr;
                } else {
                    const char* thePtr = strchr(theOwnerGroupPtr, ':');
                    if (thePtr) {
                        theUserGroupName = theOwnerGroupPtr;
                        const size_t theLen = thePtr - theOwnerGroupPtr;
                        theUserGroupName[theLen] = 0;
                        theUserNamePtr  = theUserGroupName.data();
                        theGroupNamePtr = theUserNamePtr + theLen + 1;
                    } else {
                        theUserNamePtr = theOwnerGroupPtr;
                    }
                }
                theErr = Chown(theArgsPtr, theArgCnt,
                    theUserNamePtr, theGroupNamePtr, theRecursiveFlag);
            }
        } else if (strcmp(theCmdPtr, "touchz") == 0) {
            theErr = Touchz(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "setModTime") == 0) {
            if (theArgCnt != 2) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                char*         theEndPtr  = 0;
                const int64_t theMTimeMs =
                    strtoll(theArgsPtr[1], &theEndPtr, 0);
                if (! theEndPtr || (*theEndPtr & 0xFF) > ' ') {
                    theErr = EINVAL;
                    ShortHelp(cerr, "Usage: ", theCmdPtr);
                } else {
                    theErr = SetModTime(theArgsPtr, 1, theMTimeMs);
                }
            }
        } else if (strcmp(theCmdPtr, "test") == 0) {
            theErr = Test(theArgsPtr, theArgCnt);
        } else if (strcmp(theCmdPtr, "setrep") == 0) {
            bool theRecursiveFlag = false;
            bool theWaitFlag      = false;
            int  theIdx           = 0;
            while (theIdx < theArgCnt) {
                if (! theRecursiveFlag &&
                        strcmp(theArgsPtr[theIdx], "-R") == 0) {
                    theIdx++;
                    theRecursiveFlag = true;
                } else if (! theWaitFlag &&
                        strcmp(theArgsPtr[theIdx], "-w") == 0) {
                    theIdx++;
                    theWaitFlag = true;
                } else {
                    break;
                }
            }
            if (theIdx + 1 >= theArgCnt) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                char* theEndPtr = 0;
                const int theReplication =
                    (int)strtol(theArgsPtr[theIdx++], &theEndPtr, 0);
                if (! theEndPtr || *theEndPtr > ' ' || theReplication <= 0) {
                    theErr = EINVAL;
                    ShortHelp(cerr, "Usage: ", theCmdPtr);
                } else {
                    theErr = SetReplication(
                        theArgsPtr + theIdx,
                        theArgCnt  - theIdx,
                        theReplication,
                        theRecursiveFlag,
                        theWaitFlag
                    );
                }
            }
        } else if (strcmp(theCmdPtr, "help") == 0) {
            theErr = LongHelp(cout, theArgsPtr, theArgCnt);
        } else {
            cerr << "unsupported option: " << (theCmdPtr - 1) << "\n";
            theErr = EINVAL;
            ShortHelp(cerr);
        }
        return (theErr == 0 ? 0 : 1);
    }
private:
    static const char* const sHelpStrings[];
    void ShortHelp(
        ostream&    inOutStream,
        const char* inPrefixPtr  = "          ",
        const char* inCmdNamePtr = 0)
    {
        for (const char* const* thePtr = sHelpStrings; *thePtr; ) {
            if (inCmdNamePtr && strcmp(inCmdNamePtr, *thePtr) != 0) {
                thePtr += 3;
                continue;
            }
            inOutStream << inPrefixPtr << "[-" << *thePtr << " ";
            ++thePtr;
            inOutStream << *thePtr << "]\n";
            thePtr += 2;
        }
    }
    int LongHelp(
        ostream& inOutStream,
        char**   inArgsPtr,
        int      inArgCount)
    {
        if (inArgCount <= 0) {
            ShortHelp(inOutStream, "\t");
        }
        int theRemCnt = inArgCount;
        for (const char* const* thePtr = sHelpStrings; *thePtr; ) {
            int i = 0;
            for (i = 0; i < inArgCount && strcmp(*thePtr, inArgsPtr[i]); i++)
                {}
            if (i >= inArgCount && inArgCount > 0) {
                thePtr += 3;
                continue;
            }
            theRemCnt--;
            inOutStream << "-" << *thePtr << " ";
            ++thePtr;
            inOutStream << *thePtr << ":\t";
            ++thePtr;
            inOutStream << *thePtr << "\n";
            ++thePtr;
            if (inArgCount > 0 && theRemCnt == 0) {
                break;
            }
        }
        if (theRemCnt > 0) {
            LongHelp(inOutStream, 0, 0);
            return -EINVAL;
        }
        return 0;
    }
    static const char* GlobError(
        int inError)
    {
        switch (inError) {
            case GLOB_NOSPACE:
                return "out of memory";
            case GLOB_ABORTED:
                return "read error";
            case GLOB_NOMATCH:
                return "no matches found";
            case 0:
                return "no error";
            default:
                return "unspecified error";
        }
    }
    typedef vector<pair<FileSystem*, vector<string> > > GlobResult;
    static int Glob(
        char**       inArgsPtr,
        int          inArgCount,
        ostream&     inErrorStream,
        GlobResult&  outResult,
        bool&        outMoreThanOneFsFlag)
    {
        outResult.reserve(outResult.size() + max(0, inArgCount));
        int theRet = 0;
        outMoreThanOneFsFlag = false;
        for (int i = 0; i < inArgCount; i++) {
            const string theArg   = inArgsPtr[i];
            FileSystem*  theFsPtr = 0;
            string       thePath;
            int          theErr   = FileSystem::Get(theArg, theFsPtr, &thePath);
            if (theErr || ! theFsPtr) {
                inErrorStream << theArg <<
                    ": " << FileSystem::GetStrError(theErr) << "\n";
                theRet = theErr;
                continue;
            }
            outMoreThanOneFsFlag = outMoreThanOneFsFlag ||
                (! outResult.empty() && *theFsPtr != *(outResult.back().first));
            glob_t    theGlobRes = {0};
            const int kGlobFlags = GLOB_NOSORT | GLOB_NOCHECK;
            theErr = theFsPtr->Glob(
                thePath,
                kGlobFlags,
                0, // the err func.
                &theGlobRes
            );
            if (theErr == 0) {
                outResult.resize(outResult.size() + 1);
                outResult.back().first = theFsPtr;
                string thePrefix;
                if (thePath.empty() || thePath[0] != '/') {
                    string theCwd;
                    if ((theErr = theFsPtr->GetCwd(theCwd))) {
                        inErrorStream << theArg <<
                            ": " << theFsPtr->StrError(theErr) << "\n";
                        globfree(&theGlobRes);
                        theRet = theErr;
                        continue;
                    }
                    thePrefix += theCwd;
                    if (! thePrefix.empty() && *thePrefix.rbegin() != '/' &&
                            (theGlobRes.gl_pathc > 1 ||
                            (theGlobRes.gl_pathc == 1 &&
                            theGlobRes.gl_pathv[0][0] != 0))) {
                        thePrefix += "/";
                    }
                }
                GlobResult::value_type::second_type&
                    theResult = outResult.back().second;
                theResult.reserve(theGlobRes.gl_pathc);
                for (size_t i = 0; i < theGlobRes.gl_pathc; i++) {
                    theResult.push_back(thePrefix + theGlobRes.gl_pathv[i]);
                }
            } else {
                inErrorStream << inArgsPtr[i] << ": " << GlobError(theErr) <<
                    " " << theErr << "\n";
            }
            globfree(&theGlobRes);
            if (theErr != 0) {
                theRet = theErr;
            }
        }
        return theRet;
    }
    template <typename FuncT> int Apply(
        char** inArgsPtr,
        int    inArgCount,
        FuncT& inFunctor)
    {
        char  theArg[1]     = { 0 };
        char* theArgsPtr[1] = { theArg };
        GlobResult theResult;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(
            inArgCount <= 0 ? theArgsPtr : inArgsPtr,
            inArgCount <= 0 ? 1 : inArgCount,
            cerr,
            theResult, theMoreThanOneFsFlag);
        return Apply(theResult, theMoreThanOneFsFlag, theErr, inFunctor);
    }
    template <typename FuncT> int Apply(
        GlobResult& inGlobResult,
        bool        inMoreThanOneFsFlag,
        int         inGlobError,
        FuncT&      inFunctor)
    {
        int theGlobError = inGlobError;
        if (! inFunctor.Init(theGlobError, inGlobResult, inMoreThanOneFsFlag)) {
            return theGlobError;
        }
        for (GlobResult::const_iterator theFsIt = inGlobResult.begin();
                theFsIt != inGlobResult.end();
                ++theFsIt) {
            FileSystem& theFs = *(theFsIt->first);
            for (GlobResult::value_type::second_type::const_iterator
                    theIt = theFsIt->second.begin();
                    theIt != theFsIt->second.end();
                    ++theIt) {
                if (! inFunctor.Apply(theFs, *theIt)) {
                    return inFunctor.GetStatus();
                }
            }
        }
        const int theStatus = inFunctor.GetStatus();
        return (theStatus != 0 ? theStatus : theGlobError);
    }
    class CatFunctor
    {
    public:
        CatFunctor(
            ostream&    inOutStream,
            const char* inOutStreamNamePtr,
            ostream&    inErrorStream,
            size_t      inIoBufferSize,
            char*       inIoBufferPtr)
            : mOutStream(inOutStream),
              mOutStreamNamePtr(inOutStreamNamePtr ? inOutStreamNamePtr : ""),
              mErrorStream(inErrorStream),
              mIoBufferSize(inIoBufferSize),
              mIoBufferPtr(inIoBufferPtr),
              mStatus(0)
            {}
        bool Init(
            int&              /* ioGlobError */,
            const GlobResult& /* inGlobResult */,
            bool              /* inMoreThanOneFsFlag */)
            { return true; }
        bool Apply(
            FileSystem&   inFs,
            const string& inPath)
        {
            if (! mOutStream) {
                return false;
            }
            const int theFd = inFs.Open(inPath, O_RDONLY, 0);
            if (theFd < 0) {
                mErrorStream << inFs.GetUri() << inPath <<
                    ": " << inFs.StrError(theFd) << "\n";
                mStatus = theFd;
                return true;
            }
            for (; ;) {
                const ssize_t theNRead =
                    inFs.Read(theFd, mIoBufferPtr, mIoBufferSize);
                if (theNRead == 0) {
                    break;
                }
                if (theNRead < 0) {
                    mStatus = (int)theNRead;
                    mErrorStream << inFs.GetUri() << inPath <<
                        ": " << inFs.StrError(mStatus) << "\n";
                    break;
                }
                if (! mOutStream.write(mIoBufferPtr, theNRead)) {
                    mStatus = errno;
                    mErrorStream << inFs.GetUri() << inPath <<
                        ": " << mOutStreamNamePtr <<
                        QCUtils::SysError(mStatus) << "\n";
                    break;
                }
            }
            inFs.Close(theFd);
            return true;
        }
        int GetStatus() const
            { return mStatus; }
    private:
        ostream&          mOutStream;
        const char* const mOutStreamNamePtr;
        ostream&          mErrorStream;
        const size_t      mIoBufferSize;
        char* const       mIoBufferPtr;
        int               mStatus;
    private:
        CatFunctor(
            const CatFunctor& inFunctor);
        CatFunctor& operator=(
            const CatFunctor& inFunctor);
    };
    int Cat(
        char** inArgsPtr,
        int    inArgCount)
    {
        CatFunctor theFunc(cout, "stdout", cerr, mIoBufferSize, mIoBufferPtr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class ListFunctor
    {
    public:
        ListFunctor(
            ostream&    inOutStream,
            const char* inOutStreamNamePtr,
            ostream&    inErrorStream,
            bool        inRecursiveFlag)
            : mOutStream(inOutStream),
              mOutStreamNamePtr(inOutStreamNamePtr ? inOutStreamNamePtr : ""),
              mErrorStream(inErrorStream),
              mRecursiveFlag(inRecursiveFlag),
              mShowFsUriFlag(false),
              mEmptyStr(),
              mStat(),
              mStatus(0),
              mOwnerId(kKfsUserNone),
              mGroupId(kKfsGroupNone),
              mOwner("-"),
              mGroup("-"),
              mRecursionCount(0),
              mMaxOwnerWidth(1),
              mMaxGroupWidth(1),
              mFileSizeWidth(1),
              mReplicasWidth(1),
              mMaxReplicas(0),
              mMaxFileSize(0),
              mDirListEntries(),
              mNullStat(),
              mTime(0)
            { mTmBuf[0] = 0; }
        bool Init(
            int&              /* ioGlobError */,
            const GlobResult& /* inGlobResult */,
            bool              inMoreThanOneFsFlag)
        {
            mShowFsUriFlag = inMoreThanOneFsFlag;
            return true;
        }
        bool Apply(
            FileSystem&   inFs,
            const string& inPath)
        {
            if (! mOutStream) {
                return false;
            }
            int theErr;
            if (mRecursionCount == 0) {
                theErr = inFs.Stat(inPath, mStat);
                if (theErr != 0) {
                    mErrorStream << inFs.GetUri() << inPath <<
                        ": " << inFs.StrError(theErr) << "\n";
                    mStatus = theErr;
                    return true;
                }
                Reset();
            }
            const string  kEmpty;
            if (mRecursionCount != 0 || S_ISDIR(mStat.st_mode)) {
                FileSystem::DirIterator* theItPtr = 0;
                const bool kFetchAttributesFlag = true;
                if ((theErr = inFs.Open(
                        inPath, kFetchAttributesFlag, theItPtr))) {
                    mErrorStream << inFs.GetUri() << inPath <<
                        ": " << inFs.StrError(theErr) << "\n";
                    mStatus = theErr;
                } else {
                    string theName;
                    const string& thePath = inPath == "/" ? mEmptyStr: inPath;
                    while (mOutStream) {
                        const FileSystem::StatBuf* theStatPtr = 0;
                        if ((theErr = inFs.Next(
                                theItPtr, theName, theStatPtr))) {
                            mErrorStream << inFs.GetUri() << thePath <<
                                "/" << theName << ": " <<
                                inFs.StrError(theErr) << "\n";
                            mStatus = theErr;
                        }
                        if (theName.empty()) {
                            break;
                        }
                        if (theName == "." || theName == "..") {
                            continue;
                        }
                        if (mRecursiveFlag && theStatPtr &&
                                S_ISDIR(theStatPtr->st_mode)) {
                            QCStValueIncrementor<int>
                                theIncrement(mRecursionCount, 1);
                            Apply(inFs, thePath + "/" + theName);
                        }
                        AddEntry(inFs, inPath, theName,
                            theStatPtr ? *theStatPtr : mNullStat);
                    }
                    inFs.Close(theItPtr);
                }
            } else {
                AddEntry(inFs, inPath, string(), mStat);
            }
            if (mRecursionCount == 0 &&
                    ! mRecursiveFlag && ! mDirListEntries.empty()) {
                mOutStream <<
                    "Found " << mDirListEntries.size() << " items\n";
            }
            if (mRecursionCount == 0 ||
                    mDirListEntries.size() > (size_t(32) << 10)) {
                ostringstream theStream;
                theStream << mMaxFileSize;
                mFileSizeWidth = theStream.str().length();
                theStream << mMaxReplicas;
                mReplicasWidth = theStream.str().length() - mFileSizeWidth;
                for (DirListEntries::const_iterator
                        theIt = mDirListEntries.begin();
                        theIt != mDirListEntries.end() && mOutStream;
                        ++theIt) {
                    Show(inFs, *theIt);
                }
                Reset();
            }
            return true;
        }
        int GetStatus() const
            { return mStatus; }
    private:
        struct DirListEntry
        {
            mode_t  mMode;
            int     mNumReplicas;
            string  mOwner;
            string  mGroup;
            int64_t mSize;
            time_t  mMTime;
            string  mPath;
            string  mName;
            void Set(
                const FileSystem::StatBuf& inStat)
            {
                mMode        = inStat.st_mode;
                mNumReplicas = S_ISDIR(inStat.st_mode) ?
                    0 : max(1, (int)inStat.mNumReplicas);
                mSize        = max(int64_t(0), (int64_t)inStat.st_size);
#ifndef KFS_OS_NAME_DARWIN
                mMTime = inStat.st_mtime;
#else
                mMTime = inStat.st_mtimespec.tv_sec;
#endif
            }
        };
        typedef deque<DirListEntry> DirListEntries;

        enum { kTmBufLen = 128 };
        ostream&                  mOutStream;
        const char* const         mOutStreamNamePtr;
        ostream&                  mErrorStream;
        const bool                mRecursiveFlag;
        bool                      mShowFsUriFlag;
        const string              mEmptyStr;
        FileSystem::StatBuf       mStat;
        int                       mStatus;
        kfsUid_t                  mOwnerId;
        kfsGid_t                  mGroupId;
        string                    mOwner;
        string                    mGroup;
        int                       mRecursionCount;
        size_t                    mMaxOwnerWidth;
        size_t                    mMaxGroupWidth;
        size_t                    mFileSizeWidth;
        size_t                    mReplicasWidth;
        int                       mMaxReplicas;
        int64_t                   mMaxFileSize;
        DirListEntries            mDirListEntries;
        const FileSystem::StatBuf mNullStat;
        time_t                    mTime;
        char                      mTmBuf[kTmBufLen];

        void Reset()
        {
            mDirListEntries.clear();
            mMaxOwnerWidth = 1;
            mMaxGroupWidth = 1;
            mFileSizeWidth = 1;
            mReplicasWidth = 1;
            mMaxFileSize   = 0;
            mMaxReplicas   = 0;
        }
        void AddEntry(
            FileSystem&                inFs,
            const string&              inPath,
            const string&              inName,
            const FileSystem::StatBuf& inStat)
        {
            mDirListEntries.push_back(DirListEntry());
            DirListEntry& theEntry = mDirListEntries.back();
            theEntry.Set(inStat);
            theEntry.mPath = inPath;
            theEntry.mName = inName;
            if (mOwnerId != inStat.st_uid || mOwner.empty() ||
                    mGroupId != inStat.st_gid || mGroup.empty()) {
                UpdateUserAndGroup(inFs, inStat.st_uid, inStat.st_gid);
                mOwnerId = inStat.st_uid;
                mGroupId = inStat.st_gid;
            }
            theEntry.mOwner = mOwner;
            theEntry.mGroup = mGroup;
            mMaxOwnerWidth  = max(mMaxOwnerWidth, mOwner.length());
            mMaxGroupWidth  = max(mMaxGroupWidth, mGroup.length());
            mMaxFileSize    = max(mMaxFileSize,   theEntry.mSize);
            mMaxReplicas    = max(mMaxReplicas,   theEntry.mNumReplicas);
        }
        bool IsSticky(
            mode_t inMode)
        {
            return (S_ISDIR(inMode) && (inMode & S_ISVTX) != 0);
        }
        void Show(
            FileSystem&         inFs,
            const DirListEntry& inEntry)
        {
            mOutStream << (S_ISDIR(inEntry.mMode) ? "d" : "-");
            for (int i = 8; i > 0; ) {
                const char* const kPerms[2] = {"---", "rwx"};
                const int theNBits =
                    (i == 2 && IsSticky(inEntry.mMode)) ? 2 : 3;
                for (int k = 0; k < theNBits; k++) {
                    mOutStream << kPerms[(inEntry.mMode >> i--) & 1][k];
                }
                if (theNBits < 3) {
                    mOutStream << "t";
                }
            }
            mOutStream << " " << setw((int)mReplicasWidth) << right;
            if (S_ISDIR(inEntry.mMode)) {
                mOutStream << "-";
            } else {
                mOutStream << inEntry.mNumReplicas;
            }
            mOutStream <<
                " " << setw((int)mMaxOwnerWidth) << left << inEntry.mOwner <<
                " " << setw((int)mMaxGroupWidth) << left << inEntry.mGroup <<
                " " << setw((int)mFileSizeWidth) << right << inEntry.mSize;
            if (mTmBuf[0] == 0 || mTime != inEntry.mMTime) {
                struct tm theLocalTime = {0};
                localtime_r(&inEntry.mMTime, &theLocalTime);
                strftime(mTmBuf, kTmBufLen, "%Y-%m-%d %H:%M ", &theLocalTime);
                mTime = inEntry.mMTime;
            }
            mOutStream << " " << mTmBuf;
            if (mShowFsUriFlag) {
                mOutStream << inFs.GetUri();
            }
            mOutStream << inEntry.mPath;
            if (! inEntry.mName.empty()) {
                mOutStream << "/" << inEntry.mName;
            }
            mOutStream << "\n";
        }
        void UpdateUserAndGroup(
            FileSystem& inFs,
            kfsUid_t    inUid,
            kfsGid_t    inGid)
        {
            int theErr;
            if ((inUid != kKfsUserNone || inGid != kKfsGroupNone) &&
                    (theErr = inFs.GetUserAndGroupNames(
                        inUid, inGid, mOwner, mGroup))) {
                mErrorStream << inFs.GetUri() << " userId: " << inUid <<
                    " groupId: " << inGid << " : " << inFs.StrError(theErr) <<
                "\n";
                mOwner = "?";
                mGroup = "?";
                if (mStatus == 0) {
                    mStatus = theErr;
                }
                return;
            }
            if (inUid == kKfsUserNone) {
                mOwner = "-";
            }
            if (inGid == kKfsGroupNone) {
                mGroup = "-";
            }
        }
    private:
        ListFunctor(
            const ListFunctor& inFunctor);
        ListFunctor& operator=(
            const ListFunctor& inFunctor);
    };
    int List(
        char** inArgsPtr,
        int    inArgCount,
        bool   inRecursiveFlag)
    {
        ListFunctor theFunc(cout, "stdout", cerr, inRecursiveFlag);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class ErrorReporter : public FileSystem::ErrorHandler
    {
    public:
        ErrorReporter(
            FileSystem& inFs,
            ostream&    inErrorStream,
            bool        inStopOnErrorFlag = false)
            : mFs(inFs),
              mErrorStream(inErrorStream),
              mStopOnErrorFlag(inStopOnErrorFlag),
              mStatus(0)
            {}
        virtual int operator()(
            const string& inPath,
            int           inStatus)
        {
            mErrorStream << mFs.GetUri() << inPath << ": " <<
                mFs.StrError(inStatus) << "\n";
            if (mStopOnErrorFlag) {
                mStatus = inStatus;
            }
            return (mStopOnErrorFlag ? mStatus : 0);
        }
        int operator()(
            const string& inPath,
            const char*   inMsgPtr)
        {
            mErrorStream << mFs.GetUri() << inPath << ": " <<
                (inMsgPtr ? inMsgPtr : "") << "\n";
            return 0;
        }
        int GetStatus() const
            { return mStatus; }
        ostream& GetErrorStream()
            { return mErrorStream; }
        bool GetStopOnErrorFlag() const
            { return mStopOnErrorFlag; }
    private:
        FileSystem& mFs;
        ostream&    mErrorStream;
        const bool  mStopOnErrorFlag;
        int         mStatus;
    private:
        ErrorReporter(
            const ErrorReporter& inReporter);
        ErrorReporter& operator=(
            const ErrorReporter& inReporter);
    };
    class DefaultInitFunctor
    {
    public:
        DefaultInitFunctor()
            {}
        bool operator()(
            int&              /* ioGlobError */,
            const GlobResult& /* inGlobResult */,
            ostream&          /* inErrorStream */)
            { return true; }
    private:
        DefaultInitFunctor(
            const DefaultInitFunctor& inFunct);
        DefaultInitFunctor& operator=(
            const DefaultInitFunctor& inFunct);
    };
    template<
        typename T,
        typename TInit            = DefaultInitFunctor,
        bool     TStopIfErrorFlag = false,
        bool     TReportErrorFlag = true
    > class FunctorT
    {
    public:
        FunctorT(
            T&       inFunctor,
            ostream& inErrorStream)
            : mFunctor(inFunctor),
              mInitFunctor(),
              mErrorStream(inErrorStream),
              mStatus(0)
            {}
        bool Init(
            int&        ioGlobError,
            GlobResult& inGlobResult,
            bool        /* inMoreThanOneFsFlag */)
        {
            return mInitFunctor(ioGlobError, inGlobResult, mErrorStream);
        }
        bool Apply(
            FileSystem&   inFs,
            const string& inPath)
        {
            ErrorReporter theErrorReporter(inFs, mErrorStream);
            const int     theError = mFunctor(inFs, inPath, theErrorReporter);
            if (TReportErrorFlag && theError != 0) {
                theErrorReporter(inPath, theError);
            }
            mStatus = theErrorReporter.GetStatus();
            return (! TStopIfErrorFlag || mStatus == 0);
        }
        int GetStatus() const
            { return mStatus; }
        TInit& GetInit()
            { return mInitFunctor; }
    private:
        T&       mFunctor;
        TInit    mInitFunctor;
        ostream& mErrorStream;
        int      mStatus;
    private:
        FunctorT(
            const FunctorT& inFunctor);
        FunctorT& operator=(
            const FunctorT& inFunctor);
    };
    class ChownFunctor
    {
    public:
        ChownFunctor(
            const char* inUserNamePtr,
            const char* inGroupNamePtr,
            bool        inRecursiveFlag)
            : mUserName (inUserNamePtr  ? inUserNamePtr  : ""),
              mGroupName(inGroupNamePtr ? inGroupNamePtr : ""),
              mFsToUGId(),
              mRecursiveFlag(inRecursiveFlag)
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            FsToUGId::const_iterator theIt = mFsToUGId.find(&inFs);
            if (theIt == mFsToUGId.end()) {
                kfsUid_t  theUserId  = kKfsUserNone;
                kfsGid_t  theGroupId = kKfsGroupNone;
                const int theStatus  = inFs.GetUserAndGroupIds(
                    mUserName, mGroupName, theUserId, theGroupId);
                theIt = mFsToUGId.insert(make_pair(&inFs,
                    UserAndGroup(theUserId, theGroupId, theStatus))).first;
            }
            const UserAndGroup& theUG = theIt->second;
            return (
                theUG.mStatus != 0 ?
                    theUG.mStatus :
                    inFs.Chown(inPath, theUG.mUserId, theUG.mGroupId,
                        mRecursiveFlag, &inErrorReporter)
            );
        }
    private:
        class UserAndGroup
        {
        public:
            UserAndGroup(
                kfsUid_t inUserId  = kKfsUserNone,
                kfsGid_t inGroupId = kKfsGroupNone,
                int      inStatus  = -EINVAL)
                : mUserId(inUserId),
                  mGroupId(inGroupId),
                  mStatus(inStatus)
                {}
            kfsUid_t mUserId;
            kfsGid_t mGroupId;
            int      mStatus;
        };
        typedef map<const FileSystem*, UserAndGroup> FsToUGId;

        const string mUserName;
        const string mGroupName;
        FsToUGId     mFsToUGId;
        const bool   mRecursiveFlag;

    private:
        ChownFunctor(
            const ChownFunctor& inFunctor);
        ChownFunctor& operator=(
            const ChownFunctor& inFunctor);
    };
    int Chown(
        char**      inArgsPtr,
        int         inArgCount,
        const char* inUserNamePtr,
        const char* inGroupNamePtr,
        bool        inRecursiveFlag)
    {
        ChownFunctor           theChownFunc(
            inUserNamePtr, inGroupNamePtr, inRecursiveFlag);
        FunctorT<ChownFunctor> theFunc(theChownFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class ChmodFunctor
    {
    public:
        ChmodFunctor(
            const char* inModePtr,
            bool        inRecursiveFlag)
            : mMode(inModePtr ? inModePtr : ""),
              mModeStatus(0),
              mSetModeFlag(false),
              mModeToSet(0),
              mRecursiveFlag(inRecursiveFlag),
              mStat()
        {
            if (mMode.empty()) {
                mModeStatus = -EINVAL;
                return;
            }
            char* theEndPtr = 0;
            const long theMode = strtol(mMode.c_str(), &theEndPtr, 8);
            if ((mSetModeFlag = theEndPtr && (*theEndPtr & 0xFF) <= ' ')
                    && theMode >= 0) {
                mModeToSet = (kfsMode_t)theMode & (0777 | S_ISVTX);
            } else if (GetMode(0) == kKfsModeUndef) {
                mModeStatus = -EINVAL;
            }
        }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            if (mModeStatus != 0) {
                return mModeStatus;
            }
            if (mSetModeFlag) {
                return inFs.Chmod(inPath, mModeToSet, mRecursiveFlag,
                    &inErrorReporter);
            }
            const int theStatus = inFs.Stat(inPath, mStat);
            if (theStatus != 0) {
                return theStatus;
            }
            return inFs.Chmod(inPath, GetMode(mStat.st_mode), mRecursiveFlag,
                    &inErrorReporter);
        }
        int GetModeStatus() const
            { return mModeStatus; }
    private:
        const string        mMode;
        int                 mModeStatus;
        bool                mSetModeFlag;
        kfsMode_t           mModeToSet;
        const bool          mRecursiveFlag;
        FileSystem::StatBuf mStat;

        enum
        {
            kUser  = 1,
            kGroup = 2,
            kOther = 4,
            kAll   = 7
        };

        kfsMode_t GetMode(
            kfsMode_t inMode)
        {
            kfsMode_t theMode     = inMode & (0777 | S_ISVTX);
            int       theDest     = 0;
            int       theOp       = 0;
            int       thePermBits = 0;
            for (const char* thePtr = mMode.c_str(); *thePtr; thePtr++) {
                const int theSym = *thePtr & 0xFF;
                if (theOp) {
                    switch (theSym) {
                        case 'r': thePermBits |= 4; break;
                        case 'w': thePermBits |= 2; break;
                        case 'X':
                            if (! S_ISDIR(inMode) && (inMode & 0100) == 0) {
                                break;
                            }
                            // Fall though.
                        case 'x': thePermBits |= 1; break;
                        case 't': thePermBits |= S_ISVTX; break;
                        case '-':
                        case '+':
                        case '=':
                            SetBits(theDest, theOp, thePermBits, theMode);
                            theOp       = theSym;
                            thePermBits = 0;
                            break;
                        case ',':
                            SetBits(theDest, theOp, thePermBits, theMode);
                            theDest     = 0;
                            theOp       = 0;
                            thePermBits = 0;
                            break;
                        default:
                            return kKfsModeUndef;
                    }
                } else {
                    switch (theSym) {
                        case 'u': theDest |= kUser;  break;
                        case 'g': theDest |= kGroup; break;
                        case 'o': theDest |= kOther; break;
                        case 'a': theDest |= kAll;   break;
                        case '-':
                        case '+':
                        case '=':
                            if (! theDest) {
                                theDest = kAll;
                            }
                            theOp = theSym;
                            break;
                        default:
                            return kKfsModeUndef;
                    }
                }
            }
            if (theDest && ! theOp) {
                return kKfsModeUndef;
            }
            if (theOp) {
                SetBits(theDest, theOp, thePermBits, theMode);
            }
            return theMode;
        }
        static void SetBits(
            int        inDest,
            int        inOp,
            int        inPermBits,
            kfsMode_t& ioMode)
        {
            if ((inDest & kOther) != 0) {
                SetBitsDest(inOp, inPermBits, 0, ioMode);
            }
            if ((inDest & kGroup) != 0) {
                SetBitsDest(inOp, inPermBits, 3, ioMode);
            }
            if ((inDest & kUser) != 0) {
                SetBitsDest(inOp, inPermBits, 6, ioMode);
            }
        }
        static void SetBitsDest(
            int        inOp,
            int        inPermBits,
            int        inBitIdx,
            kfsMode_t& ioMode)
        {
            const int thePerm = inPermBits << inBitIdx;
            switch (inOp) {
                case '-': ioMode &= ~thePerm; break;
                case '+': ioMode |= thePerm;  break;
                case '=':
                    ioMode &= ~(7 << inBitIdx);
                    ioMode |= thePerm;
                    break;
                default: break;
            }
        }
    private:
        ChmodFunctor(
            const ChmodFunctor& inFunctor);
        ChmodFunctor& operator=(
            const ChmodFunctor& inFunctor);
    };
    int Chmod(
        char**      inArgsPtr,
        int         inArgCount,
        const char* inModePtr,
        bool        inRecursiveFlag)
    {
        ChmodFunctor theChmodFunc(inModePtr, inRecursiveFlag);
        const int theStatus = theChmodFunc.GetModeStatus();
        if (theStatus != 0) {
            cerr << "invalid mode string: " <<
                (inModePtr ? inModePtr : "") << "\n";
            return theStatus;
        }
        FunctorT<ChmodFunctor> theFunc(theChmodFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class MkdirFunctor
    {
    public:
        MkdirFunctor(
            kfsMode_t inMode,
            bool      inCreateAllFlag)
            : mMode(inMode),
              mCreateAllFlag(inCreateAllFlag)
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& /* inErrorReporter */)
        {
            return inFs.Mkdir(inPath, mMode, mCreateAllFlag);
        }
    private:
        const kfsMode_t mMode;
        const bool      mCreateAllFlag;
    private:
        MkdirFunctor(
            const MkdirFunctor& inFunctor);
        MkdirFunctor& operator=(
            const MkdirFunctor& inFunctor);
    };
    int Mkdir(
        char**    inArgsPtr,
        int       inArgCount,
        kfsMode_t inMode,
        bool      inCreateAllFlag)
    {
        MkdirFunctor           theMkdirFunc(inMode, inCreateAllFlag);
        FunctorT<MkdirFunctor> theFunc(theMkdirFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    template<bool TDestDirFlag = false, bool TDestDirDestFlag = false>
    class GetGlobLastEntry
    {
    public:
        GetGlobLastEntry()
            : mFsPtr(0),
              mPathName(),
              mDirFlag(false),
              mExistsFlag(false),
              mUMask(0),
              mCurUMask(mUMask)
            {}
        ~GetGlobLastEntry()
        {
            if (mUMask != mCurUMask && mFsPtr) {
                mFsPtr->SetUMask(mUMask);
            }
        }
        bool operator()(
            int&        ioGlobError,
            GlobResult& inGlobResult,
            ostream&    inErrorStream)
        {
            if (inGlobResult.size() <= 1 &&
                    inGlobResult.back().second.size() <= 1) {
                inErrorStream << "source and destination required\n";
                ioGlobError = -EINVAL;
                return false;
            }
            mFsPtr = inGlobResult.back().first;
            if (! mFsPtr) {
                inErrorStream << "internal error: null fs\n";
                ioGlobError = -EINVAL;
                return false;
            }
            mPathName = inGlobResult.back().second.back();
            inGlobResult.back().second.pop_back();
            if (inGlobResult.back().second.empty()) {
                inGlobResult.pop_back();
            }
            if ((ioGlobError = mFsPtr->GetUMask(mUMask)) != 0) {
                return false;
            }
            mCurUMask = mUMask;
            if (! TDestDirFlag) {
                return true;
            }
            ErrorReporter theErrReporter(*mFsPtr, inErrorStream);
            const int theErr = mFsPtr->Stat(mPathName, mStat);
            if (theErr != 0 && theErr != -ENOENT) {
                theErrReporter(mPathName, theErr);
                ioGlobError = theErr;
                return false;
            }
            mExistsFlag = theErr == 0;
            mDirFlag    = theErr == 0 && S_ISDIR(mStat.st_mode);
            if (TDestDirDestFlag && ! mDirFlag &&
                    (inGlobResult.size() > 1 ||
                    inGlobResult.back().second.size() > 1)) {
                ioGlobError = -ENOTDIR;
                theErrReporter(mPathName, ioGlobError);
                return false;
            }
            return true;
        }
        FileSystem* GetFsPtr() const
            { return mFsPtr; }
        const string& GetPathName() const
            { return mPathName; }
        bool IsDirectory() const
            { return mDirFlag; }
        bool Exists() const
            { return mExistsFlag; }
        const FileSystem::StatBuf& GetStat() const
            { return mStat; }
        const mode_t GetUMask() const
            { return mUMask; }
        int SetUMask(
            mode_t inUMask)
        {
            if (inUMask == mUMask || mCurUMask == inUMask) {
                return 0;
            }
            if (! mFsPtr) {
                return -EINVAL;
            }
            const int theStatus = mFsPtr->SetUMask(inUMask & 0777);
            if (theStatus != 0) {
                return theStatus;
            }
            mCurUMask = inUMask & 0777;
            return theStatus;
        }
    private:
        FileSystem*         mFsPtr;
        string              mPathName;
        FileSystem::StatBuf mStat;
        bool                mDirFlag;
        bool                mExistsFlag;
        bool                mRestoreUMaskFlag;
        mode_t              mUMask;
        mode_t              mCurUMask;

    private:
        GetGlobLastEntry(
            const GetGlobLastEntry& inFunctor);
        GetGlobLastEntry& operator=(
            const GetGlobLastEntry& inFunctor);
    };
    template<typename TGetGlobLastEntry>
    class CopyFunctor
    {
    public:
        enum { kBufferSize = 6 << 20 };
        CopyFunctor(
            bool inMoveFlag = false)
            : mDestPtr(0),
              mDstName(),
              mBufferPtr(0),
              mDstDirStat(),
              mMoveFlag(inMoveFlag),
              mCheckDestFlag(true)
            {}
        ~CopyFunctor()
            { delete [] mBufferPtr; }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            if (! mDestPtr || inPath.empty()) {
                return -EINVAL;
            }
            if (mDstName.empty()) {
                mCheckDestFlag = true;
                mDstName = mDestPtr->GetPathName();
                if (mDestPtr->IsDirectory()) {
                    if (! mDstName.empty() && *(mDstName.rbegin()) != '/') {
                        mDstName += "/";
                    }
                }
            }
            const size_t theLen = mDstName.length();
            if (mDestPtr->IsDirectory()) {
                const char* const theSPtr = inPath.c_str();
                const char*       thePtr  = theSPtr + inPath.length();
                while (theSPtr < thePtr && *--thePtr == '/')
                    {}
                const char* theEndPtr = thePtr;
                while (theSPtr < thePtr && thePtr[-1] != '/') {
                    --thePtr;
                }
                if (thePtr < theEndPtr || *thePtr != '/') {
                    mDstName.append(thePtr, theEndPtr - thePtr + 1);
                }
                mCheckDestFlag = true;
            }
            FileSystem& theDstFs = *(mDestPtr->GetFsPtr());
            int theStatus;
            if (mMoveFlag && theDstFs == inFs) {
                if ((theStatus = inFs.Rename(inPath, mDstName)) != 0 &&
                        theStatus != -EXDEV) {
                    theStatus = inErrorReporter(inPath, theStatus);
                    if (theLen < mDstName.length()) {
                        mDstName.resize(theLen);
                    }
                }
                if (theStatus != -EXDEV) {
                    return theStatus;
                }
            }
            ErrorReporter theDstErrorReporter(
                theDstFs,
                inErrorReporter.GetErrorStream(),
                inErrorReporter.GetStopOnErrorFlag()
            );
            FileSystem::StatBuf theStat;
            theStatus = inFs.Stat(inPath, theStat);
            if (theStatus != 0) {
                mDstName.resize(theLen);
                return inErrorReporter(inPath, theStatus);
            }
            if ((theStatus = mDestPtr->SetUMask(0)) != 0 &&
                    (theStatus = theDstErrorReporter(mDstName, theStatus)) != 0) {
                return theStatus;
            }
            bool theSetModeFlag = false;
            if (mCheckDestFlag && S_ISDIR(theStat.st_mode)) {
                // Move: attempt to remove the destination directory to ensure
                // that the destination directory is empty.
                if (mMoveFlag && (theStatus = theDstFs.Rmdir(mDstName)) != 0 &&
                        theStatus != -ENOENT) {
                    theStatus = theDstErrorReporter(mDstName, theStatus);
                    mDstName.resize(theLen);
                    return theStatus;
                }
                const bool kCreateAllFlag = false;
                theStatus = theDstFs.Mkdir(
                    mDstName,
                    (theStat.st_mode & (0777 | S_ISVTX)) | 0600,
                    kCreateAllFlag
                );
                theSetModeFlag = theStatus == 0;
                if ((theStatus == -EEXIST || theStatus == 0) &&
                        (theStatus = theDstFs.Stat(
                            mDstName, mDstDirStat)) == 0 &&
                        ! S_ISDIR(mDstDirStat.st_mode)) {
                    theStatus = -ENOTDIR;
                }
                if (theStatus != 0) {
                    theStatus = theDstErrorReporter(mDstName, theStatus);
                    mDstName.resize(theLen);
                    return theStatus;
                }
                theSetModeFlag = theSetModeFlag &&
                    (theStat.st_mode & (0777 | S_ISVTX)) !=
                    (mDstDirStat.st_mode & (0777 | S_ISVTX));
                mCheckDestFlag = false;
            }
            if (! mBufferPtr) {
                mBufferPtr = new char[kBufferSize];
            }
            Copier theCopier(inFs, theDstFs,
                inErrorReporter, theDstErrorReporter, mBufferPtr, kBufferSize,
                inFs == theDstFs ? &mDstDirStat : 0,
                mMoveFlag
            );
            theStatus = theCopier.Copy(inPath, mDstName, theStat);
            if (theStatus == 0 && theSetModeFlag &&
                    (theStatus = theDstFs.Chmod(mDstName,
                        theStat.st_mode & (0777 | S_ISVTX), false, 0)) != 0) {
                theStatus = theDstErrorReporter(mDstName, theStatus);
            }
            if (theLen < mDstName.length()) {
                mDstName.resize(theLen);
            }
            return theStatus;
        }
        void SetDest(
            TGetGlobLastEntry& inDest)
            { mDestPtr = &inDest; }
    private:
        TGetGlobLastEntry*  mDestPtr;
        string              mDstName;
        char*               mBufferPtr;
        FileSystem::StatBuf mDstDirStat;
        const bool          mMoveFlag;
        bool                mCheckDestFlag;
    private:
        CopyFunctor(
            const CopyFunctor& inFunctor);
        CopyFunctor& operator=(
            const CopyFunctor& inFunctor);
    };
    typedef GetGlobLastEntry<true, true> CopyGetlastEntry;
    typedef CopyFunctor<CopyGetlastEntry> CpFunctor;
    int Copy(
        char** inArgsPtr,
        int    inArgCount)
    {
        CpFunctor theCopyFunc;
        FunctorT<CpFunctor, CopyGetlastEntry, true, false>
            theFunc(theCopyFunc, cerr);
        theCopyFunc.SetDest(theFunc.GetInit());
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    int CopyFromLocal(
        char**   inArgsPtr,
        int      inArgCount,
        bool     inMoveFlag,
        ostream& inErrorStream)
    {
        if (inArgCount < 2) {
            return -EINVAL;
        }
        FileSystem* theFsPtr  = 0;
        string      thePath;
        const char* theUriPtr = "file://";
        int         theErr    = FileSystem::Get(theUriPtr, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUriPtr <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        GlobResult  theGlob;
        theGlob.resize(1);
        thePath.clear();
        theGlob.front().first = theFsPtr;
        for (int i = 0; i < inArgCount - 1; i++) {
            const char* thePtr = inArgsPtr[i];
            if (! thePtr || ! *thePtr) {
                theErr = -EINVAL;
                inErrorStream << (thePtr ? thePtr : "null") <<
                    ": " << FileSystem::GetStrError(theErr) << "\n";
                return theErr;
            }
            if (*thePtr != '/') {
                if (thePath.empty()) {
                    theErr = theFsPtr->GetCwd(thePath);
                    if (theErr != 0) {
                        inErrorStream << thePtr <<
                            ": " << FileSystem::GetStrError(theErr) << "\n";
                        return theErr;
                    }
                    if (*(thePath.rbegin()) != '/') {
                        thePath += "/";
                    }
                }
                theGlob.front().second.push_back(thePath + thePtr);
            } else {
                theGlob.front().second.push_back(thePtr);
            }
        }
        theUriPtr = inArgsPtr[inArgCount - 1];
        thePath.clear();
        theFsPtr  = 0;
        theErr    = FileSystem::Get(theUriPtr, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUriPtr <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        if (theGlob.back().first != theFsPtr) {
            theGlob.resize(theGlob.size() + 1);
            theGlob.back().first = theFsPtr;
        }
        theGlob.back().second.push_back(thePath);

        CpFunctor theCopyFunc(inMoveFlag);
        FunctorT<CpFunctor, CopyGetlastEntry, true, false>
            theFunc(theCopyFunc, cerr);
        theCopyFunc.SetDest(theFunc.GetInit());
        return Apply(theGlob, theErr, theGlob.front().first != theFsPtr,
            theFunc);
    }
    int CopyToLocal(
        char**   inArgsPtr,
        int      inArgCount,
        bool     inMoveFlag,
        ostream& inErrorStream)
    {
        if (inArgCount < 2) {
            return -EINVAL;
        }
        GlobResult theGlob;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(inArgsPtr, inArgCount - 1,
            inErrorStream, theGlob, theMoreThanOneFsFlag);
        if (theErr != 0) {
            return theErr;
        }
        FileSystem* theFsPtr  = 0;
        string      thePath;
        const char* theUriPtr = "file://";
        theErr = FileSystem::Get(theUriPtr, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUriPtr <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        const char* const theDestPtr = inArgsPtr[inArgCount - 1];
        if (! theDestPtr || ! *theDestPtr) {
            theErr = -EINVAL;
            inErrorStream << (theDestPtr ? theDestPtr : "null") <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        if (*theDestPtr != '/') {
            theErr = theFsPtr->GetCwd(thePath);
            if (theErr != 0) {
                inErrorStream << theDestPtr <<
                    ": " << FileSystem::GetStrError(theErr) << "\n";
                return theErr;
            }
            if (*(thePath.rbegin()) != '/') {
                thePath += "/";
            }
            thePath += theDestPtr;
        } else {
            thePath = theDestPtr;
        }
        if (theGlob.back().first != theFsPtr) {
            theGlob.resize(theGlob.size() + 1);
            theGlob.back().first = theFsPtr;
        }
        theGlob.back().second.push_back(thePath);

        CpFunctor theCopyFunc(inMoveFlag);
        FunctorT<CpFunctor, CopyGetlastEntry, true, false>
            theFunc(theCopyFunc, cerr);
        theCopyFunc.SetDest(theFunc.GetInit());
        return Apply(theGlob, theErr, theGlob.front().first != theFsPtr,
            theFunc);
    }
    class Copier
    {
    public:
        Copier(
            FileSystem&                inSrcFs,
            FileSystem&                inDstFs,
            ErrorReporter&             inSrcErrorReporter,
            ErrorReporter&             inDstErrorReporter,
            char*                      inBufferPtr,
            size_t                     inBufferSize,
            const FileSystem::StatBuf* inSkipDirStatPtr,
            bool                       inRemoveSrcFlag)
            : mSrcFs(inSrcFs),
              mDstFs(inDstFs),
              mSrcErrorReporter(inSrcErrorReporter),
              mDstErrorReporter(inDstErrorReporter),
              mOwnsBufferFlag(! inBufferPtr || inBufferSize <= 0),
              mBufferSize(mOwnsBufferFlag ? (6 << 20) : inBufferSize),
              mBufferPtr(mOwnsBufferFlag ? new char[mBufferSize] : inBufferPtr),
              mCreateParams(),
              mStream(),
              mName(),
              mSrcName(),
              mDstName(),
              mSkipDirStatPtr(inSkipDirStatPtr),
              mRemoveSrcFlag(inRemoveSrcFlag)
            {}
        ~Copier()
        {
            if (mOwnsBufferFlag) {
                delete [] mBufferPtr;
            }
        }
        int Copy(
            const string&              inSrcPath,
            const string&              inDstPath,
            const FileSystem::StatBuf& inSrcStat)
        {
            return (
                S_ISDIR(inSrcStat.st_mode) ?
                    CopyDir(inSrcPath, inDstPath) :
                    CopyFile(inSrcPath, inDstPath, inSrcStat)
            );
        }
        int CopyDir(
            const string& inSrcPath,
            const string& inDstPath)
        {
            FileSystem::DirIterator* theDirIt             = 0;
            const bool               kFetchAttributesFlag = true;
            int                      theStatus            =
                mSrcFs.Open(inSrcPath, kFetchAttributesFlag, theDirIt);
            if (theStatus != 0) {
                mSrcErrorReporter(inSrcPath, theStatus);
                return theStatus;
            }
            const FileSystem::StatBuf* theStatPtr    = 0;
            size_t                     theSrcNameLen = 0;
            size_t                     theDstNameLen = 0;
            for (; ;) {
                mName.clear();
                if ((theStatus = mSrcFs.Next(
                        theDirIt, mName, theStatPtr)) != 0) {
                    if ((theStatus = mSrcErrorReporter(
                            mDstName, theStatus)) != 0 || mName.empty()) {
                        break;
                    }
                    continue;
                }
                if (mName.empty()) {
                    break;
                }
                if (mName == "." || mName == "..") {
                    continue;
                }
                if (! theStatPtr) {
                    theStatus = -EINVAL;
                    if (mSrcErrorReporter(inSrcPath, theStatus) == 0) {
                        continue;
                    }
                    break;
                }
                const FileSystem::StatBuf& theStat = *theStatPtr;
                theStatPtr = 0;
                SetDirPath(inSrcPath, mSrcName, theSrcNameLen).append(mName);
                SetDirPath(inDstPath, mDstName, theDstNameLen).append(mName);
                if (S_ISDIR(theStat.st_mode)) {
                    if (mSkipDirStatPtr &&
                            mSkipDirStatPtr->st_dev == theStat.st_dev &&
                            mSkipDirStatPtr->st_ino == theStat.st_ino) {
                        if ((theStatus = mSrcErrorReporter(mDstName,
                                "cannot copy directory into itself")) != 0) {
                            break;
                        }
                        continue;
                    }
                    bool         theCreatedFlag = false;
                    const size_t theCurDstLen   = mDstName.length();
                    if ((theStatus = MakeDirIfNeeded(
                            mDstFs,
                            mDstName,
                            (theStat.st_mode & (0777 | S_ISVTX)) | 0600,
                            &theCreatedFlag
                            )) != 0) {
                        if (mDstErrorReporter(mDstName, theStatus) == 0) {
                            continue;
                        }
                        break;
                    }
                    if ((theStatus = CopyDir(mSrcName, mDstName)) != 0) {
                        break;
                    }
                    if (theCreatedFlag && (theStat.st_mode & 0600) != 0600) {
                        mDstName.resize(theCurDstLen);
                        if ((theStatus = mDstFs.Chmod(
                                mDstName,
                                theStat.st_mode & (0777 | S_ISVTX),
                                false, 0)) != 0) {
                            if (mDstErrorReporter(mDstName, theStatus) == 0) {
                                continue;
                            }
                            break;
                        }
                    }
                } else {
                    if ((theStatus = CopyFile(
                            mSrcName, mDstName, theStat)) != 0) {
                        break;
                    }
                }
            }
            SetDirPath(inSrcPath, mSrcName, theSrcNameLen);
            const int theCloseStatus = mSrcFs.Close(theDirIt);
            if (mRemoveSrcFlag && (theStatus = mSrcFs.Rmdir(mSrcName)) != 0) {
                theStatus = mSrcErrorReporter(inSrcPath, theStatus);
            }
            if (theCloseStatus != 0 && theStatus == 0) {
                theStatus = mSrcErrorReporter(mSrcName, theStatus);
            }
            return theStatus;
        }
        int CopyFile(
            const string&              inSrcPath,
            const string&              inDstPath,
            const FileSystem::StatBuf& inSrcStat)
        {
            if (IsSameInode(mSrcFs, inSrcPath, mDstFs, inDstPath,
                    &inSrcStat)) {
                return mSrcErrorReporter(inSrcPath,
                    "is identical to destination (not copied).");
            }
            const int theSrcFd = mSrcFs.Open(inSrcPath, O_RDONLY, 0, 0);
            if (theSrcFd < 0) {
                return mSrcErrorReporter(inSrcPath, theSrcFd);
            }
            if (inSrcStat.mNumReplicas > 0) {
                mCreateParams.clear();
                mStream.str(mCreateParams);
                mStream << inSrcStat.mNumReplicas;
                if (inSrcStat.mNumStripes > 0) {
                    mStream <<
                        "," << inSrcStat.mNumStripes <<
                        "," << inSrcStat.mNumRecoveryStripes <<
                        "," << inSrcStat.mStripeSize <<
                        "," << inSrcStat.mStriperType;

                }
                mCreateParams = mStream.str();
                mStream.str(string());
            } else {
                mCreateParams = kDefaultCreateParams;
            }
            const int theDstFd = mDstFs.Open(
                inDstPath,
                O_WRONLY | O_TRUNC | O_CREAT,
                inSrcStat.st_mode & (0777 | S_ISVTX),
                &mCreateParams
            );
            if (theDstFd < 0) {
                mSrcFs.Close(theSrcFd);
                return mDstErrorReporter(inDstPath, theDstFd);
            }
            // For now don't attempt sparse copy like "cp" does by creating
            // holes that corresponds to runs of 0's, as write positioning
            // support depends on the underlying file system and the type of
            // the file.
            // For sparse file support the write positioning limitations can
            // be expressed by extending FileSystem interface in the future.
            int theStatus = 0;
            for (ssize_t theTotal = 0; ;) {
                const ssize_t theNRd = mSrcFs.Read(
                    theSrcFd, mBufferPtr, mBufferSize);
                if (theNRd == 0) {
                    break;
                }
                if (theNRd < 0) {
                    theStatus = mSrcErrorReporter(inSrcPath, (int)theNRd);
                    break;
                }
                // The following for loop is to support "non regular" files.
                // For example pipes / sockets.
                const char*       thePtr    = mBufferPtr;
                const char* const theEndPtr = thePtr + theNRd;
                while (thePtr < theEndPtr) {
                    const ssize_t theNWr = mDstFs.Write(
                        theDstFd, thePtr, theEndPtr - thePtr);
                    if (theNWr < 0) {
                        theStatus = mDstErrorReporter(inDstPath, (int)theNWr);
                        break;
                    }
                    thePtr += theNWr;
                }
                if (thePtr < theEndPtr) {
                    break;
                }
                theTotal += theNRd;
                if ((inSrcStat.st_mode & S_IFREG) != 0 &&
                        theNRd < (ssize_t)mBufferSize &&
                        theTotal >= inSrcStat.st_size) {
                    break;
                }
            }
            const int theCloseDstStatus = mDstFs.Close(theDstFd);
            if (theCloseDstStatus < 0 && theStatus == 0) {
                theStatus = mDstErrorReporter(inDstPath, theCloseDstStatus);
            }
            const int theCloseSrcStatus = mSrcFs.Close(theSrcFd);
            if (theCloseSrcStatus < 0 && theStatus == 0) {
                theStatus = mSrcErrorReporter(inSrcPath, theCloseSrcStatus);
            }
            if (mRemoveSrcFlag && theStatus == 0) {
                const bool kRecursiveFlag = false;
                if ((theStatus = mSrcFs.Remove(
                        inSrcPath, kRecursiveFlag, 0)) != 0) {
                    theStatus = mSrcErrorReporter(inSrcPath, theStatus);
                }
            }
            return theStatus;
        }
    private:
        FileSystem&                      mSrcFs;
        FileSystem&                      mDstFs;
        ErrorReporter&                   mSrcErrorReporter;
        ErrorReporter&                   mDstErrorReporter;
        const bool                       mOwnsBufferFlag;
        const size_t                     mBufferSize;
        char* const                      mBufferPtr;
        string                           mCreateParams;
        ostringstream                    mStream;
        string                           mName;
        string                           mSrcName;
        string                           mDstName;
        const FileSystem::StatBuf* const mSkipDirStatPtr;
        const bool                       mRemoveSrcFlag;
    private:
        Copier(
            const Copier& inCopier);
        Copier& operator=(
            const Copier& inCopier);
    };
    static string& SetDirPath(
        const string& inPath,
        string&       ioPathName,
        size_t&       ioPathNameLen)
    {
        if (ioPathNameLen <= 0) {
            if (&inPath != &ioPathName) {
                ioPathName.assign(inPath.data(), inPath.size());
            }
            if (! ioPathName.empty() && *ioPathName.rbegin() != '/') {
                ioPathName.push_back((char)'/');
            }
            ioPathNameLen = ioPathName.length();
        } else {
            ioPathName.resize(ioPathNameLen);
        }
        return ioPathName;
    }
    static int MakeDirIfNeeded(
        FileSystem&   inFs,
        const string& inPath,
        kfsMode_t     inMode,
        bool*         inCreatedFlagPtr = 0)
    {
        const bool kCreateAllFlag = false;
        int theStatus;
        if ((theStatus = inFs.Mkdir(inPath, inMode, kCreateAllFlag)) != 0) {
            if (inCreatedFlagPtr) {
                *inCreatedFlagPtr = false;
            }
            FileSystem::StatBuf theStat;
            if (theStatus == -EEXIST &&
                    (theStatus = inFs.Stat(inPath, theStat)) == 0 &&
                    ! S_ISDIR(theStat.st_mode)) {
                theStatus = -ENOTDIR;
            }
        } else if (inCreatedFlagPtr) {
            *inCreatedFlagPtr = true;
        }
        return theStatus;
    }
    static bool IsSameInode(
        FileSystem&                inSrcFs,
        const string&              inSrcPath,
        FileSystem&                inDstFs,
        const string&              inDstPath,
        const FileSystem::StatBuf* inSrcStatPtr = 0,
        const FileSystem::StatBuf* inDstStatPtr = 0)
    {
        if (inSrcFs != inDstFs) {
            return false;
        }
        if (inSrcPath == inDstPath) {
            return true;
        }
        FileSystem::StatBuf theSrcStatBuf;
        if (! inSrcStatPtr &&
                inSrcFs.Stat(inSrcPath, theSrcStatBuf) != 0) {
            return false;
        }
        FileSystem::StatBuf theDstStatBuf;
        if (! inDstStatPtr &&
                inSrcFs.Stat(inDstPath, theDstStatBuf) != 0) {
            return false;
        }
        const FileSystem::StatBuf& theSrcStat =
            inSrcStatPtr ? *inSrcStatPtr : theSrcStatBuf;
        const FileSystem::StatBuf& theDstStat =
            inDstStatPtr ? *inDstStatPtr : theDstStatBuf;
        return (
            theSrcStat.st_dev == theDstStat.st_dev &&
            theSrcStat.st_ino == theDstStat.st_ino
        );
    }
    int Move(
        char** inArgsPtr,
        int    inArgCount)
    {
        const bool kMoveFlag = true;
        CpFunctor theMoveFunctor(kMoveFlag);
        FunctorT<CpFunctor, CopyGetlastEntry, true, false>
            theFunc(theMoveFunctor, cerr);
        theMoveFunctor.SetDest(theFunc.GetInit());
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    static const char* SizeToHumanReadable(
        int64_t inSize,
        char*   inEndPtr)
    {
        const char* const theSuffixPtr[] = {
            "KB", "MB", "GB", "TB", "PB", 0
        };
        int64_t theSize = max(int64_t(0), inSize);
        int     i = 0;
        while (theSize >= (int64_t(1) << 20) && theSuffixPtr[i + 1]) {
            theSize >>= 10;
            i++;
        }
        // Two digit fractional part.
        int64_t theFrac =
            ((theSize & ((int64_t(1) << 10) - 1)) * 100 + (1 << 9)) >> 10;
        theSize >>= 10;
        if (theFrac >= 100) {
            theSize++;
            theFrac -= 100;
        }
        char* thePtr = inEndPtr;
        const size_t theLen = strlen(theSuffixPtr[i]) + 1;
        thePtr -= theLen;
        memcpy(thePtr, theSuffixPtr[i], theLen);
        *--thePtr = ' ';
        const char* const theStartPtr = thePtr;
        if (theFrac > 0) {
            for (int k = 0; k < 2; k++) {
                const int theDigit = (int)(theFrac % 10);
                theFrac /= 10;
                if (thePtr != theStartPtr || theDigit > 0) {
                    *--thePtr = (char)(theDigit + '0');
                }
            }
            *--thePtr = '.';
        }
        do {
            *--thePtr = ((theSize % 10) + '0');
            theSize /= 10;
        } while (theSize > 0);
        return thePtr;
    }
    class SubCounts
    {
    public:
        typedef int64_t Count;
        SubCounts(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
            : mDirCount(0),
              mFileCount(0),
              mByteCount(0),
              mStatus(0),
              mCurPath(),
              mFs(inFs),
              mPath(inPath),
              mErrorReporter(inErrorReporter)
            {}
        int Run()
        {
            mCurPath.reserve(4096);
            mCurPath.assign(mPath.data(), mPath.size());
            mStatus = 0;
            RunSelf();
            mCurPath.clear();
            return mStatus;
        }
        Count GetDirCount() const
            { return mDirCount; }
        Count GetFileCount() const
            { return mFileCount; }
        Count GetByteCount() const
            { return mByteCount; }
    private:
        Count          mDirCount;
        Count          mFileCount;
        Count          mByteCount;
        int            mStatus;
        string         mCurPath;
        FileSystem&    mFs;
        const string&  mPath;
        ErrorReporter& mErrorReporter;

        void RunSelf()
        {
            FileSystem::DirIterator* theItPtr             = 0;
            const bool               kFetchAttributesFlag = true;
            int                      theErr;
            if ((theErr = mFs.Open(
                    mCurPath, kFetchAttributesFlag, theItPtr)) != 0) {
                mStatus = mErrorReporter(mCurPath, theErr);
            } else {
                if (! mCurPath.empty() && mCurPath != "/" &&
                        *mCurPath.rbegin() != '/') {
                    mCurPath += "/";
                }
                const size_t theCurPathLen = mCurPath.length();
                string theName;
                for (; ;) {
                    const FileSystem::StatBuf* theStatPtr = 0;
                    if ((theErr = mFs.Next(
                            theItPtr, theName, theStatPtr))) {
                        mStatus = mErrorReporter(mCurPath + theName, theErr);
                    }
                    if (theName.empty()) {
                        break;
                    }
                    if (theName == "." || theName == "..") {
                        continue;
                    }
                    if (! theStatPtr) {
                        if (theErr == 0) {
                            mStatus = mErrorReporter(mCurPath, -EINVAL);
                        }
                        continue;
                    }
                    if (S_ISDIR(theStatPtr->st_mode)) {
                        mDirCount++;
                        mCurPath += theName;
                        RunSelf();
                        mCurPath.resize(theCurPathLen);
                    } else {
                        mFileCount++;
                        mByteCount += max(Count(0), Count(theStatPtr->st_size));
                    }
                }
                mFs.Close(theItPtr);
            }
        }
    };
    enum DiskUtilizationFormat
    {
        kDiskUtilizationFormatNone                 = 0,
        kDiskUtilizationFormatBytes                = 1,
        kDiskUtilizationFormatSummaryBytes         = 2,
        kDiskUtilizationFormatHumanReadable        = 3,
        kDiskUtilizationFormatSummaryHumanReadable = 4
    };
    class DiskUtilizationFunctor
    {
    public:
        DiskUtilizationFunctor(
            DiskUtilizationFormat inFormat,
            ostream&              inOutStream)
            : mFormat(inFormat),
              mOutStream(inOutStream),
              mDiskUtilizationEntries(),
              mBufEndPtr(mBuf + sizeof(mBuf) / sizeof(mBuf[0]))
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            mDiskUtilizationEntries.clear();
            FileSystem::StatBuf theStat;
            const int theStatus = inFs.Stat(inPath, theStat);
            if (theStatus != 0) {
                return theStatus;
            }
            if (! IsSummary() && S_ISDIR(theStat.st_mode)) {
                FileSystem::DirIterator* theItPtr             = 0;
                const bool               kFetchAttributesFlag = true;
                int                      theErr;
                if ((theErr = inFs.Open(
                        inPath, kFetchAttributesFlag, theItPtr)) != 0) {
                    inErrorReporter(inPath, theErr);
                } else {
                    string theName;
                    for (; ;) {
                        const FileSystem::StatBuf* theStatPtr = 0;
                        if ((theErr = inFs.Next(
                                theItPtr, theName, theStatPtr))) {
                            inErrorReporter(
                                inPath + (theName.empty() ? "" : "/") + theName,
                                theErr
                            );
                        }
                        if (theName.empty()) {
                            break;
                        }
                        if (theName == "." || theName == "..") {
                            continue;
                        }
                        AddEntry(inFs, inPath, theName, inErrorReporter,
                            theStatPtr);
                    }
                    inFs.Close(theItPtr);
                }
            } else {
                AddEntry(inFs, inPath, string(), inErrorReporter, &theStat);
            }
            if (! IsSummary() && mOutStream) {
                mOutStream << "Found " <<
                    mDiskUtilizationEntries.size() << " items\n";
            }
            for (DiskUtilizationEntries::const_iterator
                    theIt = mDiskUtilizationEntries.begin();
                    theIt != mDiskUtilizationEntries.end();
                    ++theIt) {
                Show(inFs, *theIt);
            }
            return theStatus;
        }
    private:
        struct DiskUtilizationListEntry
        {
            string  mPath;
            string  mName;
            int64_t mSize;
        };
        typedef deque<DiskUtilizationListEntry> DiskUtilizationEntries;

        const DiskUtilizationFormat mFormat;
        ostream&                    mOutStream;
        DiskUtilizationEntries      mDiskUtilizationEntries;
        char                        mBuf[32];
        char* const                 mBufEndPtr;

        bool IsSummary() const
        {
            return (mFormat == kDiskUtilizationFormatSummaryBytes ||
                mFormat == kDiskUtilizationFormatSummaryHumanReadable);
        }
        bool IsHumanReadable() const
        {
            return (mFormat == kDiskUtilizationFormatHumanReadable ||
                mFormat == kDiskUtilizationFormatSummaryHumanReadable);
        }
        void AddEntry(
            FileSystem&                inFs,
            const string&              inPath,
            const string&              inName,
            ErrorReporter&             inErrorReporter,
            const FileSystem::StatBuf* inStatPtr)
        {
            mDiskUtilizationEntries.push_back(DiskUtilizationListEntry());
            DiskUtilizationListEntry& theEntry = mDiskUtilizationEntries.back();
            theEntry.mPath = inPath;
            theEntry.mName = inName;
            if (! inStatPtr) {
                theEntry.mSize = 0;
                return;
            }
            const FileSystem::StatBuf& theStat = *inStatPtr;
            theEntry.mSize = theStat.st_size;
            if (theEntry.mSize < 0 && S_ISDIR(theStat.st_mode)) {
                const string thePath((inPath == "/" || inName.empty()) ?
                    inPath + inName : inPath + "/" + inName);
                SubCounts theCounts(inFs, thePath, inErrorReporter);
                theCounts.Run();
                theEntry.mSize = theCounts.GetByteCount();
            }
            if (theEntry.mSize < 0) {
                theEntry.mSize = 0;
            }
        }
        void Show(
            FileSystem&                     inFs,
            const DiskUtilizationListEntry& inEntry)
        {
            if (! mOutStream) {
                return;
            }
            if (IsSummary()) {
                ShowPath(inFs, inEntry) << "\t" << right;
            } else {
                mOutStream << setw(IsHumanReadable() ? 13 : 12) << left;
            }
            if (IsHumanReadable()) {
                mOutStream << SizeToHumanReadable(inEntry.mSize, mBufEndPtr);
            } else {
                mOutStream << inEntry.mSize;
            }
            if (! IsSummary()) {
                ShowPath(inFs, inEntry);
            }
            mOutStream << "\n";
        }
        ostream& ShowPath(
            FileSystem&                     inFs,
            const DiskUtilizationListEntry& inEntry)
        {
            return (
                mOutStream << inFs.GetUri() << inEntry.mPath <<
                ((*inEntry.mPath.rbegin() != '/' && ! inEntry.mName.empty()) ?
                    "/" : "") <<
                inEntry.mName
            );
        }
    private:
        DiskUtilizationFunctor(
            const DiskUtilizationFunctor& inFunctor);
        DiskUtilizationFunctor& operator=(
            const DiskUtilizationFunctor& inFunctor);
    };
    int DiskUtilization(
        char**                inArgsPtr,
        int                   inArgCount,
        DiskUtilizationFormat inFormat)
    {
        DiskUtilizationFunctor           theDuFunc(inFormat, cout);
        FunctorT<DiskUtilizationFunctor> theFunc(theDuFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    int DiskUtilizationBytes(
        char**                inArgsPtr,
        int                   inArgCount)
    {
        return DiskUtilization(
            inArgsPtr, inArgCount, kDiskUtilizationFormatBytes);
    }
    int DiskUtilizationSummary(
        char**                inArgsPtr,
        int                   inArgCount)
    {
        return DiskUtilization(
            inArgsPtr, inArgCount, kDiskUtilizationFormatSummaryBytes);
    }
    int DiskUtilizationHumanReadable(
        char**                inArgsPtr,
        int                   inArgCount)
    {
        return DiskUtilization(
            inArgsPtr, inArgCount, kDiskUtilizationFormatHumanReadable);
    }
    int DiskUtilizationSummaryHumanReadable(
        char**                inArgsPtr,
        int                   inArgCount)
    {
        return DiskUtilization(
            inArgsPtr, inArgCount, kDiskUtilizationFormatSummaryHumanReadable);
    }
    class CountFunctor
    {
    public:
        CountFunctor(
            bool     inShowQuotaFlag,
            ostream& inOutStream)
            : mShowQuotaFlag(inShowQuotaFlag),
              mOutStream(inOutStream),
              mStat()
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            mStat.Reset();
            const int theStatus = inFs.Stat(inPath, mStat);
            if (theStatus != 0) {
                return theStatus;
            }
            if (S_ISDIR(mStat.st_mode) &&
                    (mStat.st_size < 0 ||
                    mStat.mSubCount1 < 0 ||
                    mStat.mSubCount2 < 0)) {
                SubCounts theCounts(inFs, inPath, inErrorReporter);
                theCounts.Run();
                mStat.st_size    = theCounts.GetByteCount();
                mStat.mSubCount1 = theCounts.GetFileCount();
                mStat.mSubCount2 = theCounts.GetDirCount();
            }
            if (S_ISDIR(mStat.st_mode)) {
                mStat.mSubCount2++;
            } else {
                mStat.mSubCount2 = 0;
                mStat.mSubCount1 = 1;
            }
            Show(inFs, inPath);
            return theStatus;
        }
    private:
        const bool          mShowQuotaFlag;
        ostream&            mOutStream;
        FileSystem::StatBuf mStat;

        void Show(
            FileSystem&   inFs,
            const string& inPath)
        {
            if (mShowQuotaFlag) {
                // Quota currently not supported.
                mOutStream << right <<
                    setw(12) << "none" << " " <<
                    setw(12) << "inf"  << " " <<
                    setw(12) << "none" << " " <<
                    setw(12) << "inf"  << " " <<
                    setw(12) << mStat.mSubCount2 << " " <<
                    setw(12) << mStat.mSubCount1 << " " <<
                    setw(12) << mStat.st_size    << " " <<
                    inFs.GetUri() << inPath << "\n";
            } else {
                mOutStream << right <<
                    setw(12) << mStat.mSubCount2 << " " <<
                    setw(12) << mStat.mSubCount1 << " " <<
                    setw(12) << mStat.st_size    << " " <<
                    inFs.GetUri() << inPath << "\n";
            }
        }
    private:
        CountFunctor(
            const CountFunctor& inFunctor);
        CountFunctor& operator=(
            const CountFunctor& inFunctor);
    };
    int Count(
        char** inArgsPtr,
        int    inArgCount,
        bool   inShowQuotaFlag)
    {
        CountFunctor           theCountFunc(inShowQuotaFlag, cout);
        FunctorT<CountFunctor> theFunc(theCountFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class TouchzFunctor
    {
    public:
        TouchzFunctor()
            : mTime(),
              mStat(),
              mStatus(0),
              mTimeSetFlag(false),
              mCreateParams(kDefaultCreateParams)
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            if (mStatus != 0) {
                return mStatus;
            }
            if (! mTimeSetFlag) {
                if (gettimeofday(&mTime, 0)) {
                    mStatus = -errno;
                }
                mTimeSetFlag = true;
                // Tail recursion.
                return (*this)(inFs, inPath, inErrorReporter);
            }
            const int theStatus = inFs.Stat(inPath, mStat);
            if (theStatus == -ENOENT) {
                const int theFd = inFs.Open(
                    inPath, O_CREAT | O_WRONLY, 0666, &mCreateParams);
                if (theFd < 0) {
                    return theFd;
                }
                return inFs.Close(theFd);
            }
            if (theStatus != 0) {
                return theStatus;
            }
            if (S_ISDIR(mStat.st_mode)) {
                return -EISDIR;
            }
            if (mStat.st_size != 0) {
                return -EEXIST;
            }
            return inFs.SetMtime(inPath, mTime);
        }
    private:
        struct timeval      mTime;
        FileSystem::StatBuf mStat;
        int                 mStatus;
        bool                mTimeSetFlag;
        const string        mCreateParams;

        TouchzFunctor(
            const TouchzFunctor& inFunctor);
        TouchzFunctor& operator=(
            const TouchzFunctor& inFunctor);
    };
    int Touchz(
        char** inArgsPtr,
        int    inArgCount)
    {
        TouchzFunctor           theTouchzFunc;
        FunctorT<TouchzFunctor> theFunc(theTouchzFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    class SetModTimeFunctor
    {
    public:
        SetModTimeFunctor(
            int64_t inModTimeMs)
            : mTime()
        {
            mTime.tv_sec  = inModTimeMs / 1000;
            mTime.tv_usec = (inModTimeMs % 1000) * 1000;
        }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& /* inErrorReporter */)
        {
            return inFs.SetMtime(inPath, mTime);
        }
    private:
        struct timeval mTime;

        SetModTimeFunctor(
            const SetModTimeFunctor& inFunctor);
        SetModTimeFunctor& operator=(
            const SetModTimeFunctor& inFunctor);
    };
    int SetModTime(
        char**  inArgsPtr,
        int     inArgCount,
        int64_t inModTimeMs)
    {
        SetModTimeFunctor           theSetTimeFunc(inModTimeMs);
        FunctorT<SetModTimeFunctor> theFunc(theSetTimeFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    int Test(
        char** inArgsPtr,
        int    inArgCount)
    {
        if (inArgCount != 2 || (
                strcmp(inArgsPtr[0], "-e") != 0 &&
                strcmp(inArgsPtr[0], "-d") != 0 &&
                strcmp(inArgsPtr[0], "-z") != 0)) {
            ShortHelp(cerr, "Usage: ", "test");
            return -EINVAL;
        }
        GlobResult theResult;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(
            inArgsPtr + 1,
            inArgCount - 1,
            cerr,
            theResult,
            theMoreThanOneFsFlag);
        if (theErr != 0) {
            return theErr;
        }
        if (theResult.size() != 1 || theResult.front().second.size() != 1) {
            FileSystem*   theExFsPtr;
            const string* theExPathPtr;
            if (theResult.front().second.size() > 1) {
                theExFsPtr   = theResult.front().first;
                theExPathPtr = &(theResult.front().second[2]);
            } else {
                theExFsPtr   = theResult[1].first;
                theExPathPtr = &(theResult[1].second.front());
            }
            cerr << "extra argument: " <<
                theExFsPtr->GetUri() << *theExPathPtr << "\n";
            return -EINVAL;
        }
        FileSystem::StatBuf theStat;
        FileSystem&   theFs     = *(theResult.front().first);
        const string& thePath   = theResult.front().second.front();
        const int     theStatus = theFs.Stat(thePath, theStat);
        switch (inArgsPtr[0][1] & 0xFF) {
            case 'e':
                if (theStatus == 0 || theStatus == -ENOENT) {
                    return theStatus;
                }
                break;
            case 'd':
                if (theStatus != 0) {
                    break;
                }
                return (S_ISDIR(theStat.st_mode) ? 0 : 1);
            case 'z':
                if (theStatus != 0) {
                    break;
                }
                return ((S_ISDIR(theStat.st_mode) || theStat.st_size > 0) ?
                    1 : 0);
        }
        cerr << theFs.GetUri() << thePath << ": " <<
            theFs.StrError(theStatus) << "\n";
        return theStatus;
    }
    int CopyFromStream(
        const string& inUri,
        istream&      inInStream,
        ostream&      inErrStream,
        int           inOpenFlags    = O_CREAT | O_WRONLY | O_EXCL,
        const string& inCreateParams = kDefaultCreateParams)
    {
        FileSystem*  theFsPtr = 0;
        string       thePath;
        int          theErr   = FileSystem::Get(inUri, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrStream << inUri << ": " <<
                FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        FileSystem& theFs = *theFsPtr;
        const int theFd = theFs.Open(
            thePath,
            inOpenFlags,
            0666,
            &inCreateParams
        );
        bool theReportErrorFlag = true;
        if (theFd < 0) {
            theErr = theFd;
        } else {
            while (inInStream) {
                inInStream.read(mIoBufferPtr, mIoBufferSize);
                for (const char* thePtr = mIoBufferPtr,
                            * theEndPtr = thePtr + inInStream.gcount();
                        thePtr < theEndPtr;
                        ) {
                    const ssize_t theNWr =
                        theFs.Write(theFd, thePtr, theEndPtr - thePtr);
                    if (theNWr < 0) {
                        theErr = (int)theNWr;
                        break;
                    }
                    thePtr += theNWr;
                }
                if (theErr != 0) {
                    break;
                }
            }
            if (theErr == 0 && ! inInStream.eof()) {
                theErr = errno;
                inErrStream << "stdout: " << QCUtils::SysError(theErr) << "\n";
                theReportErrorFlag = false;
            }
            theFs.Close(theFd);
        }
        if (theErr != 0 && theReportErrorFlag) {
            inErrStream << theFs.GetUri() << thePath << ": " <<
                theFs.StrError(theErr) << "\n";
        }
        return theErr;
    }
    class SetReplicationFunctor
    {
    public:
        SetReplicationFunctor(
            int  inReplication,
            bool inRecursiveFlag)
            : mReplication(inReplication),
              mRecursiveFlag(inRecursiveFlag)
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            return inFs.SetReplication(
                inPath, mReplication, mRecursiveFlag, &inErrorReporter);
        }
    private:
        const int  mReplication;
        const bool mRecursiveFlag;

        SetReplicationFunctor(
            const SetReplicationFunctor& inFunctor);
        SetReplicationFunctor& operator=(
            const SetReplicationFunctor& inFunctor);
    };
    int SetReplication(
        char** inArgsPtr,
        int    inArgCount,
        int    inReplication,
        bool   inRecursiveFlag,
        bool   inWaitFlag)
    {
        if (inArgCount <= 0) {
            return -EINVAL;
        }
        SetReplicationFunctor theSetReplFunc(
            inReplication, inRecursiveFlag);
        FunctorT<SetReplicationFunctor> theFunc(theSetReplFunc, cerr);
        int theStatus = Apply(inArgsPtr, inArgCount, theFunc);
        if (theStatus != 0 || ! inWaitFlag) {
            return theStatus;
        }
        return theStatus;
    }
private:
    size_t const mIoBufferSize;
    char* const  mIoBufferPtr;
private:
    KfsTool(const KfsTool& inTool);
    KfsTool& operator=(const KfsTool& inTool);
};

const char* const KfsTool::sHelpStrings[] =
{
    "fs", "[local | <file system URI>]",
    "Specify the file system to use.\n\t\t"
    "'local' means use the local file system as your DFS.\n\t\t"
    "<file system URI> specifies a particular file system to\n\t\t"
    "contact. This argument is optional but if used must appear\n\t\t"
    "appear first on the command line. Exactly one additional\n\t\t"
    "argument must be specified.\n",

    "ls", "<path>",
    "List the contents that match the specified file pattern. If\n\t\t"
    "path is not specified, the contents of /user/<currentUser>\n\t\t"
    "will be listed. Directory entries are of the form \n\t\t"
    "dirName (full path) <dir>\n\t\t"
    "and file entries are of the form\n\t\t"
    "fileName(full path) <r n> size\n\t\t"
    "where n is the number of replicas specified for the file\n\t\t"
    "and size is the size of the file, in bytes.\n",

    "lsr", "<path>",
    "Recursively list the contents that match the specified\n\t\t"
    "file pattern.  Behaves very similarly to -ls,\n\t\t"
    "except that the data is shown for all the entries in the\n\t\t"
    "subtree.\n",

    "du", "<path>",
    "Show the amount of space, in bytes, used by the files that\n\t\t"
    "match the specified file pattern.  Equivalent to the unix\n\t\t"
    "command \"du -sb <path>/*\" in case of a directory,\n\t\t"
    "and to \"du -b <path>\" in case of a file.\n\t\t"
    "The output is in the form\n\t\t"
    "name(full path) size (in bytes)\n",

    "duh", "<path>",
    "Show the amount of space, in human readable form, used by\n\t\t"
    "the files that match the specified file pattern. Value is same as"
    " -du option\n",

    "dus", "<path>",
    "Show the amount of space, in bytes, used by the files that\n\t\t"
    "match the specified file pattern.  Equivalent to the unix\n\t\t"
    "command \"du -sb\"  The output is in the form\n\t\t"
    "name(full path) size (in bytes)\n",

    "dush", "<path>",
    "Show the amount of space, in human readable format, used by\n\t\t"
    "the files that match the specified file pattern."
    " Equivalent to unix du -sh\n",

    "mv", "<src> <dst>",
    "Move files that match the specified file pattern <src>\n\t\t"
    "to a destination <dst>.  When moving multiple files, the \n\t\t"
    "destination must be a directory.\n",

    "cp", "<src> <dst>",
    "Copy files that match the file pattern <src> to a\n\t\t"
    "destination.  When copying multiple files, the destination\n\t\t"
    "must be a directory.\n",

    "rm", "[-skipTrash] <src>",
    "Delete all files that match the specified file pattern.\n\t\t"
    "Equivalent to the Unix command \"rm <src>\"\n\t\t"
    "-skipTrash option bypasses trash, if enabled, and immediately\n\t\t"
    "deletes <src>\n",

    "rmr", "[-skipTrash] <src>",
    "Remove all directories which match the specified file\n\t\t"
    "pattern. Equivalent to the Unix command \"rm -rf <src>\"\n\t\t"
    "-skipTrash option bypasses trash, if enabled, and immediately\n\t\t"
    "deletes <src>\n",

    "put", "<localsrc> ... <dst>",
    "Copy files from the local file system\n\t\t"
    "into fs.\n",

    "copyFromLocal", "<localsrc> ... <dst>",
    "Identical to the -put command.\n",

    "moveFromLocal", "<localsrc> ... <dst>",
    "Same as -put, except that the source is\n\t\t"
    "deleted after it's copied.\n",

    "get", "[-ignoreCrc] [-crc] <src> <localdst>",
    "Copy files that match the file pattern <src>\n\t\t"
    "to the local name.  <src> is kept.  When copying mutiple,\n\t\t"
    "files, the destination must be a directory.\n",
/*
    "getmerge", "<src> <localdst>",
    "Get all the files in the directories that\n\t\t"
    "match the source file pattern and merge and sort them to only\n\t\t"
    "one file on local fs. <src> is kept.\n",
*/
    "cat", "<src>",
    "Fetch all files that match the file pattern <src> \n\t\t"
    "and display their content on stdout.\n",

    "copyToLocal", "[-ignoreCrc] [-crc] <src> <localdst>",
    "Identical to the -get command.\n",

    "moveToLocal", "<src> <localdst>",
    "Not implemented yet\n",

    "mkdir", "<path>",
    "Create a directory in specified location.\n",

    "setrep", "[-R] [-w] <rep> <path/file>",
    "Set the replication level of a file.\n\t\t"
    "The -R flag requests a recursive change of replication level\n\t\t"
    "for an entire tree.\n",

    "tail", "[-f] <file>",
    "Show the last 1KB of the file.\n\t\t"
    "The -f option shows apended data as the file grows.\n",

    "touchz", "<path>",
    "Set modification time to the current time\n\t\t"
    "in a file at <path>. An error is returned if the file exists with non-zero"
    " length or is directory\n",

    "test", "-[ezd] <path>",
    "If file { exists, has zero length, is a directory\n\t\t"
    "then return 0, else return 1.\n",

    "text", "<src>",
    "Takes a source file and outputs the file in text format.\n\t\t"
    "The allowed formats are zip.\n",

    "setModTime", "<src> <time>",
    "Set modification time <time in milliseconds>"
    " on <src>\n",

    "stat", "[format] <path>",
    "Print statistics about the file/directory at <path>\n\t\t"
    "in the specified format. Format accepts filesize in blocks (%b),"
    " filename (%n),\n\t\t"
    "block size (%o), replication (%r), modification date (%y, %Y)\n",

    "chmod", "[-R] <MODE[,MODE]... | OCTALMODE> PATH...",
    "Changes permissions of a file.\n\t\t"
    "This works similar to shell's chmod with a few exceptions.\n"
    "\n\t"
    "-R\tmodifies the files recursively. This is the only option\n\t\t"
    "currently supported.\n"
    "\n\t"
    "MODE\tMode is same as mode used for chmod shell command.\n\t\t"
    "Only letters recognized are 'rwxX'. E.g. a+r,g-w,+rwx,o=r\n"
    "\n\t"
    "OCTALMODE\tMode specifed in 3 digits. Unlike shell command,\n\t\t"
    "this requires all three digits.\n\t\t"
    "E.g. 754 is same as u=rwx,g=rx,o=r\n"
    "\n\t\t"
    "If none of 'augo' is specified, 'a' is assumed and unlike\n\t\t"
    "shell command, no umask is applied.\n",

    "chown", "[-R] [OWNER][:[GROUP]] PATH...",
    "Changes owner and group of a file.\n\t\t"
    "This is similar to shell's chown with a few exceptions.\n"
    "\n\t"
    "-R\tmodifies the files recursively. This is the only option\n\t\t"
    "currently supported.\n"
    "\n\t\t"
    "If only owner or group is specified then only owner or\n\t\t"
    "group is modified.\n"
    "\n",

    "chgrp", "[-R] GROUP PATH...",
    "This is equivalent to -chown ... :GROUP ...\n",

    "count", "[-q] <path>",
    "Count the number of directories, files and bytes under the paths\n\t\t"
    "that match the specified file pattern. The output columns are:\n\t\t"
    "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or\n\t\t"
    "QUOTA REMAINING_QUATA SPACE_QUOTA REMAINING_SPACE_QUOTA \n\t\t"
    "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME\n",

    "help", "[cmd]",
    "Displays help for given command or all commands if none\n\t\t"
    "is specified.\n",

    0, 0, 0, // Sentinel
    0, 0, 0
};

}
}

int
main(int argc, char** argv)
{
    KFS::tools::KfsTool theTool;
    return theTool.Run(argc, argv);
}
