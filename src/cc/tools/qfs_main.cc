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
#include "Trash.h"

#include "common/MsgLogger.h"
#include "common/Properties.h"
#include "common/IntToString.h"
#include "kfsio/ZlibInflate.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "libclient/Path.h"

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
#include <sstream>

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
using std::setw;
using std::flush;
using std::left;
using std::right;
using std::oct;
using std::dec;
using std::make_pair;

using client::Path;

const string      kTrashCfgPrefix("fs.trash.");
const char* const kMsgLogWriterCfgPrefix = "fs.msgLogWriter.";

class KfsTool
{
public:
    KfsTool()
        : mIoBufferSize(6 << 20),
          mIoBufferPtr(new char[mIoBufferSize]),
          mDefaultCreateParams("S"), // RS 6+3 64K stripe
          mConfig()
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
        string              theUri;
        int                 theArgIndex = 1;
        mConfig.clear();
        while (theArgIndex < inArgCount && inArgsPtr[theArgIndex][0] == '-') {
            const char* const theOptPtr = inArgsPtr[theArgIndex];
            if (theOptPtr[1] == 'D') {
                const char* theCfsOptPtr = theOptPtr + 2;
                if (*theCfsOptPtr == 0) {
                    if (++theArgIndex >= inArgCount) {
                        break;
                    }
                    theCfsOptPtr = inArgsPtr[theArgIndex];
                }
                const int theErr = mConfig.loadProperties(
                    theCfsOptPtr, strlen(theCfsOptPtr), char('='));
                if (theErr != 0) {
                    cerr << "-D" << theCfsOptPtr <<
                        FileSystem::GetStrError(theErr) << "\n";
                    return 1;
                }
            } else if (strcmp(theOptPtr, "-v") == 0) {
                theLogLevel = MsgLogger::kLogLevelDEBUG;
            } else if (strcmp(theOptPtr, "-fs") == 0) {
                theArgIndex++;
                if (inArgCount <= theArgIndex) {
                    ShortHelp(cerr);
                    return 1;
                }
                theUri = inArgsPtr[theArgIndex];
            } else if (strcmp(theOptPtr, "-cfg") == 0) {
                theArgIndex++;
                if (inArgCount <= theArgIndex) {
                    ShortHelp(cerr);
                    return 1;
                }
                if (mConfig.loadProperties(
                        inArgsPtr[theArgIndex], char('='),
                        theLogLevel == MsgLogger::kLogLevelDEBUG)) {
                    return 1;
                }
            } else {
                break;
            }
            theArgIndex++;
        }
        if (inArgCount <= theArgIndex) {
            ShortHelp(cerr);
            return 1;
        }
        if (theLogLevel == MsgLogger::kLogLevelDEBUG) {
            // -v overrides configuration setting, if any.
            mConfig.setValue(
                Properties::String(kMsgLogWriterCfgPrefix).Append("logLevel"),
                Properties::String("DEBUG")
            );
        }
        MsgLogger::Init(0, theLogLevel, &mConfig, kMsgLogWriterCfgPrefix);
        if (theUri.empty()) {
            theUri = mConfig.getValue("fs.default", theUri);
        }
        if (! theUri.empty()) {
            const int theErr = FileSystem::SetDefault(theUri, &mConfig);
            if (theErr != 0) {
                cerr << theUri << ": " <<
                    FileSystem::GetStrError(theErr) << "\n";
                return 1;
            }
        }
        mDefaultCreateParams = mConfig.getValue(
            "fs.createParams", mDefaultCreateParams);
        const char* const theCmdPtr  = inArgsPtr[theArgIndex++] + 1;
        if (theCmdPtr[-1] != '-') {
            ShortHelp(cerr);
            return 1;
        }
        int    theArgCnt        = inArgCount - theArgIndex;
        char** theArgsPtr       = inArgsPtr + theArgIndex;
        int    theErr           = 0;
        bool   theChgrpFlag     = false;
        bool   theMoveFlag      = false;
        bool   theRecursiveFlag = false;
        bool   theCopyFlag      = false;
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
                (theCopyFlag = strcmp(theCmdPtr, "copyFromLocal") == 0) ||
                (theMoveFlag = strcmp(theCmdPtr, "moveFromLocal") == 0)) {
            if (theCopyFlag || theMoveFlag) {
                while (theArgCnt > 0 && (strcmp(theArgsPtr[0], "-crc") == 0
                        || strcmp(theArgsPtr[0], "-ignoreCrc") == 0)) {
                    cerr << theArgsPtr[0] << " parameter ignored\n";
                    theArgsPtr++;
                    theArgCnt--;
                }
            }
            if (theArgCnt == 2 && strcmp(theArgsPtr[0], "-") == 0) {
                theErr = CopyFromStream(
                    theArgsPtr[1], cin, cerr, mDefaultCreateParams);
            } else {
                if (theArgCnt < 2) {
                    theErr = EINVAL;
                    ShortHelp(cerr, "Usage: ", theCmdPtr);
                } else {
                    theErr = CopyFromLocal(
                        theArgsPtr, theArgCnt, theMoveFlag, cerr);
                }
            }
        } else if (strcmp(theCmdPtr, "get") == 0 ||
                (theCopyFlag = strcmp(theCmdPtr, "copyToLocal") == 0) ||
                (theMoveFlag = strcmp(theCmdPtr, "moveToLocal") == 0)) {
            if (theMoveFlag) {
                // Do not allow for now as with "dfs".
                cerr << theCmdPtr << " is not implemented yet.\n";
                theErr = -EINVAL;
            } else {
                if (theCopyFlag || theMoveFlag) {
                    while (theArgCnt > 0 && (strcmp(theArgsPtr[0], "-crc") == 0
                            || strcmp(theArgsPtr[0], "-ignoreCrc") == 0)) {
                        cerr << theArgsPtr[0] << " parameter ignored\n";
                        theArgsPtr++;
                        theArgCnt--;
                    }
                }
                if (theArgCnt < 2) {
                    theErr = EINVAL;
                    ShortHelp(cerr, "Usage: ", theCmdPtr);
                } else if (strcmp(theArgsPtr[theArgCnt - 1], "-") == 0) {
                    theErr = Cat(theArgsPtr, theArgCnt - 1);
                } else {
                    theErr = CopyToLocal(
                        theArgsPtr, theArgCnt, theMoveFlag, cerr);
                }
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
        } else if (strcmp(theCmdPtr, "stat") == 0) {
            const char* theFormatPtr = 0;
            if (theArgCnt > 1 && strchr(theArgsPtr[0], '%')) {
                theFormatPtr = theArgsPtr[0];
                theArgCnt--;
                theArgsPtr++;
            }
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                theErr = Stat(theArgsPtr, theArgCnt, theFormatPtr);
            }
        } else if (strcmp(theCmdPtr, "astat") == 0) {
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                theErr = FullStat(theArgsPtr, theArgCnt);
            }
        } else if (strcmp(theCmdPtr, "tail") == 0) {
            bool theFollowFlag = theArgCnt > 0 &&
                strcmp(theArgsPtr[0], "-f") == 0;
            if (theFollowFlag) {
                theArgsPtr++;
                theArgCnt--;
            }
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                theErr = Tail(theArgsPtr, theArgCnt, theFollowFlag);
            }
        } else if (strcmp(theCmdPtr, "rm") == 0 ||
                (theRecursiveFlag = strcmp(theCmdPtr, "rmr") == 0)) {
            bool theSkipTrashFlag = theArgCnt > 0 &&
                strcmp(theArgsPtr[0], "-skipTrash") == 0;
            if (theSkipTrashFlag) {
                theArgsPtr++;
                theArgCnt--;
            }
            if (theArgCnt <= 0) {
                theErr = EINVAL;
                ShortHelp(cerr, "Usage: ", theCmdPtr);
            } else {
                theErr = Remove(theArgsPtr, theArgCnt,
                    theSkipTrashFlag, theRecursiveFlag);
            }
        } else if (strcmp(theCmdPtr, "expunge") == 0) {
            if (theArgCnt > 0) {
                theErr = EINVAL;
                ShortHelp(cerr);
            } else {
                theErr = Expunge();
            }
        } else if (strcmp(theCmdPtr, "runEmptier") == 0) {
            if (theArgCnt > 0) {
                char* theEndPtr = 0;
                const long theInterval = strtol(theArgsPtr[0], &theEndPtr, 0);
                if (theEndPtr && (*theEndPtr & 0xFF) <= ' ') {
                    if (theInterval > 0) {
                        mConfig.setValue(
                            kTrashCfgPrefix + "interval", theArgsPtr[0]);
                    }
                    theArgCnt--;
                    theArgsPtr++;
                }
            }
            if (theArgCnt > 0) {
                theErr = EINVAL;
                ShortHelp(cerr);
            } else {
                theErr = RunEmptier();
            }
        } else if (strcmp(theCmdPtr, "text") == 0) {
            theErr = Unzip(theArgsPtr, theArgCnt);
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
    static time_t GetMTime(
        const FileSystem::StatBuf& inStat)
    {
#ifndef KFS_OS_NAME_DARWIN
        return inStat.st_mtime;
#else
        return inStat.st_mtimespec.tv_sec;
#endif
    }
    static time_t GetCTime(
        const FileSystem::StatBuf& inStat)
    {
#ifndef KFS_OS_NAME_DARWIN
        return inStat.st_ctime;
#else
        return inStat.st_ctimespec.tv_sec;
#endif
    }
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
    template<typename T>
    class RecursiveApplicator
    {
    public:
        RecursiveApplicator(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter,
            T&             inFunctor)
            : mStatus(0),
              mCurPath(),
              mFs(inFs),
              mPath(inPath),
              mErrorReporter(inErrorReporter),
              mFunctor(inFunctor)
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
    private:
        int            mStatus;
        string         mCurPath;
        FileSystem&    mFs;
        const string&  mPath;
        ErrorReporter& mErrorReporter;
        T&             mFunctor;

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
                        if (mErrorReporter.GetStopOnErrorFlag() &&
                                mStatus != 0) {
                            break;
                        }
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
                            if (mErrorReporter.GetStopOnErrorFlag() &&
                                    mStatus != 0) {
                                break;
                            }
                        }
                        continue;
                    }
                    mCurPath += theName;
                    if (! mFunctor(
                            mFs, mCurPath, *theStatPtr, mErrorReporter)) {
                        mCurPath.resize(theCurPathLen);
                        break;
                    }
                    if (S_ISDIR(theStatPtr->st_mode)) {
                        RunSelf();
                    }
                    mCurPath.resize(theCurPathLen);
                }
                mFs.Close(theItPtr);
            }
        }
    private:
        RecursiveApplicator(
            const RecursiveApplicator& inApplicator);
        RecursiveApplicator operator=(
            const RecursiveApplicator& inApplicator);
    };
    typedef vector<pair<FileSystem*, vector<string> > > GlobResult;
    int GetFs(
        const string& inUri,
        FileSystem*&  outFsPtr,
        string*       outPathPtr = 0)
        { return FileSystem::Get(inUri, outFsPtr, outPathPtr, &mConfig); }
    int Glob(
        char**       inArgsPtr,
        int          inArgCount,
        ostream&     inErrorStream,
        GlobResult&  outResult,
        bool&        outMoreThanOneFsFlag,
        bool         inNormalizePathFlag = true)
    {
        outResult.reserve(outResult.size() + max(0, inArgCount));
        int  theRet = 0;
        Path theFsPath;
        outMoreThanOneFsFlag = false;
        for (int i = 0; i < inArgCount; i++) {
            const string theArg   = inArgsPtr[i];
            FileSystem*  theFsPtr = 0;
            string       thePath;
            int          theErr   = GetFs(theArg, theFsPtr, &thePath);
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
                    string theName = thePrefix + theGlobRes.gl_pathv[i];
                    if (inNormalizePathFlag &&
                            theFsPath.Set(theName.c_str(), theName.length()) &&
                            ! theFsPath.IsNormalized()) {
                        theName = theFsPath.NormPath();
                    }
                    theResult.push_back(theName);
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
        FuncT& inFunctor,
        bool   inNormalizePathFlag = true)
    {
        char  theArg[1]     = { 0 };
        char* theArgsPtr[1] = { theArg };
        GlobResult theResult;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(
            inArgCount <= 0 ? theArgsPtr : inArgsPtr,
            inArgCount <= 0 ? 1 : inArgCount,
            cerr,
            theResult,
            theMoreThanOneFsFlag,
            inNormalizePathFlag
        );
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
                char* const theEndPtr = mTmBuf + kTmBufLen;
                mFileSizeWidth = theEndPtr - IntToDecString(
                    mMaxFileSize, theEndPtr);
                mReplicasWidth = theEndPtr - IntToDecString(
                    mMaxReplicas, theEndPtr);
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
                mMTime       = GetMTime(inStat);
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
                mTmBuf[0] = 0;
                const size_t theLen = strftime(
                    mTmBuf, kTmBufLen, "%Y-%m-%d %H:%M ", &theLocalTime);
                if (theLen <= 0) {
                    strncpy(mTmBuf, "-", kTmBufLen);
                }
                mTime = inEntry.mMTime;
            }
            mOutStream << " " << mTmBuf;
            if (mShowFsUriFlag) {
                mOutStream << inFs.GetUri();
            }
            mOutStream << inEntry.mPath;
            if (! inEntry.mName.empty()) {
                if (inEntry.mPath != "/") {
                    mOutStream << "/";
                }
                mOutStream << inEntry.mName;
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
    class DefaultInitFunctor
    {
    public:
        DefaultInitFunctor()
            {}
        bool operator()(
            int&              /* ioGlobError */,
            const GlobResult& /* inGlobResult */,
            ostream&          /* inErrorStream */,
            bool              /* inMoreThanOneFsFlag */)
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
        template<typename InitFuncArgT>
        FunctorT(
            T&            inFunctor,
            ostream&      inErrorStream,
            InitFuncArgT& inInitFuncArg)
            : mFunctor(inFunctor),
              mInitFunctor(inInitFuncArg),
              mErrorStream(inErrorStream),
              mStatus(0)
            {}
        bool Init(
            int&        ioGlobError,
            GlobResult& inGlobResult,
            bool        inMoreThanOneFsFlag)
        {
            return mInitFunctor(ioGlobError, inGlobResult, mErrorStream,
                inMoreThanOneFsFlag);
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
    template <typename FuncT> int ApplyT(
        char** inArgsPtr,
        int    inArgCount,
        FuncT& inFunctor,
        bool   inNormalizePathFlag = true)
    {
        FunctorT<FuncT> theFunc(inFunctor, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc, inNormalizePathFlag);
    }
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
        ChownFunctor theChownFunc(
            inUserNamePtr, inGroupNamePtr, inRecursiveFlag);
        return ApplyT(inArgsPtr, inArgCount, theChownFunc);
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
            int theStatus = inFs.Stat(inPath, mStat);
            if (theStatus != 0) {
                return theStatus;
            }
            if (mRecursiveFlag && S_ISDIR(mStat.st_mode)) {
                RecursiveApplicator<ChmodFunctor>
                    theApplicator(inFs, inPath, inErrorReporter, *this);
                if ((theStatus = theApplicator.Run()) != 0) {
                    return theStatus;
                }
            }
            const bool kRecursiveFlag = false;
            return inFs.Chmod(inPath, GetMode(mStat.st_mode),
                kRecursiveFlag, 0);
        }
        bool operator()(
            FileSystem&                inFs,
            const string&              inPath,
            const FileSystem::StatBuf& inStat,
            ErrorReporter&             inErrorReporter)
        {
            const bool kRecursiveFlag = false;
            int theStatus = inFs.Chmod(inPath, GetMode(inStat.st_mode),
                kRecursiveFlag, 0);
            if (theStatus != 0) {
                theStatus = inErrorReporter(inPath, theStatus);
            }
            return (theStatus == 0);
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
        return ApplyT(inArgsPtr, inArgCount, theChmodFunc);
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
        MkdirFunctor theMkdirFunc(inMode, inCreateAllFlag);
        const bool   kNormalizePathFlag = false;
        return ApplyT(inArgsPtr, inArgCount, theMkdirFunc, kNormalizePathFlag);
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
            ostream&    inErrorStream,
            bool        /* inMoreThanOneFsFlag */ )
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
            const string& inDefaultCreateParams,
            bool          inMoveFlag      = false,
            bool          inOverwriteFlag = true)
            : mDestPtr(0),
              mDstName(),
              mBufferPtr(0),
              mDstDirStat(),
              mMoveFlag(inMoveFlag),
              mOverwriteFlag(inOverwriteFlag),
              mCheckDestFlag(true),
              mDefaultCreateParams(inDefaultCreateParams)
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
            Copier theCopier(
                inFs,
                theDstFs,
                inErrorReporter,
                theDstErrorReporter,
                mBufferPtr,
                kBufferSize,
                inFs == theDstFs ? &mDstDirStat : 0,
                mMoveFlag,
                mOverwriteFlag,
                mDefaultCreateParams
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
        const bool          mOverwriteFlag;
        bool                mCheckDestFlag;
        const string        mDefaultCreateParams;
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
        CpFunctor theCopyFunc(mDefaultCreateParams);
        FunctorT<CpFunctor, CopyGetlastEntry, true, false>
            theFunc(theCopyFunc, cerr);
        theCopyFunc.SetDest(theFunc.GetInit());
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
    static int AbsPath(
        FileSystem& inFs,
        string&     ioPath)
    {
        if (! ioPath.empty() && ioPath[0] == '/') {
            return 0;
        }
        string thePath;
        int theErr = inFs.GetCwd(thePath);
        if (theErr != 0) {
            return theErr;
        }
        if (*(thePath.rbegin()) != '/' && ! ioPath.empty()) {
            thePath += "/";
        }
        ioPath = thePath + ioPath;
        return 0;
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
        string      theUri("file://");
        int         theErr    = GetFs(theUri, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUri <<
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
        theUri = inArgsPtr[inArgCount - 1];
        thePath.clear();
        theFsPtr  = 0;
        theErr    = GetFs(theUri, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUri <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        if (theGlob.back().first != theFsPtr) {
            theGlob.resize(theGlob.size() + 1);
            theGlob.back().first = theFsPtr;
        }
        theErr = AbsPath(*theFsPtr, thePath);
        if (theErr) {
            inErrorStream << theUri <<
                ": " << FileSystem::GetStrError(theErr) << "\n";
            return theErr;
        }
        theGlob.back().second.push_back(thePath);
        const bool kOverwriteFlag = false;
        CpFunctor theCopyFunc(mDefaultCreateParams, inMoveFlag, kOverwriteFlag);
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
        string      theUri("file://");
        theErr = GetFs(theUri, theFsPtr, &thePath);
        if (theErr || ! theFsPtr) {
            inErrorStream << theUri <<
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
            if (thePath.empty() || thePath[0] != '/') {
                thePath = theDestPtr;
                theErr = AbsPath(*theFsPtr, thePath);
                if (theErr != 0) {
                    inErrorStream << theDestPtr <<
                        ": " << FileSystem::GetStrError(theErr) << "\n";
                    return theErr;
                }
            } else {
                if (*(thePath.rbegin()) != '/') {
                    thePath += "/";
                }
                thePath += theDestPtr;
            }
        } else {
            thePath = theDestPtr;
        }
        if (theGlob.back().first != theFsPtr) {
            theGlob.resize(theGlob.size() + 1);
            theGlob.back().first = theFsPtr;
        }
        theGlob.back().second.push_back(thePath);
        const bool kOverwriteFlag = false;
        CpFunctor theCopyFunc(mDefaultCreateParams, inMoveFlag, kOverwriteFlag);
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
            bool                       inRemoveSrcFlag,
            bool                       inOverwriteFlag,
            const string&              inDefaultCreateParams)
            : mSrcFs(inSrcFs),
              mDstFs(inDstFs),
              mSrcErrorReporter(inSrcErrorReporter),
              mDstErrorReporter(inDstErrorReporter),
              mOwnsBufferFlag(! inBufferPtr || inBufferSize <= 0),
              mBufferSize(mOwnsBufferFlag ? (6 << 20) : inBufferSize),
              mBufferPtr(mOwnsBufferFlag ? new char[mBufferSize] : inBufferPtr),
              mCreateParams(),
              mName(),
              mSrcName(),
              mDstName(),
              mSkipDirStatPtr(inSkipDirStatPtr),
              mRemoveSrcFlag(inRemoveSrcFlag),
              mOverwriteFlag(inOverwriteFlag),
              mDefaultCreateParams(inDefaultCreateParams)
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
                char* theEndPtr = mTmpBuf + kTmpBufSize;
                char* thePtr    = theEndPtr;
                if (inSrcStat.mNumStripes > 0) {
                    thePtr = IntToDecString(
                        (int)inSrcStat.mStriperType, thePtr);
                    *--thePtr = ',';
                    thePtr = IntToDecString(
                        inSrcStat.mStripeSize, thePtr);
                    *--thePtr = ',';
                    thePtr = IntToDecString(
                        inSrcStat.mNumRecoveryStripes, thePtr);
                    *--thePtr = ',';
                    thePtr = IntToDecString(
                        inSrcStat.mNumStripes, thePtr);
                    *--thePtr = ',';
                }
                thePtr = IntToDecString(inSrcStat.mNumReplicas, thePtr);
                if (thePtr < mTmpBuf) {
                    cerr << "CopyFile() internal error";
                    cerr.flush();
                    abort();
                }
                mCreateParams.assign(thePtr, theEndPtr);
            } else {
                mCreateParams = mDefaultCreateParams;
            }
            const int theDstFd = mDstFs.Open(
                inDstPath,
                O_WRONLY | O_CREAT |
                    (mOverwriteFlag ? O_TRUNC : O_EXCL),
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
            if (theStatus != 0) {
                // Attempt to remove the file in case of failure.
                const bool kRecursiveFlag = false;
                mDstFs.Remove(inDstPath, kRecursiveFlag, 0);
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
        // 5 comma separated 64 bit integers.
        enum { kTmpBufSize = (64 * 3 / 10 + 3) * 5 + 1 };

        FileSystem&                      mSrcFs;
        FileSystem&                      mDstFs;
        ErrorReporter&                   mSrcErrorReporter;
        ErrorReporter&                   mDstErrorReporter;
        const bool                       mOwnsBufferFlag;
        const size_t                     mBufferSize;
        char* const                      mBufferPtr;
        string                           mCreateParams;
        string                           mName;
        string                           mSrcName;
        string                           mDstName;
        const FileSystem::StatBuf* const mSkipDirStatPtr;
        const bool                       mRemoveSrcFlag;
        const bool                       mOverwriteFlag;
        const string                     mDefaultCreateParams;
        char                             mTmpBuf[kTmpBufSize];
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
        CpFunctor theMoveFunctor(mDefaultCreateParams, kMoveFlag);
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
              mApplicator(inFs, inPath, inErrorReporter, *this)
            {}
        int Run()
            { return mApplicator.Run(); }
        Count GetDirCount() const
            { return mDirCount; }
        Count GetFileCount() const
            { return mFileCount; }
        Count GetByteCount() const
            { return mByteCount; }
    private:
        typedef RecursiveApplicator<SubCounts> Applicator;
        friend class RecursiveApplicator<SubCounts>;

        Count      mDirCount;
        Count      mFileCount;
        Count      mByteCount;
        Applicator mApplicator;

        bool operator()(
            FileSystem&                /* inFs */,
            const string&              /* inPath */,
            const FileSystem::StatBuf& inStat,
            ErrorReporter&             /* inErrorReporter */)
        {
            if (S_ISDIR(inStat.st_mode)) {
                mDirCount++;
            } else {
                mFileCount++;
                mByteCount += max(Count(0), Count(inStat.st_size));
            }
            return true;
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
              mBufEndPtr(mBuf + sizeof(mBuf) / sizeof(mBuf[0])),
              mMaxEntrySize(0),
              mShowFsUriFlag(false)
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
            int theWidth;
            if (IsHumanReadable()) {
                theWidth = 13;
            } else {
                if (mMaxEntrySize > 0) {
                    theWidth = max(10, (int)(mBufEndPtr -
                        IntToDecString(mMaxEntrySize, mBufEndPtr)));
                } else {
                    theWidth = 10;
                }
                theWidth += 2;
            }
            for (DiskUtilizationEntries::const_iterator
                    theIt = mDiskUtilizationEntries.begin();
                    theIt != mDiskUtilizationEntries.end();
                    ++theIt) {
                Show(inFs, *theIt, theWidth);
            }
            return theStatus;
        }
        void SetShowFsUriFlag(
            bool inFlag)
            { mShowFsUriFlag = inFlag; }
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
        int64_t                     mMaxEntrySize;
        bool                        mShowFsUriFlag;

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
            mMaxEntrySize = max(mMaxEntrySize, theEntry.mSize);
        }
        void Show(
            FileSystem&                     inFs,
            const DiskUtilizationListEntry& inEntry,
            int                             inWidth)
        {
            if (! mOutStream) {
                return;
            }
            if (IsSummary()) {
                ShowPath(inFs, inEntry) << "\t" << right;
            } else {
                mOutStream << setw(inWidth) << left;
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
            if (mShowFsUriFlag) {
                mOutStream << inFs.GetUri();
            }
            return (
                mOutStream << inEntry.mPath <<
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
    class DiskUtilizationInitFunctor
    {
    public:
        DiskUtilizationInitFunctor(
            DiskUtilizationFunctor& inDuFunc)
            : mDuFunc(inDuFunc)
            {}
        bool operator()(
            int&              /* ioGlobError */,
            const GlobResult& /* inGlobResult */,
            ostream&          /* inErrorStream */,
            bool              inMoreThanOneFsFlag)
        {
            mDuFunc.SetShowFsUriFlag(inMoreThanOneFsFlag);
            return true;
        }
    private:
        DiskUtilizationFunctor& mDuFunc;
    private:
        DiskUtilizationInitFunctor(
            const DiskUtilizationInitFunctor& inFunct);
        DiskUtilizationInitFunctor& operator=(
            const DiskUtilizationInitFunctor& inFunct);
    };
    int DiskUtilization(
        char**                inArgsPtr,
        int                   inArgCount,
        DiskUtilizationFormat inFormat)
    {
        DiskUtilizationFunctor theDuFunc(inFormat, cout);
        FunctorT<DiskUtilizationFunctor, DiskUtilizationInitFunctor>
            theFunc(theDuFunc, cerr, theDuFunc);
        DiskUtilizationInitFunctor theDuInitFunc(theDuFunc);
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
        CountFunctor theCountFunc(inShowQuotaFlag, cout);
        return ApplyT(inArgsPtr, inArgCount, theCountFunc);
    }
    class TouchzFunctor
    {
    public:
        TouchzFunctor(
            const string& inCreateParams)
            : mTime(),
              mStat(),
              mStatus(0),
              mTimeSetFlag(false),
              mCreateParams(inCreateParams)
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
        TouchzFunctor theTouchzFunc(mDefaultCreateParams);
        return ApplyT(inArgsPtr, inArgCount, theTouchzFunc);
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
        SetModTimeFunctor theSetTimeFunc(inModTimeMs);
        return ApplyT(inArgsPtr, inArgCount, theSetTimeFunc);
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
        const string& inCreateParams,
        int           inOpenFlags    = O_CREAT | O_WRONLY | O_EXCL)
    {
        FileSystem*  theFsPtr = 0;
        string       thePath;
        int          theErr   = GetFs(inUri, theFsPtr, &thePath);
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
    class WaitReplicationFunctor
    {
    public:
        WaitReplicationFunctor(
            int      inReplication,
            bool     inRecursiveFlag,
            ostream& inProgressStream)
            : mReplication(inReplication),
              mRecursiveFlag(inRecursiveFlag),
              mProgressStream(inProgressStream)
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            const int theStatus = Wait(inFs, inPath);
            if (theStatus == -EISDIR && mRecursiveFlag) {
                Applicator theApplicator(inFs, inPath, inErrorReporter, *this);
                return theApplicator.Run();
            }
            return theStatus;
        }
    private:
        const int  mReplication;
        const bool mRecursiveFlag;
        ostream&   mProgressStream;

        typedef RecursiveApplicator<WaitReplicationFunctor> Applicator;
        friend class RecursiveApplicator<WaitReplicationFunctor>;

        bool operator()(
            FileSystem&                 inFs,
            const string&               inPath,
            const FileSystem::StatBuf&  inStat,
            ErrorReporter&              inErrorReporter)
        {
            if (S_ISDIR(inStat.st_mode)) {
                return true;
            }
            const int theStatus = Wait(inFs, inPath);
            if (theStatus != 0) {
                inErrorReporter(inPath, theStatus);
            }
            return true;
        }
        int Wait(
            FileSystem&   inFs,
            const string& inPath)
        {
            FileSystem::StatBuf theStat;
            int                 theMinReplication = 0;
            int                 theMaxReplication = 0;
            bool                theShowNameFlag   = true;
            for (; ;) {
                const int theStatus = inFs.GetReplication(
                    inPath, theStat, theMinReplication, theMaxReplication);
                if (theStatus < 0) {
                    return theStatus;
                }
                if (S_ISDIR(theStat.st_mode)) {
                    return -EISDIR;
                }
                if (theStat.mNumReplicas <= 0 ||
                        (theStat.mNumReplicas == theMinReplication &&
                        theStat.mNumReplicas == theMaxReplication)) {
                    break;
                }
                if (theShowNameFlag) {
                    mProgressStream << inFs.GetUri() << inPath <<
                        " " << theStat.mNumReplicas <<
                        " [" << theMinReplication << "," <<
                        theMaxReplication << "] ";
                    theShowNameFlag = false;
                }
                mProgressStream << "." << flush;
                sleep(1);
            }
            if (! theShowNameFlag) {
                mProgressStream << "\n";
            }
            return 0;
        }
    private:
        WaitReplicationFunctor(
            const WaitReplicationFunctor& inFunctor);
        WaitReplicationFunctor& operator=(
            const WaitReplicationFunctor& inFunctor);
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
        GlobResult theResult;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(
            inArgsPtr,
            inArgCount,
            cerr,
            theResult,
            theMoreThanOneFsFlag
        );
        SetReplicationFunctor theSetReplFunc(inReplication, inRecursiveFlag);
        FunctorT<SetReplicationFunctor> theFunc(theSetReplFunc, cerr);
        int theStatus = Apply(theResult, theMoreThanOneFsFlag, theErr, theFunc);
        if (theStatus != 0 || ! inWaitFlag) {
            return theStatus;
        }
        WaitReplicationFunctor theWaitFunc(
            inReplication, inRecursiveFlag, cout);
        FunctorT<WaitReplicationFunctor> theWFunc(theWaitFunc, cerr);
        return Apply(theResult, theMoreThanOneFsFlag, theErr, theWFunc);
    }
    class StatFunctor
    {
    public:
        StatFunctor(
            const char* inFormatPtr,
            ostream&    inStream)
            : mFormat((inFormatPtr && *inFormatPtr) ? inFormatPtr : "%y"),
              mOutStream(inStream),
              mStat(),
              mTime()
        {
            mTmBuf[0] = 0;
        }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& /* inErrorReporter */)
        {
            const int theStatus = inFs.Stat(inPath, mStat);
            if (theStatus != 0) {
                return theStatus;
            }
            for (const char* thePtr = mFormat.c_str(),
                        * const theEndPtr = thePtr + mFormat.size();
                    thePtr < theEndPtr;
                    ++thePtr) {
                if ((*thePtr & 0xff) != '%') {
                    mOutStream.put(*thePtr);
                    continue;
                }
                if (++thePtr >= theEndPtr) {
                    break;
                }
                const int theSym = *thePtr & 0xFF;
                switch (theSym) {
                    case 'b':
                        mOutStream << max(int64_t(0), (int64_t)mStat.st_size);
                        break;
                    case 'F':
                        mOutStream << (S_ISDIR(mStat.st_mode) ?
                            "directory" : "regular file");
                        break;
                    case 'n': {
                            const char* const theSPtr = inPath.c_str();
                            const char*       theNPtr =
                                theSPtr + inPath.size() - 1;
                            while (theSPtr < theNPtr && *theNPtr == '/') {
                                --theNPtr;
                            }
                            const char* const theEPtr = theNPtr;
                            while (theSPtr < theNPtr && theNPtr[-1] != '/') {
                                --theNPtr;
                            }
                            mOutStream.write(theNPtr, theEPtr - theNPtr + 1);
                        }
                        break;
                    case 'o':
                        mOutStream << mStat.st_blksize;
                        break;
                    case 'r':
                        mOutStream << (S_ISDIR(mStat.st_mode) ?
                            0 : max(1, (int)mStat.mNumReplicas));
                        break;
                    case 'y': {
                            const time_t theMTime = GetMTime(mStat);
                            if (mTmBuf[0] == 0 || mTime != theMTime) {
                                struct tm theLocalTime = {0};
                                localtime_r(&theMTime, &theLocalTime);
                                const size_t theLen = strftime(mTmBuf, kTmBufLen,
                                    "%Y-%m-%d %H:%M ", &theLocalTime);
                                if (theLen <= 0) {
                                    strncpy(mTmBuf, "-", kTmBufLen);
                                }
                                mTime = theMTime;
                            }
                            mOutStream << mTmBuf;
                        }
                        break;
                    case 'Y':
                        mOutStream << GetMTime(mStat);
                        break;
                    default:
                        mOutStream.put(*thePtr);
                        break;
                }
            }
            mOutStream << "\n";
            return 0;
        }
    private:
        enum { kTmBufLen = 128 };

        const string        mFormat;
        ostream&            mOutStream;
        FileSystem::StatBuf mStat;
        time_t              mTime;
        char                mTmBuf[kTmBufLen];

        StatFunctor(
            const StatFunctor& inFunctor);
        StatFunctor& operator=(
            const StatFunctor& inFunctor);
    };
    int Stat(
        char**      inArgsPtr,
        int         inArgCount,
        const char* inFormatPtr)
    {
        StatFunctor theStatFunc(inFormatPtr, cout);
        return ApplyT(inArgsPtr, inArgCount, theStatFunc);
    }
    class TailFunctor
    {
    public:
        TailFunctor(
            ostream& inOutStream,
            bool     inShowNameFlag,
            int64_t  inSize,
            char*    inBufPtr  = 0,
            size_t   inBufSize = 0)
            : mOutStream(inOutStream),
              mShowNameFlag(inShowNameFlag),
              mSize(inSize),
              mAllocBufFlag(! inBufPtr || inBufSize <= 0),
              mBufSize(mAllocBufFlag ? (6 << 20) : inBufSize),
              mBufPtr(mAllocBufFlag ? new char[mBufSize] : inBufPtr),
              mNextNamePrefixPtr(""),
              mStat()
            {}
        ~TailFunctor()
        {
            if (mAllocBufFlag) {
                delete [] mBufPtr;
            }
        }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& /* inErrorReporter*/)
        {
            const int theFd = inFs.Open(inPath, O_RDONLY, 0, 0);
            if (theFd < 0) {
                return theFd;
            }
            const int theStatus = inFs.Stat(theFd, mStat);
            if (theStatus != 0) {
                inFs.Close(theFd);
                return theStatus;
            }
            if (S_ISDIR(mStat.st_mode)) {
                inFs.Close(theFd);
                return -EISDIR;
            }
            int64_t theRet = inFs.Seek(
                theFd, -min(mStat.st_size, mSize), SEEK_END);
            if (theRet < 0) {
                inFs.Close(theFd);
                return (int)theRet;
            }
            theRet = 0;
            if (mShowNameFlag) {
                mOutStream << mNextNamePrefixPtr <<
                    "==> " << inFs.GetUri() << inPath << " <==\n";
                mNextNamePrefixPtr = "\n";
            }
            while (mOutStream) {
                const ssize_t theNRd = inFs.Read(theFd, mBufPtr, mBufSize);
                if (theNRd < 0) {
                    theRet = theNRd;
                    break;
                }
                if (theNRd == 0) {
                    break;
                }
                mOutStream.write(mBufPtr, (size_t)theNRd);
            }
            inFs.Close(theFd);
            return (int)theRet;
        }
    private:
        ostream&            mOutStream;
        const bool          mShowNameFlag;
        const int64_t       mSize;
        const bool          mAllocBufFlag;
        const size_t        mBufSize;
        char* const         mBufPtr;
        const char*         mNextNamePrefixPtr;
        FileSystem::StatBuf mStat;

        TailFunctor(
            const TailFunctor& inFunctor);
        TailFunctor& operator=(
            const TailFunctor& inFunctor);
    };
    int Tail(
        char** inArgsPtr,
        int    inArgCount,
        bool   inFollowFlag)
    {
        if (inArgCount <= 0) {
            return -EINVAL;
        }
        GlobResult theResult;
        bool       theMoreThanOneFsFlag = false;
        int theErr = Glob(
            inArgsPtr,
            inArgCount,
            cerr,
            theResult,
            theMoreThanOneFsFlag
        );
        TailFunctor theTailFunc(
            cout,
            theMoreThanOneFsFlag ||
                theResult.size() > 1 ||
                (! theResult.empty() && theResult.front().second.size() > 1),
            1 << 10,
            mIoBufferPtr,
            mIoBufferSize
        );
        FunctorT<TailFunctor> theFunc(theTailFunc, cerr);
        int theStatus;
        for (; ;) {
            theStatus = Apply(theResult, theMoreThanOneFsFlag, theErr, theFunc);
            if (inFollowFlag && theStatus == 0) {
                sleep(5);
            } else {
                break;
            }
        }
        return theStatus;
    }
    int Expunge()
    {
        FileSystem* theFsPtr = 0;
        int theStatus = GetFs(string(), theFsPtr);
        if (theStatus || ! theFsPtr) {
            cerr << FileSystem::GetStrError(theStatus) << "\n";
            return theStatus;
        }
        ErrorReporter theErrorReporter(*theFsPtr, cerr);
        Trash         theTrash(*theFsPtr, mConfig, kTrashCfgPrefix);
        return theTrash.Expunge(&theErrorReporter);
    }
    class RemoveFunctor
    {
    public:
        RemoveFunctor(
            bool              inSkipTrashFlag,
            bool              inRecursiveFlag,
            ostream*          inProgressStreamPtr,
            const Properties& inConfig)
            : mSkipTrashFlag(inSkipTrashFlag),
              mRecursiveFlag(inRecursiveFlag),
              mProgressStreamPtr(inProgressStreamPtr),
              mConfig(inConfig),
              mTrashPtr(0),
              mFsPtr(0),
              mStat(),
              mMessage()
            {}
        ~RemoveFunctor()
        {
            delete mTrashPtr;
        }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            if (mSkipTrashFlag) {
                return inFs.Remove(inPath, mRecursiveFlag, &inErrorReporter);
            }
            if (! mRecursiveFlag) {
                const int theStatus = inFs.Stat(inPath, mStat);
                if (theStatus != 0) {
                    return theStatus;
                }
                if (S_ISDIR(mStat.st_mode)) {
                    inErrorReporter(inPath,
                        "Please use -rmr to remove directories.");
                    return -EISDIR;
                }
            }
            if (mFsPtr != &inFs || ! mTrashPtr) {
                delete mTrashPtr;
                mTrashPtr = new Trash(inFs, mConfig, kTrashCfgPrefix); 
            }
            bool theMovedFlag = false;
            mMessage.clear();
            const int theStatus = mTrashPtr->MoveTo(
                inPath, theMovedFlag, &mMessage);
            if (theMovedFlag && mProgressStreamPtr) {
                (*mProgressStreamPtr) << "Moved to trash: " <<
                    inFs.GetUri() << inPath << "\n";
            }
            if (! mMessage.empty()) {
                inErrorReporter(inPath, mMessage.c_str());
            }
            mMessage.clear();
            return theStatus;
        }
    private:
        const bool          mSkipTrashFlag;
        const bool          mRecursiveFlag;
        ostream* const      mProgressStreamPtr;
        const Properties&   mConfig; 
        Trash*              mTrashPtr;
        FileSystem*         mFsPtr;
        FileSystem::StatBuf mStat;
        string              mMessage;

    private:
        RemoveFunctor(
            const RemoveFunctor& inFunctor);
        RemoveFunctor& operator=(
            const RemoveFunctor& inFunctor);
    };
    int Remove(
        char** inArgsPtr,
        int    inArgCount,
        bool   inSkipTrashFlag,
        bool   inRecursiveFlag)
    {
        RemoveFunctor theRemoveFunc(
            inSkipTrashFlag, inRecursiveFlag, &cout, mConfig);
        return ApplyT(inArgsPtr, inArgCount, theRemoveFunc);
    }
    int RunEmptier()
    {
        FileSystem* theFsPtr = 0;
        int theStatus = GetFs(string(), theFsPtr);
        if (theStatus || ! theFsPtr) {
            cerr << FileSystem::GetStrError(theStatus) << "\n";
            return theStatus;
        }
        ErrorReporter theErrorReporter(*theFsPtr, cerr);
        Trash         theTrash(*theFsPtr, mConfig, kTrashCfgPrefix);
        while ((theStatus = theTrash.RunEmptier(&theErrorReporter) == 0)) {
            const int theInterval = theTrash.GetEmptierIntervalSec();
            if (theInterval <= 0) {
                break;
            }
            sleep(theInterval);
        }
        return theStatus;
    }
    class UnzipFunctor : private ZlibInflate::Output
    {
    public:
        UnzipFunctor(
            ostream&    inOutStream,
            const char* inOutStreamNamePtr,
            size_t      inIoBufferSize,
            char*       inIoBufferPtr)
            : ZlibInflate::Output(),
              mOutStream(inOutStream),
              mOutStreamNamePtr(inOutStreamNamePtr ? inOutStreamNamePtr : ""),
              mIoBufferSize(inIoBufferSize),
              mIoBufferPtr(inIoBufferPtr),
              mOutBufferPtr(0),
              mStatus(0),
              mZlibInflate()
        {
            if (mIoBufferSize < 2 || ! inIoBufferPtr) {
                cerr << "UnzipFunctor: internal error\n";
                cerr.flush();
                abort();
            }
        }
        ~UnzipFunctor()
            { delete [] mOutBufferPtr; }
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& inErrorReporter)
        {
            if (! mOutStream) {
                if (mStatus == 0) {
                    mStatus = errno;
                    if (mStatus > 0) {
                        mStatus = -mStatus;
                    } else if (mStatus == 0) {
                        mStatus = -EIO;
                    }
                }
                return mStatus;
            }
            mStatus = 0;
            const int theFd = inFs.Open(inPath, O_RDONLY, 0);
            if (theFd < 0) {
                return theFd;
            }
            int       theStatus      = 0;
            bool      theInflateFlag = true;
            int       theFmtPos      = 0;
            const int kFmtSize       = 2;
            bool      theDoneFlag    = true;
            for (; ;) {
                const ssize_t thePos   = theFmtPos < kFmtSize ? theFmtPos : 0;
                ssize_t       theNRead = inFs.Read(
                    theFd, mIoBufferPtr + thePos, mIoBufferSize - thePos);
                if (theNRead == 0 && thePos <= 0) {
                    break;
                }
                if (theNRead < 0) {
                    theStatus = (int)theNRead;
                    break;
                }
                if (theFmtPos < kFmtSize) {
                    if ((mIoBufferPtr[0] & 0xFF) != 0x1f ||
                            (kFmtSize <= theNRead + thePos &&
                            (mIoBufferPtr[1] & 0xFF) != 0x8b) ||
                            theNRead <= 0) {
                        theFmtPos      = kFmtSize;
                        theInflateFlag = false;
                    } else if (kFmtSize <= theNRead + thePos) {
                        theFmtPos = kFmtSize;
                    } else {
                        theFmtPos++;
                        continue;
                    }
                }
                theNRead += thePos;
                if (theInflateFlag) {
                    theDoneFlag = false;
                    theStatus = mZlibInflate.Run(mIoBufferPtr, theNRead,
                        *this, theDoneFlag);
                    if (theStatus != 0) {
                        if (mStatus == 0) {
                            inErrorReporter(
                                inPath, mZlibInflate.StrError(theStatus));
                            theStatus = -EINVAL;
                            break;
                        }
                    }
                } else if (! mOutStream.write(mIoBufferPtr, theNRead)) {
                    mStatus   = errno;
                    theStatus = mStatus;
                    break;
                }
            }
            inFs.Close(theFd);
            if (theStatus == 0) {
                theStatus = mStatus;
            }
            if (! theDoneFlag && theStatus == 0) {
                inErrorReporter(inPath, "zlib: unexpected enf of file");
                theStatus = -EINVAL;
            }
            if (theStatus != 0) {
                mZlibInflate.Reset();
            }
            return theStatus;
        }
    private:
        ostream&          mOutStream;
        const char* const mOutStreamNamePtr;
        const size_t      mIoBufferSize;
        char* const       mIoBufferPtr;
        char*             mOutBufferPtr;
        int               mStatus;
        ZlibInflate       mZlibInflate;

        virtual int GetBuffer(
            char*&  outBufferPtr,
            size_t& outBufferSize)
        {
            if (! mOutBufferPtr) {
                mOutBufferPtr = new char[mIoBufferSize];
            }
            outBufferPtr  = mOutBufferPtr;
            outBufferSize = mIoBufferSize;
            return 0;
        }
        virtual int Write(
            const char* inBufferPtr,
            size_t      inBufferSize)
        {
            if (mStatus == 0 &&
                    ! mOutStream.write(inBufferPtr, inBufferSize)) {
                mStatus = errno;
            }
            return mStatus;
        }
    private:
        UnzipFunctor(
            const UnzipFunctor& inFunctor);
        UnzipFunctor& operator=(
            const UnzipFunctor& inFunctor);
    };
    int Unzip(
        char** inArgsPtr,
        int    inArgCount)
    {
        UnzipFunctor theFunc(cout, "stdout", mIoBufferSize, mIoBufferPtr);
        return ApplyT(inArgsPtr, inArgCount, theFunc);
    }
    class FullStatFunc
    {
    public:
        FullStatFunc(
            ostream& inOutStream)
            : mOutStream(inOutStream),
              mStat()
            {}
        int operator()(
            FileSystem&    inFs,
            const string&  inPath,
            ErrorReporter& /* inErrorReporter */)
        {
            const int theErr = inFs.Stat(inPath, mStat);
            if (theErr != 0) {
                return theErr;
            }
            mOutStream <<
                "Uri:              " << inFs.GetUri() << inPath     << "\n"
                "Type:             " << (S_ISDIR(mStat.st_mode) ?
                    "dir" : "file") << "\n"
                "Created:          " << GetCTime(mStat)             << "\n"
                "Modified:         " << GetMTime(mStat)             << "\n"
                "Size:             " << mStat.st_size               << "\n"
                "I-node:           " << mStat.st_ino                << "\n"
                "Mode:             " << oct << mStat.st_mode << dec << "\n"
                "Owner:            " << mStat.st_uid                << "\n"
                "Group:            " << mStat.st_gid                << "\n"
            ;
            if (S_ISDIR(mStat.st_mode)) {
                mOutStream <<
                "Files:            " << mStat.mSubCount1 << "\n"
                "Dirs:             " << mStat.mSubCount2 << "\n"
                ;
            } else {
                mOutStream <<
                "Chunks:           " << mStat.mSubCount1          << "\n"
                "Replicas:         " << mStat.mNumReplicas        << "\n"
                "Data stripes:     " << mStat.mNumStripes         << "\n"
                "Recovery stripes: " << mStat.mNumRecoveryStripes << "\n"
                "Striper type:     " << mStat.mStriperType        << "\n"
                "Stripe size:      " << mStat.mStripeSize         << "\n"
                ;
            }
            return theErr;
        }
    private:
        ostream&            mOutStream;
        FileSystem::StatBuf mStat;
    };
    int FullStat(
        char** inArgsPtr,
        int    inArgCount)
    {
        FullStatFunc theFullStatFunc(cout);
        FunctorT<FullStatFunc> theFunc(theFullStatFunc, cerr);
        return Apply(inArgsPtr, inArgCount, theFunc);
    }
private:
    size_t const mIoBufferSize;
    char* const  mIoBufferPtr;
    string       mDefaultCreateParams;
    Properties   mConfig;
private:
    KfsTool(const KfsTool& inTool);
    KfsTool& operator=(const KfsTool& inTool);
};

const char* const KfsTool::sHelpStrings[] =
{
    "v", "",
    "verbose -- set qfs log level debug",

    "cfg", "<configuration file name>",
    "Qfs configuration: key value pairs separated with = like\n\t\t"
    "with -D option below",

    "D", "key=value",
    "Define qfs key value configuration pair\n\t\t"
    "fs.default               = local\n\t\t\t"
        "default file system url\n\t\t"
    "fs.euser                 = <effective user id>\n\t\t\t"
        "set effective user id with qfs\n\t\t"
    "fs.egroup                = <effective group id>\n\t\t\t"
        "set effective group id with qfs\n\t\t"
    "fs.createParams          = S\n\t\t\t"
        "qfs file create parameters: 1,6,3,65536,2\n\t\t\t"
        "<replication>,<data stripe count>,<recovery stripe count>,"
        "<stripe size>,<striper type>\n\t\t\t"
        "S is shortcut for 1,6,3,65536,2\n\t\t\t"
        "data stripe count:     from 1 to 64 with Reed Solomon encoding (RS),"
            " and up to 511 with no recovery\n\t\t\t"
        "recovery stripe count: 0, or 3 with RS\n\t\t\t"
        "stripe size:           from 4096 to 67108864 in 4096 increments"
            "\n\t\t\t"
        "stiper type:           1 no stiping, or 2 is RS\n\t\t"
    "fs.trash.trash           = .Trash\n\t\t\t"
        "trash directory name\n\t\t"
    "fs.trash.homesPrefix     = /user\n\t\t\t"
        "qfs (non local) homes directories pfefix\n\t\t"
    "fs.trash.current         = Current\n\t\t\t"
        "trash current directory name\n\t\t"
    "fs.trash.interval        = 60\n\t\t\t"
        "trash emptier interval in sec\n\t\t"
    "fs.trash.minPathDepth    = 5\n\t\t\t"
        "min. path depth permitted to be moved to trash\n\t\t\t"
        "without dfs.force.remove=true set\n\t\t"
    "dfs.force.remove         = false\n\t\t\t"
        "see the above\n\t\t"
    "fs.msgLogWriter.logLevel = INFO\n\t\t\t"
        "trace log level: {DEBUG|INFO|NOTICE|WARN|ERROR|FATAL}\n",

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
    "files, the destination must be a directory.\n\t\t"
    "-crc and -ignoreCrc options are ignored.\n",
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
    "Identical to the -get command.\n\t\t"
    "-crc and -ignoreCrc options are ignored.\n",

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
    "The only allowed format is zip.\n",

    "setModTime", "<src> <time>",
    "Set modification time <time in milliseconds>"
    " on <src>\n",

    "stat", "[format] <path>",
    "Print statistics about the file/directory at <path>\n\t\t"
    "in the specified format. Format accepts filesize in bytes (%b),"
    " filename (%n),\n\t\t"
    "block size (%o), replication (%r), modification date (%y, %Y)\n\t\t"
    "file type (%F)\n",

    "astat", "<glob> [<glob>...]",
    "displays all attributes",

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
    "QUOTA REMAINING_QUATA SPACE_QUOTA REMAINING_SPACE_QUOTA\n\t\t"
    "\tDIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME\n",

    "expunge", "",
    "Expunge user's trash by deleting all trash checkpoints except the\n\t\t"
    "most recent one.\n",

    "runEmptier", "[interval in seconds]",
    "run trash emptier forever",

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
