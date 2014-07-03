//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/03/02
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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
// Double buffered message log writer implementation.
//
//----------------------------------------------------------------------------

#include "BufferedLogWriter.h"
#include "Properties.h"

#include "qcdio/QCMutex.h"
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCThread.h"

#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <dirent.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>
#include <string.h>

#include <vector>
#include <string>
#include <sstream>
#include <iomanip>
#include <iostream>

namespace KFS
{

using std::string;
using std::max;
using std::min;
using std::ostringstream;
using std::ostream;
using std::streambuf;
using std::setw;
using std::setfill;
using std::streamsize;
using std::vector;

const char* const kBufferedLogWriter_LogLevels[] = {
    "FATAL",
    "ALERT",
    "CRIT",
    "ERROR",
    "WARN",
    "NOTICE",
    "INFO",
    "DEBUG",
    "NOTSET"
};

const int64_t kLogWriterMinLogFileSize               = 16 << 10;
const int64_t kLogWriterMinOpenRetryIntervalMicroSec = 10000;
const int     kLogWriterMinLogBufferSize             = 16 << 10;
const int64_t kLogWirterDefaultTimeToKeepSecs        = 60 * 60 * 24 * 30;
const int     kLogWriterDefaulOpenFlags              =
    O_CREAT | O_APPEND | O_WRONLY /* | O_SYNC */;

class BufferedLogWriter::Impl : public QCRunnable
{
public:
    Impl(
        int         inFd,
        const char* inFileNamePtr,
        int         inBufSize,
        const char* inTrucatedSuffixPtr,
        int64_t     inOpenRetryIntervalMicroSec,
        int64_t     inFlushIntervalMicroSec,
        int64_t     inMaxLogFileSize,
        int         inMaxLogsFiles,
        int64_t     inMaxLogWaitTimeMicroSec,
        const char* inTimeStampFormatPtr,
        bool        inUseGMTFlag)
        : mMutex(),
          mWriteCond(),
          mWriteDoneCond(),
          mThread(0, "LogWriter"),
          mLogFileNamePrefixes(),
          mFileName(inFileNamePtr ? inFileNamePtr : ""),
          mTruncatedSuffix(inTrucatedSuffixPtr ? inTrucatedSuffixPtr : "..."),
          mTimeStampFormat(inTimeStampFormatPtr ?
            inTimeStampFormatPtr : "%m-%d-%Y %H:%M:%S"),
          mNewLogSuffix(),
          mDroppedCount(0),
          mTotalDroppedCount(0),
          mTruncatedCount(0),
          mBufWaitersCount(0),
          mWriteErrCount(0),
          mWriteBytesDiscardedCount(0),
          mCurWritten(0),
          mBufWaitedCount(0),
          mMaxLogFileSize(inMaxLogFileSize <= 0 ? inMaxLogFileSize :
            max(kLogWriterMinLogFileSize, inMaxLogFileSize)),
          mMaxLogFiles(inMaxLogFileSize > 0 ? inMaxLogsFiles : -1),
          mNextOpenRetryTime(Now() - Seconds(10)),
          mOpenRetryInterval(max(kLogWriterMinOpenRetryIntervalMicroSec,
            inOpenRetryIntervalMicroSec)),
          mFlushInterval(max((int64_t)0, inFlushIntervalMicroSec)),
          mNextFlushTime(-1),
          mTotalLogWaitedTime(0),
          mCurLogWatedTime(0),
          mMaxLogWaitTime(inMaxLogWaitTimeMicroSec),
          mNextDeleteOldLogsTimeSec(0),
          mMinModTimeToKeepSec(kLogWirterDefaultTimeToKeepSecs),
          mOpenFlags(kLogWriterDefaulOpenFlags),
          mOpenMode(0644),
          mBufSize(max(inBufSize > 0 ? inBufSize : (1 << 20),
            (int)mTruncatedSuffix.size() + kLogWriterMinLogBufferSize)),
          mLastError(0),
          mFd(inFd >= 0 ? dup(inFd) : inFd),
          mMaxAppendLength(mBufSize - mTruncatedSuffix.size() + 1),
          mRunFlag(false),
          mDeleteOldLogsFlag(false),
          mBuf0Ptr(new char [mBufSize * 2]),
          mBuf1Ptr(mBuf0Ptr + mBufSize),
          mBufPtr(mBuf0Ptr),
          mCurPtr(mBufPtr),
          mEndPtr(mBuf0Ptr + mBufSize),
          mWritePtr(0),
          mWriteEndPtr(0),
          mCloseFlag(false),
          mUseGMTFlag(inUseGMTFlag),
          mTimeSec(0),
          mLogTimeStampSec(0),
          mMsgAppendCount(0),
          mRotateLogsDailyFlag(true),
          mMaxMsgStreamCount(256),
          mMsgStreamCount(0),
          mMsgStreamHeadPtr(0)
    {
        if (! mFileName.empty()) {
            mLogFileNamePrefixes.push_back(mFileName);
        }
        mLogTimeStampPrefixStr[0] = 0;
        int64_t theSec      = 0;
        int64_t theMicroSec = 0;
        Now(theSec, theMicroSec);
        mTimeSec         = theSec - 1;
        mLogTimeStampSec = mTimeSec;
        UpdateTimeTm(theSec);
        mLastLogTm       = mTimeTm;
        GetLogTimeStampPrefixPtr(theSec);
        mNextFlushTime = Seconds(theSec) + theMicroSec + mFlushInterval;
    }
    static const char* GetLogLevelNamePtr(
        LogLevel inLogLevel)
    {
        const int theLogLevel = inLogLevel / 100;
        return (
            (theLogLevel < 0 || theLogLevel >=
                int(sizeof(kBufferedLogWriter_LogLevels) /
                    sizeof(kBufferedLogWriter_LogLevels[0]))) ?
            "INVALID" :  kBufferedLogWriter_LogLevels[theLogLevel]
        );
    }
    virtual ~Impl()
    {
        Impl::Stop();
        delete [] mBuf0Ptr;
        while (mMsgStreamHeadPtr) {
            QCASSERT(mMsgStreamCount > 0);
            MsgStream* const thePtr = mMsgStreamHeadPtr;
            mMsgStreamHeadPtr = thePtr->Next();
            delete thePtr;
            mMsgStreamCount--;
        }
    }
    void SetParameters(
        string            inPropsPrefix,
        const Properties& inProps)
    {
        QCStMutexLocker theLocker(mMutex);
        mTruncatedSuffix = inProps.getValue(
            "truncatedSuffix", mTruncatedSuffix.c_str());
        if (mTruncatedSuffix.empty()) {
            mTruncatedSuffix = "...";
        }
        mBufSize = inProps.getValue(
            inPropsPrefix + "bufferSize", mBufSize);
        mBufSize = max(
            mBufSize <= 0 ? (1 << 20) : mBufSize,
            (int)mTruncatedSuffix.size() + (16 << 10)
        );
        mOpenRetryInterval   = max((Time)kLogWriterMinOpenRetryIntervalMicroSec,
            (Time)inProps.getValue(
            inPropsPrefix + "openRetryIntervalMicroSec",
            (double)mOpenRetryInterval));
        mMaxLogFileSize      = max(kLogWriterMinLogFileSize,
            (int64_t)inProps.getValue(
            inPropsPrefix + "maxLogFileSize", (double)mMaxLogFileSize));
        mMaxLogFiles         = mMaxLogFileSize <= 0 ? -1 : (int)inProps.getValue(
            inPropsPrefix + "maxLogFiles", (double)mMaxLogFiles);
        mUseGMTFlag          = inProps.getValue(
            inPropsPrefix + "useGMT", mUseGMTFlag ? 1 : 0) != 0;
        mTimeStampFormat     = inProps.getValue(
            inPropsPrefix + "timeStampFormat", mTimeStampFormat);
        mMinModTimeToKeepSec = (int64_t)inProps.getValue(
            inPropsPrefix + "minOldLogModTimeSec",
            (double)mMinModTimeToKeepSec);
        mMaxMsgStreamCount   = inProps.getValue(
            inPropsPrefix + "maxMsgStreamCount",
            mMaxMsgStreamCount);
        mRotateLogsDailyFlag = inProps.getValue(
            inPropsPrefix + "rotateLogsDaily",
            mRotateLogsDailyFlag ? 1 : 0) != 0;
        string theLogFilePrefixes;
        for (LogFileNames::const_iterator theIt =
                mLogFileNamePrefixes.begin();
                theIt != mLogFileNamePrefixes.end();
                ++theIt) {
            if (theIt != mLogFileNamePrefixes.begin()) {
                theLogFilePrefixes += ":";
            }
            theLogFilePrefixes += *theIt;
        }
        theLogFilePrefixes = inProps.getValue(
            inPropsPrefix + "logFilePrefixes", theLogFilePrefixes);
        ClearLogFileNamePrefixes();
        size_t theStart = 0;
        while (theStart < theLogFilePrefixes.length()) {
            const size_t theEnd = theLogFilePrefixes.find(':', theStart);
            if (theStart != theEnd) {
                AddLogFileNamePrefix(theLogFilePrefixes.substr(theStart,
                    theEnd == string::npos ? string::npos : theEnd - theStart));
            }
            if (theEnd == string::npos) {
                break;
            }
            theStart = theEnd + 1;
        }
        if (! mLogFileNamePrefixes.empty()) {
            if (mFileName.empty()) {
                if (mFd >= 0) {
                    ::close(mFd);
                }
                mFd = -1;
                mFileName = mLogFileNamePrefixes.front();
            } else {
                LogFileNames::const_iterator theIt;
                for (theIt = mLogFileNamePrefixes.begin();
                        theIt != mLogFileNamePrefixes.end() &&
                            *theIt != mFileName;
                        ++theIt)
                    {}
                if (theIt == mLogFileNamePrefixes.end()) {
                    mCloseFlag = true;
                    mFileName  = mLogFileNamePrefixes.front();
                }
            }
        }
        SetFlushInterval((int64_t)inProps.getValue(
            inPropsPrefix + "flushIntervalMicroSec", (double)mFlushInterval)
        );
        SetMaxLogWaitTime((int64_t)inProps.getValue(
            inPropsPrefix + "waitMicroSec", (double)mMaxLogWaitTime)
        );
        while (mMsgStreamHeadPtr && mMaxMsgStreamCount < mMsgStreamCount) {
            QCASSERT(mMsgStreamCount > 0);
            MsgStream* const thePtr = mMsgStreamHeadPtr;
            mMsgStreamHeadPtr = thePtr->Next();
            delete thePtr;
            mMsgStreamCount--;
        }

    }
    void Stop()
    {
        QCStMutexLocker theLocker(mMutex);
        if (! mRunFlag) {
            return;
        }
        FlushSelf();
        mRunFlag = false;
        mWriteCond.Notify();
        theLocker.Unlock();
        mThread.Join();
    }
    void Start()
    {
        QCStMutexLocker theLocker(mMutex);
        if (mRunFlag) {
            return;
        }
        mRunFlag = true;
        const int kStackSize = 32 << 10;
        mThread.Start(this, kStackSize, "LogWriter");
    }
    bool Reopen()
    {
        QCStMutexLocker theLocker(mMutex);
        if (mFileName.empty() || mFd < 0) {
            return false;
        }
        mCloseFlag = true;
        return true;
    }
    int Open(
        const char* inFileNamePtr,
        int         inFlags,
        int         inMode,
        bool        inOpenHereFlag)
    {
        if (! inFileNamePtr || ! *inFileNamePtr) {
            return -EINVAL;
        }
        int theFd  = -1;
        int theErr = -1;
        while (inOpenHereFlag &&
                (theFd = ::open(inFileNamePtr, inFlags, inMode)) < 0) {
            theErr = errno;
            if (theErr != EINTR && theErr != EAGAIN) {
                break;
            }
        }
        if (inOpenHereFlag) {
            if (theFd < 0) {
                return theErr;
            }
            ::fcntl(theFd, F_SETFD, FD_CLOEXEC);
        }
        QCStMutexLocker theLocker(mMutex);
        if (theFd >= 0) {
            int64_t theSec      = 0;
            int64_t theMicroSec = 0;
            Now(theSec, theMicroSec);
            UpdateTimeTm(theSec);
            mLastLogTm = mTimeTm;
        }
        mNextOpenRetryTime = Now() - Seconds(10);
        mFileName          = inFileNamePtr;
        mOpenFlags         = inFlags;
        mOpenMode          = inMode;
        mFd                = theFd;
        AddLogFileNamePrefix(mFileName);
        return 0;
    }
    bool AddLogFileNamePrefix(
        string inName)
    {
        if (inName.empty()) {
            return false;
        }
        QCStMutexLocker theLocker(mMutex);
        for (LogFileNames::const_iterator theIt =
                mLogFileNamePrefixes.begin();
                theIt != mLogFileNamePrefixes.end();
                ++theIt) {
            if (*theIt == inName) {
                return false;
            }
        }
        mLogFileNamePrefixes.push_back(inName);
        return true;
    }
    void ClearLogFileNamePrefixes()
    {
        QCStMutexLocker theLocker(mMutex);
        mLogFileNamePrefixes.clear();
        if (! mFileName.empty()) {
            mLogFileNamePrefixes.push_back(mFileName);
        }
    }
    void Close()
    {
        QCStMutexLocker theLocker(mMutex);
        if (mFd >= 0) {
            ::close(mFd);
        }
        mFd = -1;
        mFileName.clear();
    }
    void Flush()
    {
        QCStMutexLocker theLocker(mMutex);
        FlushSelf();
    }
    void Sync()
    {
        QCStMutexLocker theLocker(mMutex);
        while (! FlushSelf() || mWritePtr) {
            mBufWaitersCount++;
            mWriteDoneCond.Wait(mMutex);
            mBufWaitersCount--;
        }
    }
    void SetMaxLogWaitTime(
        int64_t inMaxLogWaitTimeMicroSec)
    {
        QCStMutexLocker theLocker(mMutex);
        const Time theMaxLogWaitTime = max((Time)0, inMaxLogWaitTimeMicroSec);
        if (mMaxLogWaitTime != theMaxLogWaitTime) {
            const bool theWakeupFlag =
                mMaxLogWaitTime > theMaxLogWaitTime;
            mMaxLogWaitTime = theMaxLogWaitTime;
            if (theWakeupFlag) {
                mWriteDoneCond.NotifyAll();
            }
        }
    }
    void SetFlushInterval(
        int64_t inMicroSecs)
    {
        QCStMutexLocker theLocker(mMutex);
        const Time theInterval = max(int64_t(0), inMicroSecs);
        if (mFlushInterval == theInterval) {
            return;
        }
        mNextFlushTime -= mFlushInterval;
        mFlushInterval = theInterval;
        mNextFlushTime += mFlushInterval;
    }
    void GetCounters(
        Counters& outCounters)
    {
        QCStMutexLocker theLocker(mMutex);
        outCounters.mAppendCount         = mMsgAppendCount;
        outCounters.mDroppedCount        = mTotalDroppedCount;
        outCounters.mWriteErrorCount     = mWriteErrCount;
        outCounters.mAppendWaitCount     = mBufWaitedCount;
        outCounters.mAppendWaitMicroSecs = mTotalLogWaitedTime;
    }
    void PrepareToFork()
        { mMutex.Lock(); }
    void ForkDone()
        { mMutex.Unlock(); }
    void ChildAtFork()
    {
        mRunFlag = false;
        if (mFd >= 0) {
            close(mFd);
            mFd = -1;
        }
    }
    void Append(
        LogLevel inLogLevel,
        Writer&  inWriter)
    {
        if (! mRunFlag) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        static va_list theArgs; // dummy
        AppendSelf(inLogLevel, &inWriter, 0, "", theArgs);
    }
    void Append(
        LogLevel    inLogLevel,
        const char* inFmtStrPtr,
        va_list     inArgs)
    {
        if (! mRunFlag) {
            return;
        }
        QCStMutexLocker theLocker(mMutex);
        AppendSelf(inLogLevel, 0, -1, inFmtStrPtr, inArgs);
    }
    void AppendSelf(
        LogLevel    inLogLevel,
        const char* inStrPtr,
        int         inStrLen)
    {
        static va_list theArgs; // dummy
        AppendSelf(inLogLevel, 0, max(0, inStrLen), inStrPtr, theArgs);
    }
    void AppendSelf(
        LogLevel    inLogLevel,
        Writer*     inWriterPtr,
        int         inStrLen,
        const char* inFmtStrPtr,
        va_list     inArgs)
    {
        mMsgAppendCount++;

        // The time includes mutex wait, but guarantees that the time stamps in
        // the log are monotonically increasing. In the case of log wait this
        // holds only in the case when thread library services the condition
        // queue in FIFO order.
        int64_t      theSec             = 0;
        int64_t      theMicroSec        = 0;
        bool         theGetTimeFlag     = true;
        const bool   theWasFlushingFlag = IsFlushing();
        const size_t kAvgPrefSize       = 64;
        const int    kAvgMsgSize        = 256;
        size_t       theBufSize         = min(GetMaxRecordSize(),
            kAvgPrefSize + (inWriterPtr ? inWriterPtr->GetMsgLength() : 0) +
            (inStrLen >= 0 ? inStrLen : kAvgMsgSize));
        Time       theTimeWaited      = 0;
        for (int i = 0; ; i++) {
            while (mCurPtr + theBufSize >= mEndPtr && ! FlushSelf()) {
                if (theTimeWaited >= mMaxLogWaitTime || ! mRunFlag || i >= 4 ||
                        (size_t)mBufSize < theBufSize + 1) {
                    mDroppedCount++;
                    mTotalDroppedCount++;
                    theBufSize = 0;
                    break;
                }
                if (theGetTimeFlag) {
                    Now(theSec, theMicroSec);
                    theGetTimeFlag = false;
                }
                mBufWaitersCount++;
                const QCMutex::Time theTimeoutNanoSecs =
                    NanoSec(mMaxLogWaitTime - theTimeWaited);
                mWriteDoneCond.Wait(mMutex, theTimeoutNanoSecs);
                mBufWaitersCount--;
                mBufWaitedCount++;
                mCurLogWatedTime    -= theTimeWaited;
                mTotalLogWaitedTime -= theTimeWaited;
                theTimeWaited = Now() - (Seconds(theSec) + theMicroSec);
                mTotalLogWaitedTime += theTimeWaited;
                mCurLogWatedTime    += theTimeWaited;
            }
            if (theBufSize <= 0) {
                break;
            }
            if (theGetTimeFlag) {
                Now(theSec, theMicroSec);
                theGetTimeFlag = false;
            }
            const size_t theLen = MsgPrefix(theSec, theMicroSec, inLogLevel);
            if (theLen <= 0) {
                theBufSize = kAvgMsgSize + mEndPtr - mCurPtr;
                continue;
            }
            const size_t theMaxLen =
                min(GetMaxRecordSize(), (size_t)(mEndPtr - mCurPtr));
            if (theMaxLen <= theLen + 1) {
                theBufSize = max(theBufSize, theLen + 1);
                continue;
            }
            size_t theMaxMsgLen = theMaxLen - theLen; // with \0
            int theRet = inWriterPtr ?
                inWriterPtr->Write(mCurPtr + theLen, theMaxMsgLen) :
                0;
            if (inStrLen < 0) {
                if (theMaxMsgLen > 0) {
                    theRet += ::vsnprintf(
                        mCurPtr + theLen, theMaxMsgLen, inFmtStrPtr, inArgs);
                }
            } else {
                theRet += inStrLen;
                if ((size_t)inStrLen < theMaxMsgLen ||
                        theMaxLen >= GetMaxRecordSize()) {
                    memcpy(mCurPtr + theLen, inFmtStrPtr,
                        min((size_t)inStrLen, theMaxMsgLen));
                }
            }
            if (theRet < 0 || theRet >= (int)theMaxMsgLen) {
                if (theMaxLen >= GetMaxRecordSize()) {
                    mTruncatedCount++;
                    mCurPtr += theMaxLen;
                    ::memcpy(mCurPtr - theMaxLen,
                        mTruncatedSuffix.c_str(), mTruncatedSuffix.size());
                    mCurPtr[-1] = '\n';
                    mDroppedCount = 0;
                    mCurLogWatedTime = 0;
                    break;
                }
                if (theRet < 0 && i >= 2) {
                    mDroppedCount++;
                    mTotalDroppedCount++;
                    break;
                }
                theBufSize = theLen + theRet + 32;
            } else {
                mCurPtr += theLen + theRet;
                QCASSERT(mCurPtr < mEndPtr);
                if (mCurPtr < mEndPtr) {
                    *mCurPtr++ = '\n';
                }
                mDroppedCount = 0;
                mCurLogWatedTime = 0;
                break;
            }
        }
        if (! theGetTimeFlag) {
            if (! theWasFlushingFlag) {
                // Call even if it is now flushing, to update the next flush
                // time.
                FlushIfNeeded(theSec, theMicroSec);
            }
            RunTimers(theSec, theMicroSec);
        }
        if (0 < mBufWaitersCount) {
            mWriteDoneCond.Notify(); // Wake next thread.
        }
    }
    void Run()
    {
        QCStMutexLocker theLocker(mMutex);
        QCASSERT(
            mBufSize > 0 &&
            mBuf0Ptr &&
            mBuf1Ptr
        );
        for (; ;) {
            while (mRunFlag && ! mWritePtr) {
                const QCMutex::Time theTimeoutNanoSecs = NanoSec(max(
                    QCMutex::Time(10000),
                    (QCMutex::Time)(mFlushInterval > 0 ?
                        mFlushInterval / 2 : Time(500000))
                ));
                mWriteCond.Wait(mMutex, theTimeoutNanoSecs);
                if (mWritePtr) {
                    break;
                }
                int64_t theSec      = 0;
                int64_t theMicroSec = 0;
                Now(theSec, theMicroSec);
                FlushIfNeeded(theSec, theMicroSec);
                RunTimers(theSec, theMicroSec);
                if (mDeleteOldLogsFlag && mMaxLogFiles > 0) {
                    mDeleteOldLogsFlag = false;
                }
                if (mDeleteOldLogsFlag || mWritePtr) {
                    break;
                }
            }
            if (! mWritePtr && ! mRunFlag) {
                QCASSERT(mBufWaitersCount <= 0);
                break;
            }
            if (mCloseFlag && mFd >= 0 && ! mFileName.empty()) {
                const int theFd = mFd;
                mFd                = -1;
                mCurWritten        = 0;
                mNextOpenRetryTime = Now() - Seconds(10);
                QCStMutexUnlocker theUnlocker(mMutex);
                ::close(theFd);
            }
            mCloseFlag = false;
            if (mFd >= 0 && ! mFileName.empty()) {
                if (mMaxLogFiles > 0) {
                    if ((int64_t)mCurWritten > mMaxLogFileSize) {
                        const int    theFd          = mFd;
                        const string theFileName    = mFileName;
                        const int    theMaxLogFiles = mMaxLogFiles;
                        mFd                = -1;
                        mCurWritten        = 0;
                        mNextOpenRetryTime = Now() - Seconds(10);
                        {
                            QCStMutexUnlocker theUnlocker(mMutex);
                            ::close(theFd);
                        }
                        RotateLogs(theFileName, theMaxLogFiles, mMutex);
                    }
                } else if (! mNewLogSuffix.empty() || mDeleteOldLogsFlag) {
                    const string  theNewLogSuffix        = mNewLogSuffix;
                    const string  theNewLogName          =
                        theNewLogSuffix.empty() ?
                        theNewLogSuffix : mFileName + theNewLogSuffix;
                    const string  theFileName            = mFileName;
                    const int     thePrevFd              = mFd;
                    int           theFd                  = mFd;
                    const int64_t theMinModTimeToKeepSec = mMinModTimeToKeepSec;
                    {
                        QCStMutexUnlocker theUnlocker(mMutex);
                        if (! theNewLogName.empty() &&
                                ::rename(
                                    theFileName.c_str(),
                                    theNewLogName.c_str()) == 0) {
                            ::close(theFd);
                            theFd = -1;
                        }
                    }
                    DeleteOldLogsFiles(theFileName, theMinModTimeToKeepSec,
                        mMutex);
                    mDeleteOldLogsFlag = false;
                    if (theNewLogSuffix == mNewLogSuffix) {
                        mNewLogSuffix.clear();
                    }
                    if (theFd < 0) {
                        if (thePrevFd == mFd) {
                            mFd                = -1;
                            mCurWritten        = 0;
                            mNextOpenRetryTime = Now() - Seconds(10);
                        }
                    } else if (thePrevFd != mFd) {
                        QCStMutexUnlocker theUnlocker(mMutex);
                        ::close(theFd);
                    }
                }
            }
            if (mFd < 0 && ! mFileName.empty() && mNextOpenRetryTime < Now()) {
                OpenLogFile();
            }
            if (mFd >= 0 && mWritePtr < mWriteEndPtr) {
                bool              theRetryWriteFlag   = false;
                const int         theFd               = mFd;
                const char*       thePtr              = mWritePtr;
                const char* const theEndPtr           = mWriteEndPtr;
                bool              theShutdownFlag     = ! mRunFlag;
                const Time        kMaxShutdownTimeSec = 180;
                Time              theShutdownEndTime  = mRunFlag ? 0 :
                    Now() + min(Seconds(kMaxShutdownTimeSec), mMaxLogWaitTime);
                QCStMutexUnlocker theUnlocker(mMutex);
                int theError = 0;
                while (thePtr < theEndPtr) {
                    const ssize_t theNWr = ::write(
                        theFd, thePtr, theEndPtr - thePtr);
                    if (theNWr < 0) {
                        theError = errno;
                        if (theShutdownFlag) {
                            if (theShutdownEndTime <= Now()) {
                                break;
                            }
                        } else if (! mRunFlag) {
                            theShutdownFlag = true;
                            theShutdownEndTime = Now() + min(
                                Seconds(kMaxShutdownTimeSec), mMaxLogWaitTime);
                        }
                        if (theError == EINTR) {
                            continue;
                        }
                        if (theError == EAGAIN) {
                            struct pollfd thePoll = { 0 };
                            thePoll.fd     = theFd;
                            thePoll.events = POLLOUT;
                            const int theRet = poll(&thePoll, 1, 1000);
                            if (theRet > 0) {
                                continue;
                            }
                            theError = errno;
                            if (theError == EAGAIN || theError == EINTR) {
                                continue;
                            }
                        }
                        break;
                    }
                    if (theNWr == 0) {
                        theError = -1;
                        break;
                    }
                    thePtr += theNWr;
                }
                theUnlocker.Lock();
                if (thePtr < theEndPtr) {
                    mWriteErrCount++;
                    if (theFd == mFd && mLogFileNamePrefixes.size() > 1) {
                        mFd = -1; // Close it, and swith to a new one.
                        theRetryWriteFlag = true;
                    } else {
                        mWriteBytesDiscardedCount += theEndPtr - thePtr;
                    }
                }
                mCurWritten += thePtr - mWritePtr;
                if (theError != 0) {
                    mLastError = theError;
                }
                if (theFd != mFd) {
                    QCStMutexUnlocker theUnlocker(mMutex);
                    ::close(theFd);
                }
                if (theRetryWriteFlag) {
                    continue;
                }
            } else if (mWritePtr < mWriteEndPtr) {
                mWriteErrCount++;
                mWriteBytesDiscardedCount += mWriteEndPtr - mWritePtr;
            }
            mWritePtr    = 0;
            mWriteEndPtr = 0;
            mWriteDoneCond.Notify();
        }
        if (mFd >= 0) {
            ::close(mFd);
            mFd = -1;
        }
    }
    ostream& GetStream(
        LogLevel inLogLevel,
        bool     inDiscardFlag)
    {
        QCStMutexLocker theLocker(mMutex);

        MsgStream* theRetPtr = mMsgStreamHeadPtr;
        if (theRetPtr) {
            QCASSERT(mMsgStreamCount > 0);
            mMsgStreamHeadPtr = theRetPtr->Next();
            theRetPtr->Clear(inLogLevel, inDiscardFlag);
            mMsgStreamCount--;
        } else {
            QCASSERT(mMsgStreamCount == 0);
            theRetPtr = new MsgStream(inLogLevel, inDiscardFlag);
        }
        return *theRetPtr;
    }
    void PutStream(
        ostream& inStream)
    {
        QCStMutexLocker theLocker(mMutex);

        MsgStream& theStream = static_cast<MsgStream&>(inStream);
        if (! theStream.IsDiscard()) {
            AppendSelf(theStream.GetLogLevel(),
                theStream.GetMsgPtr(), theStream.GetMsgLength());
        }
        if (mMsgStreamCount < mMaxMsgStreamCount) {
            theStream.tie(0);
            theStream.Next() = mMsgStreamHeadPtr;
            mMsgStreamHeadPtr = &theStream;
            mMsgStreamCount++;
        } else {
            delete &theStream;
        }
    }
    void SetUseNonBlockingIo(
        bool inFlag)
    {
        if (0 <= mFd && ! isatty(mFd)) {
            int theFlags = fcntl(mFd, F_GETFL);
            if (theFlags != -1) {
                if (inFlag) {
                    theFlags |= O_NONBLOCK;
                } else {
                    theFlags &= ~((int)O_NONBLOCK);
                }
                ::fcntl(mFd, F_SETFL, theFlags);
            }
        }
    }
private:
    typedef int64_t        Time;
    typedef uint64_t       Count;
    typedef vector<string> LogFileNames;
    class MsgStream :
        private streambuf,
        public ostream
    {
    public:
        MsgStream(
            LogLevel inLogLevel,
            bool     inDiscardFlag)
            : streambuf(),
              ostream(this),
              mLogLevel(inLogLevel),
              mNextPtr(0)
            { setp(mBuffer, mBuffer + (inDiscardFlag ? 0 : kMaxMsgSize)); }
        virtual ~MsgStream()
            {}
        MsgStream*& Next()
            { return mNextPtr; }
        virtual streamsize xsputn(
            const char* inBufPtr,
            streamsize  inLength)
        {
            char* const theEndPtr = epptr();
            char* const theCurPtr = pptr();
            const streamsize theSize(min(max(streamsize(0), inLength),
                streamsize(theEndPtr - theCurPtr)));
            memcpy(theCurPtr, inBufPtr, theSize);
            pbump(theSize);
            return theSize;
        }
        const char* GetMsgPtr() const
        {
            *pptr() = 0;
            return mBuffer;
        }
        const int GetMsgLength() const
            { return (pptr() - mBuffer); }
        size_t GetLength() const
            { return (epptr() - pptr()); }
        LogLevel GetLogLevel() const
            { return mLogLevel; }
        void Clear(
            LogLevel inLogLevel,
            bool     inDiscardFlag)
        {
            mLogLevel = inLogLevel;
            mNextPtr  = 0;
            ostream::clear();
            ostream::flags(ostream::dec | ostream::skipws);
            ostream::precision(6);
            ostream::width(0);
            ostream::fill(' ');
            ostream::tie(0);
            setp(mBuffer, mBuffer + (inDiscardFlag ? 0 : kMaxMsgSize));
        }
        bool IsDiscard() const
            { return (mBuffer == epptr()); }
    private:
        enum { kMaxMsgSize = 512 << 10 };
        LogLevel   mLogLevel;
        MsgStream* mNextPtr;
        char       mBuffer[kMaxMsgSize + 1];
    private:
        MsgStream(
            const MsgStream&);
        MsgStream& operator=(
            const MsgStream&);
    };

    QCMutex      mMutex;
    QCCondVar    mWriteCond;
    QCCondVar    mWriteDoneCond;
    QCThread     mThread;
    LogFileNames mLogFileNamePrefixes;
    string       mFileName;
    string       mTruncatedSuffix;
    string       mTimeStampFormat;
    string       mNewLogSuffix;
    Count        mDroppedCount;
    Count        mTotalDroppedCount;
    Count        mTruncatedCount;
    Count        mBufWaitersCount;
    Count        mWriteErrCount;
    Count        mWriteBytesDiscardedCount;
    Count        mCurWritten;
    Count        mBufWaitedCount;
    int64_t      mMaxLogFileSize;
    int64_t      mMaxLogFiles;
    Time         mNextOpenRetryTime;
    Time         mOpenRetryInterval;
    Time         mFlushInterval;
    Time         mNextFlushTime;
    Time         mTotalLogWaitedTime;
    Time         mCurLogWatedTime;
    Time         mMaxLogWaitTime;
    int64_t      mNextDeleteOldLogsTimeSec;
    int64_t      mMinModTimeToKeepSec;
    int          mOpenFlags;
    int          mOpenMode;
    int          mBufSize;
    int          mLastError;
    int          mFd;
    const int    mMaxAppendLength;
    bool         mRunFlag;
    bool         mDeleteOldLogsFlag;
    char* const  mBuf0Ptr;
    char* const  mBuf1Ptr;
    char*        mBufPtr;
    char*        mCurPtr;
    char*        mEndPtr;
    const char*  mWritePtr;
    const char*  mWriteEndPtr;
    bool         mCloseFlag;
    bool         mUseGMTFlag;
    int64_t      mTimeSec;
    int64_t      mLogTimeStampSec;
    int64_t      mMsgAppendCount;
    struct tm    mTimeTm;
    struct tm    mLastLogTm;
    bool         mRotateLogsDailyFlag;
    int          mMaxMsgStreamCount;
    int          mMsgStreamCount;
    MsgStream*   mMsgStreamHeadPtr;
    char         mLogTimeStampPrefixStr[256];

    static inline Time Seconds(
        Time inSec)
        { return (inSec * 1000000); }
    static inline Time NanoSec(
        Time inMicroSec)
        { return (inMicroSec * 1000); }
    bool IsFlushing() const
        { return (mWritePtr != 0); }
    bool FlushSelf()
    {
        QCASSERT(mMutex.IsOwned());
        if (mCurPtr <= mBufPtr) {
            return true;
        }
        if (mWritePtr) {
            return false;
        }
        mWritePtr    = mBufPtr;
        mWriteEndPtr = mCurPtr;
        mBufPtr = mBufPtr == mBuf0Ptr ? mBuf1Ptr : mBuf0Ptr;
        mCurPtr = mBufPtr;
        mEndPtr = mBufPtr + mBufSize;
        mWriteCond.Notify();
        return true;
    }
    void FlushIfNeeded(
        int64_t inSec,
        int64_t inMicroSec)
    {
        if (mCurPtr <= mBufPtr) {
            return;
        }
        const Time theNowMicroSecs = Seconds(inSec) + inMicroSec;
        if (mNextFlushTime > theNowMicroSecs) {
            return;
        }
        mNextFlushTime = theNowMicroSecs + mFlushInterval;
        FlushSelf();
    }
    size_t GetMaxRecordSize() const
        { return (mMaxAppendLength + mTruncatedSuffix.size() + 1); }
    static void DeleteOldLogsFiles(
        string   inFileName,
        int64_t  inMinModTimeSec,
        QCMutex& inMutex)
    {
        const size_t thePos = inFileName.rfind('/');
        const string theDirName(thePos != string::npos ?
            inFileName.substr(0, thePos) : ".");
        const string thePrefix(thePos != string::npos ?
            (thePos < inFileName.length() ?
                inFileName.substr(thePos + 1, inFileName.length() - thePos - 1) :
                "") :
            inFileName);
        if (theDirName == "/" && thePrefix.empty()) {
            return;
        }
        int64_t theSec      = 0;
        int64_t theMicroSec = 0;
        Now(theSec, theMicroSec);
        struct dirent** theNamesPtr = 0;
        const int theNameCount =
            ::scandir(theDirName.c_str(), &theNamesPtr, 0, alphasort);
        for (int i = 0; i < theNameCount; i++) {
            const char* const theNamePtr = theNamesPtr[i]->d_name;
            if (::strlen(theNamePtr) > thePrefix.length() &&
                    ::memcmp(thePrefix.data(), theNamePtr,
                        thePrefix.length()) == 0) {
                const string thePath = theDirName + "/" + theNamePtr;
                struct stat  theStat = {0};
                QCStMutexUnlocker theUnlocker(inMutex);
                if (::stat(thePath.c_str(), &theStat) == 0 &&
                        S_ISREG(theStat.st_mode) &&
                        theStat.st_mtime + inMinModTimeSec < theSec) {
                    // std::cout << "deleting: " << thePath.c_str() << "\n";
                    ::unlink(thePath.c_str());
                }
            }
            ::free(theNamesPtr[i]);
        }
        ::free(theNamesPtr);
    }
    static void RotateLogs(
        string   inFileName,
        int      inKeepCount,
        QCMutex& inMutex)
    {
        for (int i = inKeepCount - 1; i >= 0; i--) {
            const string theFrom(i == 0 ? inFileName : MakeName(inFileName, i));
            const string theTo(MakeName(inFileName, i + 1));
            const char* const theFromPtr = theFrom.c_str();
            const char* const theToPtr   = theTo.c_str();
            QCStMutexUnlocker theUnlocker(inMutex);
            ::rename(theFromPtr, theToPtr);
        }
    }
    static string MakeName(
        string   inFileName,
        int      inIndex)
    {
        ostringstream theStream;
        theStream << inFileName << "." << inIndex;
        return theStream.str();
    }
    static void Now(
        int64_t& outSec,
        int64_t& outMicroSec)
    {
        struct timeval theTime = {0};
        if (::gettimeofday(&theTime, 0)) {
            QCUtils::FatalError("gettimeofday", errno);
        }
        outSec      = theTime.tv_sec;
        outMicroSec = theTime.tv_usec;
    }
    static Time Now()
    {
        int64_t theSec      = 0;
        int64_t theMicroSec = 0;
        Now(theSec, theMicroSec);
        return (Seconds(theSec) + theMicroSec);
    }
    void OpenLogFile()
    {
        const LogFileNames theLogFileNames   = mLogFileNamePrefixes;
        const string       theCurFileName    = mFileName;
        const int          theOpenFlags      = mOpenFlags;
        const int          theOpenMode       = mOpenMode;
        const int          theMaxLogFiles    = mMaxLogFiles;
        const int64_t      theMaxLogFileSize = mMaxLogFileSize;
        string             theFileName       = mFileName;
        int                theError          = -1;
        int                theFd             = -1;
        Count              theSize           = 0;
        for (LogFileNames::const_iterator theIt = theLogFileNames.begin(); ;) {
            const char* const theFileNamePtr = theFileName.c_str();
            {
                QCStMutexUnlocker theUnlocker(mMutex);
                while ((theFd = ::open(
                        theFileNamePtr, theOpenFlags, theOpenMode)) < 0) {
                    theError = errno;
                    if (theError != EINTR && theError != EAGAIN) {
                        break;
                    }
                }
            }
            if (theFd >= 0) {
                QCStMutexUnlocker theUnlocker(mMutex);
                struct stat theStat = {0};
                if (::fstat(theFd, &theStat) == 0) {
                    theSize = (Count)max((off_t)0, (off_t)theStat.st_size);
                } else {
                    theSize = 0;
                }
                if (mMaxLogFiles > 0 && theMaxLogFileSize > 0 &&
                        theSize >= (Count)theMaxLogFileSize) {
                    ::close(theFd);
                    theUnlocker.Lock();
                    RotateLogs(theFileName, theMaxLogFiles, mMutex);
                    continue;
                }
                ::fcntl(theFd, F_SETFD, FD_CLOEXEC);
                break;
            }
            while (theIt != theLogFileNames.end() &&
                    theCurFileName == *theIt)  {
                ++theIt;
            }
            if (theIt == theLogFileNames.end()) {
                break;
            }
            theFileName = *theIt++;
        }
        if (mFd >= 0) {
            if (theFd >= 0) {
                QCStMutexUnlocker theUnlocker(mMutex);
                ::close(theFd);
            }
        } else {
            mFd                = theFd;
            mCurWritten        = theSize;
            mNextOpenRetryTime = mFd < 0 ?
                Now() + mOpenRetryInterval : Now() - Seconds(10);
            if (mFd >= 0) {
                mFileName = theFileName;
                int64_t theSec      = 0;
                int64_t theMicroSec = 0;
                Now(theSec, theMicroSec);
                UpdateTimeTm(theSec);
                mLastLogTm = mTimeTm;
            }
        }
    }
    size_t MsgPrefix(
        int64_t  inSec,
        int64_t  inMicroSec,
        LogLevel inLogLevel)
    {
        if (mEndPtr <= mCurPtr) {
            return 0;
        }
        if (mDroppedCount <= 0 && mCurLogWatedTime < Seconds(2)) {
            return MsgPrefixSelf(inSec, inMicroSec, inLogLevel);
        }
        size_t theLen = MsgPrefixSelf(inSec, inMicroSec, kLogLevelINFO);
        if (theLen <= 0) {
            return 0;
        }
        char* thePtr = mCurPtr + theLen;
        if (thePtr + 1 >= mEndPtr) {
            return 0;
        }
        const size_t theMaxSz = mEndPtr - thePtr;
        const int theNWr      = ::snprintf(thePtr, theMaxSz,
            "*** log records dropped: %.0f, %.0f total,"
            " wated: %g sec., %g sec. %.0f times total",
            (double)mDroppedCount,
            (double)mTotalDroppedCount,
            (double)mCurLogWatedTime * 1e-6,
            (double)mTotalLogWaitedTime * 1e-6,
            (double)mBufWaitedCount
        );
        if (theNWr < 0 || theNWr >= (int)theMaxSz) {
            return 0;
        }
        thePtr += theNWr;
        *thePtr++ = '\n';
        {
            QCStValueChanger<char*> theChanger(mCurPtr, thePtr);
            theLen = MsgPrefixSelf(inSec, inMicroSec, inLogLevel);
        }
        return (theLen <= 0 ? 0 : (thePtr - mCurPtr) + theLen);
    }
    size_t MsgPrefixSelf(
        int64_t  inSec,
        int64_t  inMicroSec,
        LogLevel inLogLevel)
    {
        if (mEndPtr <= mCurPtr) {
            return 0;
        }
        const size_t theMaxSz = mEndPtr - mCurPtr;
        const int    theLen   = ::snprintf(mCurPtr, theMaxSz,
            "%s.%03ld %s - ",
            GetLogTimeStampPrefixPtr(inSec),
            (long)(inMicroSec / 1000),
            GetLogLevelNamePtr(inLogLevel)
        );
        return ((theLen < 0 || theLen >= (int)theMaxSz) ? 0 : theLen);
    }
    void RunTimers(
        int64_t inSec,
        int64_t inMicroSec)
    {
        if (mFd < 0 || mMaxLogFiles > 0 || ! mNewLogSuffix.empty() ||
                ! mRotateLogsDailyFlag) {
            return;
        }
        UpdateTimeTm(inSec);
        if (! mDeleteOldLogsFlag && mNextDeleteOldLogsTimeSec < inSec) {
            mDeleteOldLogsFlag = true;
            const int64_t kMinCleanupIntervalSec = 60 * 60 * 12;
            mNextDeleteOldLogsTimeSec = inSec + min(
                kMinCleanupIntervalSec, (60 + mMinModTimeToKeepSec / 3 * 2));
        }
        if (mLastLogTm.tm_mday == mTimeTm.tm_mday &&
                mLastLogTm.tm_mon == mTimeTm.tm_mon &&
                mLastLogTm.tm_year == mTimeTm.tm_year) {
            return;
        }
        char theBuf[64];
        const size_t theLen = ::strftime(
            theBuf, sizeof(theBuf), ".%Y-%m-%d", &mLastLogTm);
        if (theLen <= 0 || theLen >= sizeof(theBuf)) {
            ostringstream theStream;
            theStream <<
                "." << (1900 + mLastLogTm.tm_year) <<
                "-" << setfill('0') << setw(2) << (mLastLogTm.tm_mon + 1) <<
                "-" << setw(2) << mLastLogTm.tm_mday
            ;
            mNewLogSuffix = theStream.str();
        } else {
            mNewLogSuffix.assign(theBuf, theLen);
        }
        mLastLogTm = mTimeTm;
    }
    void UpdateTimeTm(
        int64_t inSec)
    {
        if (inSec != mTimeSec) {
            const time_t theTime = (time_t)inSec;
            if (mUseGMTFlag) {
                ::gmtime_r(&theTime, &mTimeTm);
            } else {
                ::localtime_r(&theTime, &mTimeTm);
            }
        }
    }
    const char* GetLogTimeStampPrefixPtr(
        int64_t inSec)
    {
        if (mLogTimeStampSec == inSec) {
            return mLogTimeStampPrefixStr;
        }
        UpdateTimeTm(inSec);
        const size_t theLen = mTimeStampFormat.empty() ? 0 :
            ::strftime(
                mLogTimeStampPrefixStr, sizeof(mLogTimeStampPrefixStr),
                mTimeStampFormat.c_str(), &mTimeTm
            );
        if (theLen <= 0 || theLen >= sizeof(mLogTimeStampPrefixStr)) {
            const int theLen = ::snprintf(
                mLogTimeStampPrefixStr, sizeof(mLogTimeStampPrefixStr),
                "%ld.", (long)inSec
            );
            if (theLen <= 0 || theLen >= (int)sizeof(mLogTimeStampPrefixStr)) {
                ::strncpy(mLogTimeStampPrefixStr, "X",
                    sizeof(mLogTimeStampPrefixStr));
            }
        }
        mLogTimeStampSec = inSec;
        return mLogTimeStampPrefixStr;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

BufferedLogWriter::BufferedLogWriter(
    int                         inFd,
    const char*                 inFileNamePtr               /* = 0 */,
    int                         inBufSize                   /* = 1 << 20 */,
    const char*                 inTrucatedSuffixPtr         /* = 0 */,
    int64_t                     inOpenRetryIntervalMicroSec /* = 5000000 */,
    int64_t                     inFlushIntervalMicroSec     /* = 1000000 */,
    int64_t                     inMaxLogFileSize            /* = -1 */,
    int                         inMaxLogsFiles              /* = -1 */,
    BufferedLogWriter::LogLevel inLogLevel                  /* = DEBUG */,
    int64_t                     inMaxLogWaitTimeMicroSec    /* = -1 */,
    const char*                 inTimeStampFormatPtr        /* = 0 */,
    bool                        inUseGMTFlag                /* = false */)
    : mLogLevel(inLogLevel),
      mImpl(*(new Impl(
        inFd,
        inFileNamePtr,
        inBufSize,
        inTrucatedSuffixPtr,
        inOpenRetryIntervalMicroSec,
        inFlushIntervalMicroSec,
        inMaxLogFileSize,
        inMaxLogsFiles,
        inMaxLogWaitTimeMicroSec,
        inTimeStampFormatPtr,
        inUseGMTFlag)
      ))
{
    mImpl.Start();
}

BufferedLogWriter::~BufferedLogWriter()
{
    delete &mImpl;
}

void
BufferedLogWriter::SetParameters(
    const Properties& inProps,
    const char*       inPropsPrefixPtr /* = 0 */)
{
    const string thePropsPrefix = inPropsPrefixPtr ? inPropsPrefixPtr : "";
    mImpl.SetParameters(thePropsPrefix, inProps);
    SetLogLevel(inProps.getValue(thePropsPrefix + "logLevel",
        Impl::GetLogLevelNamePtr(mLogLevel)
    ));
}

bool
BufferedLogWriter::Reopen()
{
    return mImpl.Reopen();
}

void
BufferedLogWriter::Close()
{
    mImpl.Close();
}

void
BufferedLogWriter::Stop()
{
    mImpl.Stop();
}

int
BufferedLogWriter::Open(
    const char* inFileNamePtr,
    int         inOpenMode,
    int         inOpenFlags,
    bool        inOpenHereFlag)
{
    return mImpl.Open(inFileNamePtr, inOpenMode, inOpenFlags, inOpenHereFlag);
}

int
BufferedLogWriter::Open(
    const char* inFileNamePtr)
{
    return mImpl.Open(inFileNamePtr,
        kLogWriterDefaulOpenFlags, 0644, false);
}

void
BufferedLogWriter::Flush()
{
    mImpl.Flush();
}

void
BufferedLogWriter::Sync()
{
    mImpl.Sync();
}

void
BufferedLogWriter::SetMaxLogWaitTime(
    int64_t inTimeMicroSec)
{
    mImpl.SetMaxLogWaitTime(inTimeMicroSec);
}

void
BufferedLogWriter::SetFlushInterval(
    int64_t inMicroSecs)
{
    mImpl.SetFlushInterval(inMicroSecs);
}

bool
BufferedLogWriter::AddLogFileNamePrefix(
    const char* inFileNamePtr)
{
    return (inFileNamePtr && mImpl.AddLogFileNamePrefix(inFileNamePtr));
}

void
BufferedLogWriter::ClearLogFileNamePrefixes()
{
    mImpl.ClearLogFileNamePrefixes();
}

void
BufferedLogWriter::Append(
    BufferedLogWriter::LogLevel inLogLevel,
    const char*                 inFmtStrPtr,
    va_list                     inArgs)
{
    if (mLogLevel < inLogLevel) {
        return;
    }
    mImpl.Append(inLogLevel, inFmtStrPtr, inArgs);
}

bool
BufferedLogWriter::SetLogLevel(
    const char* inLogLevelNamePtr)
{
    if (! inLogLevelNamePtr || ! *inLogLevelNamePtr) {
        return false;
    }
    struct { const char* mNamePtr; LogLevel mLevel; } const kLogLevels[] = {
        { "EMERG",  kLogLevelEMERG  },
        { "FATAL",  kLogLevelFATAL  },
        { "ALERT",  kLogLevelALERT  },
        { "CRIT",   kLogLevelCRIT   },
        { "ERROR",  kLogLevelERROR  },
        { "WARN",   kLogLevelWARN   },
        { "NOTICE", kLogLevelNOTICE },
        { "INFO",   kLogLevelINFO   },
        { "DEBUG",  kLogLevelDEBUG  },
        { "NOTSET", kLogLevelNOTSET }
    };
    const size_t kNumLogLevels = sizeof(kLogLevels) / sizeof(kLogLevels[0]);
    for (size_t i = 0; i < kNumLogLevels; i++) {
        if (::strcmp(kLogLevels[i].mNamePtr, inLogLevelNamePtr) == 0) {
            mLogLevel = kLogLevels[i].mLevel;
            return true;
        }
    }
    return false;
}

/* static */ const char*
BufferedLogWriter::GetLogLevelNamePtr(
    BufferedLogWriter::LogLevel inLogLevel)
{
    return Impl::GetLogLevelNamePtr(inLogLevel);
}

void
BufferedLogWriter::GetCounters(
    BufferedLogWriter::Counters& outCounters)
{
    mImpl.GetCounters(outCounters);
}

void
BufferedLogWriter::ChildAtFork()
{
    mImpl.ChildAtFork();
}

void
BufferedLogWriter::PutStream(
    ostream& inStream)
{
    mImpl.PutStream(inStream);
}

ostream&
BufferedLogWriter::GetStream(
    LogLevel inLogLevel)
{
    return mImpl.GetStream(inLogLevel, mLogLevel < inLogLevel);
}

void
BufferedLogWriter::Append(
    LogLevel inLogLevel,
    Writer&  inWriter)
{
    return mImpl.Append(inLogLevel, inWriter);
}

void
BufferedLogWriter::PrepareToFork()
{
    mImpl.PrepareToFork();
}

void
BufferedLogWriter::ForkDone()
{
    mImpl.ForkDone();
}

void
BufferedLogWriter::SetUseNonBlockingIo(
    bool inFlag)
{
    mImpl.SetUseNonBlockingIo(inFlag);
}

}
