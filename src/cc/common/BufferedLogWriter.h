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
// 
//----------------------------------------------------------------------------

#ifndef BUFFEREDLOGWRITER_H
#define BUFFEREDLOGWRITER_H

#include <inttypes.h>
#include <stdarg.h>
#include <iostream>

namespace KFS
{
using std::ostream;
class Properties;

// Double buffered message log writer.
// Message log writer can be configured to write into files with max file size,
// number of files, and roll over time interval, or it can be configured to
// write into file descriptor (stderr or socket for example).
// The max wait / block time on the message write can be configured as well.
// if the wait exceeds max time (0 -- never wait) then the corresponding
// message(s) are discarded, and the log record with the number of discarded
// log messages will appear in the next successful buffer write / flush. This is
// need to prevent blocking on message log write with "bad" disks in the cases
// where the disk becomes unavailable or just cannot keep up. Chunk and meta
// servers message log writes are configured with 0 write wait time by default.
class BufferedLogWriter
{
public:
    enum LogLevel
    {
        kLogLevelEMERG  = 0, 
        kLogLevelFATAL  = 0,
        kLogLevelALERT  = 100,
        kLogLevelCRIT   = 200,
        kLogLevelERROR  = 300, 
        kLogLevelWARN   = 400,
        kLogLevelNOTICE = 500,
        kLogLevelINFO   = 600,
        kLogLevelDEBUG  = 700,
        kLogLevelNOTSET = 800
    };
    struct Counters
    {
        int64_t mAppendCount;
        int64_t mDroppedCount;
        int64_t mWriteErrorCount;
        int64_t mAppendWaitCount;
        int64_t mAppendWaitMicroSecs;
    };
    BufferedLogWriter(
        int         inFd                        = -1,
        const char* inFileNamePtr               = 0,
        int         inBufSize                   = 1 << 20,
        const char* inTrucatedSuffixPtr         = 0,
        int64_t     inOpenRetryIntervalMicroSec = 5000000,
        int64_t     inFlushIntervalMicroSec     = 1000000,
        int64_t     inMaxLogFileSize            = -1,
        int         inMaxLogsFiles              = -1,
        LogLevel    inLogLevel                  = kLogLevelDEBUG,
        int64_t     inMaxLogWaitTimeMicroSec    = -1,
        const char* inTimeStampFormatPtr        = 0,      // see strftime
        bool        inUseGMTFlag                = false); // GMT vs local
    ~BufferedLogWriter();
    void SetParameters(
        const Properties& inProps,
        const char*       inPropsPrefixPtr = 0);
    bool Reopen();
    void Close();
    void Stop();
    int Open(
        const char* inFileNamePtr,
        int         inOpenMode,
        int         inOpenFlags,
        bool        inOpenHereFlag = false);
    int Open(
        const char* inFileNamePtr);
    bool AddLogFileNamePrefix(
        const char* inFileNamePtr);
    void ClearLogFileNamePrefixes();
    void Flush(); // Schedules write of the buffered data.
    void Sync();  // Waits for write completion of all buffered data.
    void SetMaxLogWaitTime(
        int64_t inMaxLogWaitTimeMicroSec);
    void SetFlushInterval(
        int64_t inMicroSecs);
    void SetLogLevel(
        LogLevel inLogLevel)
        { mLogLevel = inLogLevel; }
    bool SetLogLevel(
        const char* inLogLevelNamePtr);
    LogLevel GetLogLevel() const
        { return mLogLevel; }
    static const char* GetLogLevelNamePtr(
        LogLevel inLogLevel);
    bool IsLogLevelEnabled(
        LogLevel inLogLevel) const
        { return (mLogLevel >= inLogLevel); }
    void Append(
        LogLevel    inLogLevel,
        const char* inFmtStrPtr,
        ...)
    {
        if (mLogLevel < inLogLevel) {
            return;
        }
        va_list theArgs;
        va_start(theArgs, inFmtStrPtr);
        Append(inLogLevel, inFmtStrPtr, theArgs);
        va_end(theArgs);
    }
    void Append(
        LogLevel    inLogLevel,
        const char* inFmtStrPtr,
        va_list     inArgs);
    void GetCounters(
        Counters& outCounters);
    ostream& GetStream(LogLevel inLogLevel);
    void PutStream(ostream& inStreamPtr);
    void ChildAtFork();

    class StStream
    {
    public:
        StStream(
            BufferedLogWriter& inLogWriter,
            LogLevel           inLogLevel)
            : mLogWriter(inLogWriter),
              mStream(inLogWriter.GetStream(inLogLevel))
            {}
        ~StStream()
            { mLogWriter.PutStream(mStream); }
        operator ostream& ()
            { return mStream; }
        ostream& GetStream()
            { return mStream; }
    private:
        BufferedLogWriter& mLogWriter;
        ostream&           mStream;
    };

    class Writer
    {
    public:
        Writer()
            {}
        virtual ~Writer()
            {}
        virtual int GetMsgLength() = 0;
        virtual int Write(
            char* inBufPtr,
            int   inBufSize) = 0;
    protected:
        Writer(
            const Writer&)
            {}
        Writer& operator=(
            const Writer&)
            { return *this; }
    };
    void Append(
        LogLevel inLogLevel,
        Writer&  inWriter);
    void PrepareToFork();
    void ForkDone();
    void SetUseNonBlockingIo(
        bool inFlag);
private:
    class Impl;
    volatile LogLevel mLogLevel;
    Impl&             mImpl;

private:
    BufferedLogWriter(
        const BufferedLogWriter& inWriter);
    BufferedLogWriter& operator=(
        const BufferedLogWriter& inWriter);
};
}

#endif /* BUFFEREDLOGWRITER_H */
