//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2005/03/01
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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
//----------------------------------------------------------------------------

#include "MsgLogger.h"
#include "Version.h"
#include <stdio.h>
#include <limits>

namespace KFS
{

MsgLogger* MsgLogger::logger = 0;
const MsgLogger::LogLevel kMsgLoggerDefaultLogLevel =
#ifdef NDEBUG
    MsgLogger::kLogLevelINFO
#else
    MsgLogger::kLogLevelDEBUG
#endif
;

MsgLogger::MsgLogger(
    const char*         filename,
    MsgLogger::LogLevel logLevel,
    const Properties*   props,
    const char*         propPrefix,
    const char*         logLevelStr)
    : BufferedLogWriter(filename ? -1 : fileno(stderr), filename)
{
    BufferedLogWriter::SetLogLevel(logLevel);
    BufferedLogWriter::SetLogLevel(logLevelStr);
    BufferedLogWriter::SetMaxLogWaitTime(std::numeric_limits<int>::max());
    if (props) {
        BufferedLogWriter::SetParameters(*props, propPrefix);
    }
    BufferedLogWriter::Append(kLogLevelDEBUG, "version: %s %s",
        KFS_BUILD_VERSION_STRING.c_str(), KFS_SOURCE_REVISION_STRING.c_str());
}

MsgLogger::~MsgLogger()
{
    if (this == logger) {
        logger = 0;
    }
}

void
MsgLogger::Init(
    const char* filename)
{
    Init(filename, kMsgLoggerDefaultLogLevel, 0, 0);
}

void
MsgLogger::Init(
    const char*         filename,
    MsgLogger::LogLevel logLevel)
{
    Init(filename, logLevel, 0, 0);
}

void
MsgLogger::Init(
    const Properties& props,
    const char*       propPrefix)
{
    Init(0, kMsgLoggerDefaultLogLevel, &props, propPrefix);
}

void
MsgLogger::Init(
    const char*         filename,
    MsgLogger::LogLevel logLevel,
    const Properties*   props,
    const char*         propPrefix,
    const char*         logLevelStr)
{
    if (logger) {
        if (props) {
            logger->SetParameters(*props, propPrefix);
        }
    } else {
        static MsgLogger sLogger(
            filename, logLevel, props, propPrefix, logLevelStr);
        logger = &sLogger;
    }
}

void
MsgLogger::Stop()
{
    if (logger) {
        logger->BufferedLogWriter::Stop();
    }
}

} // namespace KFS
