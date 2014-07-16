//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/06/20
// Author: Sriram Rao
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
// \file Logger.h
// \brief Code for handling logging between checkpoints
//
//----------------------------------------------------------------------------

#ifndef CHUNKSERVER_LOGGER_H
#define CHUNKSERVER_LOGGER_H

#include <string>

namespace KFS
{
using std::string;

struct KfsOp;

// Remains of the chunk server transaction log. Presently transaction log is not
// used, Submit just dispatches the op.
class Logger {
public:
    Logger()
        : mLogDir()
        {}
    ~Logger()
        {}

    void Init(const string& logDir) {
        mLogDir = logDir;
    }
    void Start() {}

    /// Submit a request for logging.  This is called by the main
    /// thread and the request is sent down to the logger thread.
    /// @param[in] op  The op that needs to be logged
    void Submit(KfsOp *op);

    int GetVersionFromCkpt();

    int GetLoggerVersionNum() const {
        return KFS_LOG_VERSION;
    }

private:
    /// Version # to be written out in the ckpt file
    static const int KFS_LOG_VERSION = 2;
    static const int KFS_LOG_VERSION_V1 = 1;

    /// The path to the directory for writing out logs
    string mLogDir;
private:
    Logger(const Logger&);
    Logger& operator=(const Logger&);
};

extern Logger gLogger;

}

#endif // CHUNKSERVER_LOGGER_H
