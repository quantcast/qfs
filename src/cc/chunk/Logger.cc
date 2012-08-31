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
// 
//----------------------------------------------------------------------------

#include <string>
#include <fstream>

#include "Logger.h"
#include "KfsOps.h"

namespace KFS
{
using std::ifstream;
using std::string;

Logger gLogger;

// Remains of the chunk server transaction log. Presently transaction log is not
// used.
void
Logger::Submit(KfsOp *op)
{
    if (op->op == CMD_CHECKPOINT) {
        delete op;
        return;
    }
    if (op->op == CMD_WRITE) {
        KFS::SubmitOpResponse(op);
    } else {
        assert(op->clnt != NULL);
        op->clnt->HandleEvent(EVENT_CMD_DONE, op);
    }
}

int
Logger::GetVersionFromCkpt()
{
    const string lastCP(mLogDir + "/ckpt_latest");
    ifstream ifs(lastCP.c_str(), ifstream::in);
    if (!ifs) {
        return KFS_LOG_VERSION;
    }

    // Read the header
    // Line 1 is the version
    string versStr;
    int    vers = 0;
    if (! (ifs >> versStr >> vers) || versStr != "version:") {
        return 0;
    }
    return vers;
}

}
