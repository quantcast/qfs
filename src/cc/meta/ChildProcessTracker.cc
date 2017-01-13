
//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2009/04/30
// Author: Sriram Rao, Mike Ovsiannikov
//
// Copyright 2009-2012,2016 Quantcast Corporation. All rights reserved.
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
// \file ChildProcessTracker.cc
// \brief Handler for tracking child process that are forked off, retrieve
// their exit status.
//
//----------------------------------------------------------------------------

#include "MetaRequest.h"
#include "ChildProcessTracker.h"
#include "kfsio/Globals.h"
#include "common/MsgLogger.h"

#include <vector>
#include <map>

#include <sys/wait.h>
#include <errno.h>
#include <signal.h>

namespace KFS
{
using std::vector;
using std::pair;
using std::back_inserter;
using std::make_pair;
using libkfsio::globalNetManager;

ChildProcessTrackingTimer::~ChildProcessTrackingTimer()
{
    if (! mPending.empty()) {
        globalNetManager().RegisterTimeoutHandler(this);
    }
}

void
ChildProcessTrackingTimer::Track(pid_t pid, MetaRequest *r)
{
    if (mPending.empty()) {
        globalNetManager().RegisterTimeoutHandler(this);
    }
    mPending.insert(make_pair(pid, r));
}

void
ChildProcessTrackingTimer::Timeout()
{
    while (! mPending.empty()) {
        int         status = 0;
        const pid_t pid    = waitpid(-1, &status, WNOHANG);
        if (pid <= 0) {
            return;
        }
        pair<Pending::iterator, Pending::iterator> const range =
            mPending.equal_range(pid);
        if (range.first == range.second) {
            // Assume that all children are reaped here.
            KFS_LOG_STREAM_ERROR <<
                "untracked child exited:"
                " pid: "     << pid <<
                " status: "  << status <<
            KFS_LOG_EOM;
            continue;
        }
        mTmpRequests.clear();
        copy(range.first, range.second, back_inserter(mTmpRequests));
        mPending.erase(range.first, range.second);
        const bool lastReqFlag = mPending.empty();
        if (lastReqFlag) {
            globalNetManager().UnRegisterTimeoutHandler(this);
        }
        for (Requests::const_iterator it = mTmpRequests.begin();
                it != mTmpRequests.end();
                ++it) {
            MetaRequest* const req = it->second;
            req->status = WIFEXITED(status) ? WEXITSTATUS(status) :
                (WIFSIGNALED(status) ? -WTERMSIG(status) : -11111);
            req->suspended = false;
            KFS_LOG_STREAM_INFO <<
                "child exited:"
                " pid: "     << pid <<
                " status: "  << req->status <<
                " request: " << req->Show() <<
            KFS_LOG_EOM;
            submit_request(req);
        }
        mTmpRequests.clear();
        if (lastReqFlag) {
            return;
        }
    }
}

void
ChildProcessTrackingTimer::KillAll(int signal)
{
    for (Pending::const_iterator it = mPending.begin();
            it != mPending.end();
            ++it) {
        kill(it->first, signal);
    }
}

void
ChildProcessTrackingTimer::CancelAll()
{
    mTmpRequests.clear();
    copy(mPending.begin(), mPending.end(), back_inserter(mTmpRequests));
    for (Requests::const_iterator it = mTmpRequests.begin();
            it != mTmpRequests.end();
            ++it) {
        MetaRequest* const req = it->second;
        req->status = -ECANCELED;
        submit_request(req);
    }
    mTmpRequests.clear();
}

}
