/*!
 * $Id$
 *
 * Copyright 2008-2012 Quantcast Corp.
 * Copyright 2006-2008 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * \file logger.cc
 * \brief metadata transaction logger.
 * \author Sriram Rao (Quantcast Corp) and Blake Lewis (Kosmix Corp.)
 *         Mike Ovsiannikov -- implement transaction log checksum.
 */

#include "Logger.h"
#include "Checkpoint.h"
#include "util.h"
#include "Replay.h"
#include "common/MsgLogger.h"
#include "kfsio/Globals.h"
#include "NetDispatch.h"

#include <iomanip>

namespace KFS
{
using std::hex;
using std::dec;
using std::ofstream;
using std::ifstream;
using libkfsio::globalNetManager;

// default values
string LOGDIR("./kfslog");
string LASTLOG(LOGDIR + "/last");

Logger oplog(LOGDIR);

void
Logger::dispatch(MetaRequest *r)
{
    r->seqno = ++nextseq;
    if (r->mutation && r->status == 0) {
        if (log(r) < 0) {
            panic("Logger::dispatch", true);
        }
        cp.note_mutation();
    }
    gNetDispatch.Dispatch(r);
}

/*!
 * \brief log the request and flush the result to the fs buffer.
*/
int
Logger::log(MetaRequest *r)
{
    const int res = r->log(logstream);
    if (res >= 0) {
        flushResult(r);
    }
    return res;
}

/*!
 * \brief flush log entries to disk
 *
 * Make sure that all of the log entries are on disk and
 * update the highest sequence number logged.
 */
void
Logger::flushLog()
{
    seq_t last = nextseq;

    logstream.flush();
    if (fail()) {
        panic("Logger::flushLog", true);
    }
    committed = last;
}

/*!
 * \brief set the log filename/log # to seqno
 * \param[in] seqno the next log sequence number (lognum)
 */
void
Logger::setLog(int seqno)
{
    assert(seqno >= 0);
    lognum = seqno;
    logname = logfile(lognum);
}

/*!
 * \brief open a new log file for writing
 * \param[in] seqno the next log sequence number (lognum)
 * \return      0 if successful, negative on I/O error
 */
int
Logger::startLog(int seqno, bool appendFlag /* = false */,
    int logAppendIntBase /* = -1 */)
{
    assert(seqno >= 0);
    lognum = seqno;
    logname = logfile(lognum);
    if (appendFlag) {
        // following log replay, until the next CP, we
        // should continue to append to the logfile that we replayed.
        // seqno will be set to the value we got from the chkpt file.
        // So, don't overwrite the log file.
        KFS_LOG_STREAM_INFO <<
            "log append:" <<
            " int base: " << logAppendIntBase <<
            " file: "     << logname <<
        KFS_LOG_EOM;
        logf.open(logname.c_str(), ofstream::app | ofstream::binary);
        md.SetStream(&logf);
        md.SetWriteTrough(false);
        switch (logAppendIntBase) {
            case 10: logstream << dec; break;
            case 16: logstream << hex; break;
            default:
                panic("invalid int base parameter", false);
                logf.close();
                return -EINVAL;
        }
        return (fail() ? -EIO : 0);
    }
    logf.open(logname.c_str(),
        ofstream::out | ofstream::binary | ofstream::trunc);
    md.SetWriteTrough(false);
    md.Reset(&logf);
    logstream <<
        "version/" << VERSION << "\n"
        "checksum/last-line\n"
        "setintbase/16\n";
    ;
    logstream << "time/" << DisplayIsoDateTime() << '\n';
    logstream << hex;
    logstream.flush();
    return (fail() ? -EIO : 0);
}

/*!
 * \brief close current log file and begin a new one
 */
int
Logger::finishLog()
{
    // if there has been no update to the log since the last roll, don't
    // roll the file over; otherwise, we'll have a file every N mins
    if (incp == committed) {
        return 0;
    }
    logstream << "time/" << DisplayIsoDateTime() << '\n';
    logstream.flush();
    const string checksum = md.GetMd();
    logf << "checksum/" << checksum << '\n';
    logf.close();
    if (fail()) {
        panic("Logger::finishLog, close", true);
    }
    if (link_latest(logname, LASTLOG)) {
        panic("Logger::finishLog, link", true);
    }
    incp = committed;
    const int status = startLog(lognum + 1);
    if (status < 0) {
        panic("Logger::finishLog, startLog", true);
    }
    cp.resetMutationCount();
    return status;
}

/*!
 * \brief make sure result is on disk
 * \param[in] r the result of interest
 *
 * If this result has a higher sequence number than what is
 * currently known to be on disk, flush the log to disk.
 */
void
Logger::flushResult(MetaRequest *r)
{
    if (r->seqno > committed) {
        flushLog();
        assert(r->seqno <= committed);
    }
}

void
logger_setup_paths(const string& logdir)
{
    if (! logdir.empty()) {
        LOGDIR = logdir;
        LASTLOG = LOGDIR + "/last";
        oplog.setLogDir(LOGDIR);
    }
}

class LogRotater : private ITimeout
{
public:
    static LogRotater& Instance()
    {
        static LogRotater sInstance;
        return sInstance;
    }
    ~LogRotater()
    {
        if (mStartedFlag) {
            globalNetManager().UnRegisterTimeoutHandler(this);
        }
        sInstancePtr = 0;
    }
    void Start()
    {
        if (mStartedFlag) {
            return;
        }
        mStartedFlag = true;
        globalNetManager().RegisterTimeoutHandler(this);
    }
    void SetInterval(int rotateIntervalSec)
        { SetTimeoutInterval(rotateIntervalSec * 1000); };
    virtual void Timeout()
        { oplog.finishLog(); }
private:
    LogRotater()
        : ITimeout(),
          mStartedFlag(false)
        { sInstancePtr = this; }

    bool mStartedFlag;
    static LogRotater* sInstancePtr;
private:
    LogRotater(const LogRotater&);
    LogRotater& operator=(const LogRotater&);
};
LogRotater* LogRotater::sInstancePtr = 0;

void
logger_set_rotate_interval(int rotateIntervalSec)
{
    LogRotater::Instance().SetInterval(rotateIntervalSec);
}

void
logger_init(int rotateIntervalSec)
{
    const int  num        = replayer.logno();
    const bool appendFlag = replayer.getAppendToLastLogFlag();
    if (num > 0 && ! file_exists(LASTLOG)) {
        const string logfn = oplog.logfile(num - 1);
        if (file_exists(logfn) && link_latest(logfn, LASTLOG)) {
            panic("KFS::logger_init, link " +
                logfn + " " + LASTLOG, true);
        }
    }
    if (oplog.startLog(num, appendFlag,
            replayer.getLastLogIntBase()) != 0) {
        panic("KFS::logger_init, startLog", true);
    }
    logger_set_rotate_interval(rotateIntervalSec);
    LogRotater::Instance().Start();
}

} // namespace KFS.
