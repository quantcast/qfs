/*!
 * $Id$
 *
 * \file Logger.h
 * \brief metadata logger
 * \author Blake Lewis (Kosmix Corp.)
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
 */
#if !defined(KFS_LOGGER_H)
#define KFS_LOGGER_H

#include <fstream>
#include <sstream>
#include <string>

#include "kfstypes.h"
#include "MetaRequest.h"
#include "util.h"
#include "common/MdStream.h"

#include "kfsio/ITimeout.h"

namespace KFS
{
using std::string;
using std::ostringstream;
using std::ofstream;

/*!
 * \brief Class for logging metadata updates
 *
 *  - RPCs when they are done are logged (if necessary, such as, they mutate the
 *  tree) and are then dispatched to the sender.
 *  - a timer that periodically causes log rollover.  Whenever
 *  the log rollover occurs, after we close the log file, we create a link from
 *  "LAST" to the recently closed log file.  This is used by the log compactor
 *  to determine the set of files that can be compacted.
 */

class Logger
{
public:
    static const int VERSION = 1;
    Logger(string d)
        : logdir(d),
          lognum(-1),
          logname(),
          logf(),
          md(),
          logstream(md),
          nextseq(0),
          committed(0),
          incp(0)
        {}
    ~Logger()
    {
        logstream.flush();
        logf.close();
    }
    void setLogDir(const string &d)
    {
        logdir = d;
    }
    string logfile(int n)   //!< generate a log file name
    {
        return makename(logdir, "log", n);
    }
    /*!
     * \brief check whether request is stored on disk
     * \param[in] r the request of interest
     * \return  whether it is on disk
     */
    bool iscommitted(MetaRequest *r)
    {
        return r->seqno != 0 && r->seqno <= committed;
    }
    //!< log a request
    int log(MetaRequest *r);
    //!< add to the log and dispatch downstream to netdispatcher
    void dispatch(MetaRequest *r);
    seq_t checkpointed() { return incp; } //!< highest seqno in CP
    void setLog(int seqno); //!< set the log filename based on seqno
    //!< create or open log file
    int startLog(int seqno,
        bool appendFlag = false, int logAppendIntBase = -1);
    int finishLog(); //!< rollover the log file
    const string name() const { return logname; } //!< name of log file
    /*!
     * \brief set initial sequence numbers at startup
     * \param[in] last last sequence number from checkpoint or log
     */
    void set_seqno(seq_t last)
    {
        incp = committed = nextseq = last;
    }
    MdStream& getMdStream() { return md; }
private:
    string   logdir;      //!< directory where logs are kept
    int      lognum;      //!< for generating log file names
    string   logname;     //!< name of current log file
    ofstream logf;        //!< the current log file
    MdStream md;
    ostream& logstream;
    seq_t    nextseq;     //!< next request sequence no.
    seq_t    committed;   //!< highest request known to be on disk
    seq_t    incp;        //!< highest request in a checkpoint
    string genfile(int n) //!< generate a log file name
    {
        ostringstream f(ostringstream::out);
        f << n;
        return logdir + "/log." + f.str();
    }
    bool fail() const { return (logf.fail() || md.fail()); }
    void flushLog();
    void flushResult(MetaRequest *r);
private:
    // No copy.
    Logger(const Logger&);
    Logger& operator=(Logger&);
};

extern string LOGDIR;
extern string LASTLOG;
extern Logger oplog;
extern void logger_setup_paths(const string& logdir);
extern void logger_init(int rotateIntervalSec);
extern void logger_set_rotate_interval(int rotateIntervalSec);

}
#endif // !defined(KFS_LOGGER_H)
