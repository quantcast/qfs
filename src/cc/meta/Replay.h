/*!
 * $Id$
 *
 * \file Replay.h
 * \brief log replay definitions
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
#if !defined(KFS_REPLAY_H)
#define KFS_REPLAY_H

#include <string>
#include <fstream>

namespace KFS
{
using std::string;
using std::ifstream;

class Replay
{
public:
    Replay()
        : file(),
          path(),
          number(-1),
          lastLogNum(-1),
          lastLogIntBase(-1),
          appendToLastLogFlag(false),
          rollSeeds(0)
        {}
    ~Replay()
        {}
    bool verifyLogSegmentsPresent()
    {
        lastLogNum = -1;
        return getLastLog(lastLogNum) == 0;
    }
    //!< open the log file for replay
    int openlog(const string &p);
    int logno() { return number; }
    //!< read and apply its contents
    int playLogs(bool includeLastLogFlag = false);
    //!< starting from log for logno(),
    //!< replay all logs we have in the logdir.
    int playAllLogs() { return playLogs(true); }
    bool getAppendToLastLogFlag() const { return appendToLastLogFlag; }
    int getLastLogIntBase() const { return lastLogIntBase; }
    inline void setRollSeeds(int64_t roll);
    int64_t getRollSeeds() const { return rollSeeds; }
private:
    ifstream file;   //!< the log file being replayed
    string   path;   //!< path name for log file
    int      number; //!< sequence number for log file
    int      lastLogNum;
    int      lastLogIntBase;
    bool     appendToLastLogFlag;
    int64_t  rollSeeds;

    int playLogs(int lastlog, bool includeLastLogFlag);
    int playlog(bool& lastEntryChecksumFlag);
    int getLastLog(int& lastlog);
private:
    // No copy.
    Replay(const Replay&);
    Replay& operator=(const Replay&);
};

extern Replay replayer;

}
#endif // !defined(KFS_REPLAY_H)
