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

#include "common/kfstypes.h"
#include "common/MdStream.h"

#include <string>
#include <istream>
#include <fstream>

namespace KFS
{
using std::string;
using std::ifstream;
using std::istream;

class DETokenizer;
class DiskEntry;

class Replay
{
public:
    Replay();
    ~Replay();
    bool verifyLogSegmentsPresent()
    {
        lastLogNum = -1;
        return getLastLogNum() == 0;
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
    int64_t getErrChksum() const
        { return errChecksum; }
    seq_t getCommitted() const
        { return committed; }
    void setErrChksum(int64_t sum)
        { errChecksum = sum; }
    void setCommitted(seq_t seq)
    {
        checkpointCommitted = seq;
        committed           = seq;
    }
    const string& getCurLog() const
        { return path; }
    string getLastLogName() const
    {
        if (0 <= lastLogNum) {
            return const_cast<Replay*>(this)->logfile(lastLogNum);
        }
        return string();
    }
    int getLastCommittedStatus() const
        { return lastCommittedStatus; }
    MdStateCtx getMdState() const
        { return mds.GetMdState(); }
    seq_t getLogNum() const
        { return number; }
    seq_t getLastLogStart() const
        { return lastLogStart; }
    seq_t getLastBlockSeq() const
        { return lastBlockSeq; }
    int playLine(const char* line, int len, seq_t blockSeq);
    bool logSegmentHasLogSeq() const
        { return logSegmentHasLogSeq(number); }
    void verifyAllLogSegmentsPreset(bool flag)
        { verifyAllLogSegmentsPresetFlag = flag; }
    void setLogDir(const char* dir);
    seq_t getCheckpointCommitted() const
        { return checkpointCommitted; }

    class BlockChecksum
    {
    public:
        BlockChecksum();
        uint32_t blockEnd(size_t skip);
        bool write(const char* buf, size_t len);
        bool flush() { return true; }
    private:
        size_t   skip;
        uint32_t checksum;
    };
    class ReplayState;
    class Tokenizer
    {
    public:
        Tokenizer(istream& file, Replay* replay);
        ~Tokenizer();
        DETokenizer& Get() { return tokenizer; }
        ReplayState& GetState() { return state; }
    private:
        ReplayState& state;
        DETokenizer& tokenizer;
    };
    static void AddRestotreEntries(DiskEntry& e);
private:
    typedef MdStreamT<BlockChecksum> MdStream;

    ifstream         file;   //!< the log file being replayed
    string           path;   //!< path name for log file
    seq_t            number; //!< sequence number for log file
    seq_t            lastLogNum;
    int              lastLogIntBase;
    bool             appendToLastLogFlag;
    bool             verifyAllLogSegmentsPresetFlag;
    seq_t            checkpointCommitted;
    seq_t            committed;
    seq_t            lastLogStart;
    seq_t            lastBlockSeq;
    int64_t          errChecksum;
    int64_t          rollSeeds;
    int              lastCommittedStatus;
    size_t           tmplogprefixlen;
    string           tmplogname;
    string           logdir;
    MdStream         mds;
    Tokenizer        replayTokenizer;
    const DiskEntry& entrymap;
    BlockChecksum    blockChecksum;
    seq_t            maxLogNum;
    seq_t            logSeqStartNum;

    int playLogs(seq_t lastlog, bool includeLastLogFlag);
    int playlog(bool& lastEntryChecksumFlag);
    int getLastLogNum();
    const string& logfile(seq_t num);
    bool logSegmentHasLogSeq(seq_t num) const
        { return  (0 <= logSeqStartNum && logSeqStartNum <= num); }
    string getLastLog();
private:
    // No copy.
    Replay(const Replay&);
    Replay& operator=(const Replay&);
};

bool restore_chunk_server_end(DETokenizer& c);
extern Replay replayer;

}
#endif // !defined(KFS_REPLAY_H)
