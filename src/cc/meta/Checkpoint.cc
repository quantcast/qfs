/*!
 * $Id$
 *
 * Copyright 2008-2012,2016 Quantcast Corporation. All rights reserved.
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
 *
 * \file checkpoint.cc
 * \brief KFS metadata checkpointing
 * \author Sriram Rao and Blake Lewis
 *
 * The metaserver during its normal operation writes out log records.  Every
 * N minutes, the metaserver rolls over the log file.  Periodically, a sequence
 * of log files are compacted to create a checkpoint: a previous checkpoint is
 * loaded and subsequent log files are replayed to update the tree.  At the end
 * of replay, a checkpoint is saved to disk.  To save a checkpoint, we iterate
 * through the leaf nodes of the tree copying the contents of each node to a
 * checkpoint file.
 */

#include "Checkpoint.h"
#include "kfstree.h"
#include "MetaRequest.h"
#include "NetDispatch.h"
#include "LayoutManager.h"
#include "LogWriter.h"
#include "MetaVrSM.h"
#include "MetaVrLogSeq.h"
#include "util.h"

#include "common/MdStream.h"
#include "common/FdWriter.h"
#include "common/StBuffer.h"
#include "common/IntToString.h"

#include <iostream>
#include <iomanip>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace KFS
{
using std::hex;
using std::dec;

template<typename OST>
int
Checkpoint::write_leaves(OST& os)
{
    LeafIter li(metatree.firstLeaf(), 0);
    Meta *m = li.current();
    int status = 0;
    while (status == 0 && m) {
        status = m->checkpoint(os);
        li.next();
        Node* const p = li.parent();
        m = p ? li.current() : 0;
    }
    return status;
}

string
Checkpoint::cpfile(
    const MetaVrLogSeq& committedseq)
{
    string fname = makename(cpdir, "chkpt", committedseq.mEpochSeq);
    fname += '.';
    AppendDecIntToString(fname, committedseq.mViewSeq);
    fname += '.';
    AppendDecIntToString(fname, committedseq.mLogSeq);
    return fname;
}

int
Checkpoint::write(
    const string&       logname,
    const MetaVrLogSeq& logseq,
    int64_t             errchksum)
{
    if (logname.empty()) {
        return -EINVAL;
    }
    cpname = cpfile(logseq);
    StringBufT<256> tmpStr(cpname.data(), cpname.size());
    tmpStr.Append('.');
    const size_t prefLen = tmpStr.GetSize();
    const char*  tmpname;
    int          fd;
    int          status;
    for (int i = 64; ; i--) {
        tmpname = AppendHexIntToString(
            tmpStr.Truncate(prefLen), gLayoutManager.GetRandom().Rand()
        ).Append(".tmp").GetPtr();
        if (0 <= (fd = open(
                tmpname,
                O_WRONLY | (writesync ? O_SYNC : 0) |
                    O_EXCL | O_CREAT | O_TRUNC,
                0666))) {
            status = 0;
            break;
        }
        const int err = errno;
        if (EEXIST != err || i <= 0) {
            status = err > 0 ? -err : -EIO;
            break;
        }
    }
    if (status == 0) {
        FdWriter fdw(fd);
        const bool kSyncFlag = false;
        MdStreamT<FdWriter> os(&fdw, kSyncFlag, string(), writebuffersize);
        os << dec;
        os << "checkpoint/" << logseq.mLogSeq << "/" << errchksum <<
            "/" << logseq.mEpochSeq << "/" << logseq.mViewSeq << '\n';
        os << "checksum/last-line\n";
        os << "version/" << VERSION << '\n';
        os << "filesysteminfo/fsid/" << metatree.GetFsId() << "/crtime/" <<
            ShowTime(metatree.GetCreateTime()) << '\n';
        os << "fid/" << fileID.getseed() << '\n';
        os << "chunkId/" << chunkID.getseed() << '\n';
        os << "time/" << DisplayIsoDateTime() << '\n';
        os << "shortnames/1\n";
        if (kHexIntFormatFlag) {
            os << "setintbase/16\n" << hex;
        }
        os << "log/" << logname << "\n\n";
        status = gLayoutManager.WriteChunkServers(os);
        if (status == 0 && os) {
            status = write_leaves(os);
        }
        if (status == 0 && os) {
            status = gLayoutManager.WritePendingMakeStable(os);
        }
        if (status == 0 && os) {
            status = gLayoutManager.WritePendingChunkVersionChange(os);
        }
        if (status == 0 && os) {
            status = gNetDispatch.WriteCanceledTokens(os);
        }
        if (status == 0 && os) {
            status = gLayoutManager.GetIdempotentRequestTracker().Write(os);
        }
        if (status == 0 && os) {
            status = gLayoutManager.GetUserAndGroup().WriteGroups(os);
        }
        if (status == 0 && os) {
            status = gLayoutManager.WritePendingObjStoreDelete(os);
        }
        if (status == 0 && os) {
            status = MetaRequest::GetLogWriter().GetMetaVrSM().Checkpoint(os);
        }
        if (status == 0 && os) {
            status = gNetDispatch.CheckpointCryptoKeys(os);
        }
        if (status == 0) {
            os << "worm/" << (getWORMMode() ? 1 : 0) << '\n';
            os << "time/" << DisplayIsoDateTime() << '\n';
            const string md = os.GetMd();
            os << "checksum/" << md << '\n';
            os.SetStream(0);
            if ((status = fdw.GetError()) != 0) {
                if (status > 0) {
                    status = -status;
                }
            } else if (! os) {
                status = -EIO;
            }
        }
        if (status == 0) {
            if (close(fd)) {
                status = errno > 0 ? -errno : -EIO;
            } else {
                if (rename(tmpname, cpname.c_str())) {
                    status = errno > 0 ? -errno : -EIO;
                } else {
                    fd = -1;
                    status = link_latest(cpname, LASTCP);
                }
            }
        } else {
            close(fd);
        }
    }
    if (status != 0 && fd >= 0) {
        unlink(tmpname);
    }
    return status;
}

}
