/*!
 * $Id$
 *
 * Copyright 2015 Quantcast Corp.
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
 * \brief metadata transaction log writer.
 * \author Mike Ovsiannikov.
 */

#ifndef KFS_META_LOG_WRITER_H
#define KFS_META_LOG_WRITER_H

#include "common/kfstypes.h"

namespace KFS
{

struct MetaRequest;
class Properties;
class NetManager;
class MdStateCtx;

class LogWriter
{
public:
    enum { VERSION = 1 };

    LogWriter();
    ~LogWriter();
    int Start(
        NetManager&       inNetManager,
        seq_t             inLogSeq,
        const MdStateCtx* inLogAppendMdStatePtr,
        bool              inLogAppendHexFlag,
        const char*       inParametersPrefixPtr,
        const Properties& inParameters);
    void Enqueue(
        MetaRequest& inRequest);
    void ScheduleFlush();
    void Shutdown();
private:
    class Impl;
    Impl& mImpl;
};

} // namespace KFS

#endif /* KFS_META_LOG_WRITER_H */
