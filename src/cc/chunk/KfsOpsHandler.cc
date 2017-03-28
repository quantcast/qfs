//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/12/17
// Author: Mike Ovsiannikov
//
// Copyright 2014-2017 Quantcast Corporation. All rights reserved.
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

#include "KfsOps.h"
#include "common/RequestParser.h"

namespace KFS
{

template<typename T>
static T&
MakeCommonRequestHandler(
    T& handler)
{
    return handler
    .MakeParser("SIZE", static_cast<const SizeOp*>(0))
    ;
}

template<typename T>
static const T&
MakeClientRequestHandler(const T* typeArg = 0)
{
    static T sHandler;
    return MakeCommonRequestHandler(sHandler)
    .MakeParser("CLOSE",
        static_cast<const CloseOp*>(0))
    .MakeParser("READ",
        static_cast<const ReadOp*>(0))
    .MakeParser("WRITE_ID_ALLOC",
        static_cast<const WriteIdAllocOp*>(0))
    .MakeParser("WRITE_PREPARE",
        static_cast<const WritePrepareOp*>(0))
    .MakeParser("WRITE_SYNC",
        static_cast<const WriteSyncOp*>(0))
    .MakeParser("RECORD_APPEND",
        static_cast<const RecordAppendOp*>(0))
    .MakeParser("GET_RECORD_APPEND_OP_STATUS",
        static_cast<const GetRecordAppendOpStatus*>(0))
    .MakeParser("CHUNK_SPACE_RESERVE",
        static_cast<const ChunkSpaceReserveOp*>(0))
    .MakeParser("CHUNK_SPACE_RELEASE",
        static_cast<const ChunkSpaceReleaseOp*>(0))
    .MakeParser("GET_CHUNK_METADATA",
        static_cast<const GetChunkMetadataOp*>(0))
    .MakeParser("PING",
        static_cast<const PingOp*>(0))
    .MakeParser("DUMP_CHUNKMAP",
        static_cast<const DumpChunkMapOp*>(0))
    .MakeParser("STATS",
        static_cast<const StatsOp*>(0))
    ;
}

template<typename T>
static const T&
MakeMetaRequestHandler(const T* typeArg = 0)
{
    static T sHandler;
    return MakeCommonRequestHandler(sHandler)
    .MakeParser("ALLOCATE",
        static_cast<const AllocChunkOp*>(0))
    .MakeParser("DELETE",
        static_cast<const DeleteChunkOp*>(0))
    .MakeParser("TRUNCATE",
        static_cast<const TruncateChunkOp*>(0))
    .MakeParser("REPLICATE",
        static_cast<const ReplicateChunkOp*>(0))
    .MakeParser("HEARTBEAT",
        static_cast<const HeartbeatOp*>(0))
    .MakeParser("STALE_CHUNKS",
        static_cast<const StaleChunksOp*>(0))
    .MakeParser("CHUNK_VERS_CHANGE",
        static_cast<const ChangeChunkVersOp*>(0))
    .MakeParser("BEGIN_MAKE_CHUNK_STABLE",
        static_cast<const BeginMakeChunkStableOp*>(0))
    .MakeParser("MAKE_CHUNK_STABLE",
        static_cast<const MakeChunkStableOp*>(0))
    .MakeParser("RETIRE",
        static_cast<const RetireOp*>(0))
    .MakeParser("CMD_SET_PROPERTIES",
        static_cast<const SetProperties*>(0))
    .MakeParser("RESTART_CHUNK_SERVER",
        static_cast<const RestartChunkServerOp*>(0))
    ;
}

template <typename SUPER, typename OBJ>
class LongNamesClientRequestParser : public RequestParser<
    SUPER,
    OBJ,
    ValueParserT<DecIntParser>,
    false // Use long names / format.
> {};
typedef RequestHandler<
    KfsOp,
    LongNamesClientRequestParser
> ChunkRequestHandler;

template <typename SUPER, typename OBJ>
class ShortNamesClientRequestParser : public RequestParser<
    SUPER,
    OBJ,
    ValueParserT<HexIntParser>,
    true, // Use short names / format.
    PropertiesTokenizer,
    NopOstream,
    true,  // Invoke Validate
    RequestDeleter,
    RequestParserShortNamesDictionary
> {};
typedef RequestHandler<
    KfsOp,
    ShortNamesClientRequestParser
> ChunkRequestHandlerShort;

static const ChunkRequestHandler& sClientRequestHandler =
    MakeClientRequestHandler<ChunkRequestHandler>();
static const ChunkRequestHandler& sMetaRequestHandler   =
    MakeMetaRequestHandler<ChunkRequestHandler>();

static const ChunkRequestHandlerShort& sClientRequestHandlerShort =
    MakeClientRequestHandler<ChunkRequestHandlerShort>();
static const ChunkRequestHandlerShort& sMetaRequestHandlerShort   =
    MakeMetaRequestHandler<ChunkRequestHandlerShort>();

///
/// Given a command in a buffer, parse it out and build a "Command"
/// structure which can then be executed.  For parsing, we take the
/// string representation of a command and build a Properties object
/// out of it; we can then pull the various headers in whatever order
/// we choose.
/// Commands are of the form:
/// <COMMAND NAME> \r\n
/// {header: value \r\n}+\r\n
///
/// @param[in] ioBuf: buffer containing the request sent by the client
/// @param[in] len:   length of command header
/// @param[out] res:  result (RPC) the caller is repsonseible for deleting
////the RPC.
/// @retval 0 on success; -1 if there is an error
///

template <typename TS, typename T>
static int
ParseCommand(const TS& shortRequestHandlers, const T& requestHandlers,
    char* tmpBuf, RpcFormat& ioRpcFormat,
    const IOBuffer& ioBuf, int len, KfsOp** res)
{
    *res = 0;
    if (len <= 0 || MAX_RPC_HEADER_LEN < len) {
        return -1;
    }
    // Copy if request header spans two or more buffers.
    // Requests on average are over a magnitude shorter than single
    // io buffer (4K page), thus the copy should be infrequent, and
    // small enough. With modern cpu the copy should be take less
    // cpu cycles than buffer boundary handling logic (or one symbol
    // per call processing), besides the request headers are small
    // enough to fit into cpu cache.
    IOBuffer::BufPos  reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(tmpBuf, reqLen);
    assert(reqLen == len);
    if (reqLen != len) {
        return -1;
    }
    if (ioRpcFormat != kRpcFormatLong) {
        *res = shortRequestHandlers.Handle(buf, reqLen);
        if (*res) {
            if (kRpcFormatUndef != ioRpcFormat || 0 <= (*res)->seq) {
                if (kRpcFormatUndef == ioRpcFormat) {
                    ioRpcFormat = kRpcFormatShort;
                }
                return 0;
            }
            delete *res;
            *res = 0;
        }
        if (kRpcFormatUndef != ioRpcFormat) {
            return -1;
        }
    }
    *res = requestHandlers.Handle(buf, reqLen);
    if (! *res) {
        return -1;
    }
    if (kRpcFormatUndef == ioRpcFormat) {
        ioRpcFormat = kRpcFormatLong;
    }
    return 0;
}

// Main thread's buffer
static char sTempParseBuf[MAX_RPC_HEADER_LEN];

int
ParseMetaCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
     RpcFormat& ioRpcFormat)
{
    return ParseCommand(sMetaRequestHandlerShort, sMetaRequestHandler,
        sTempParseBuf, ioRpcFormat, ioBuf, len, res);
}

int
ParseClientCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
    RpcFormat& ioRpcFormat, char* tmpBuf)
{
    return ParseCommand(sClientRequestHandlerShort, sClientRequestHandler,
        tmpBuf ? tmpBuf : sTempParseBuf, ioRpcFormat, ioBuf, len, res);
}


} // namespace KFS

