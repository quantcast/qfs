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

typedef RequestHandler<
    KfsOp,
    ValueParserT<DecIntParser>,
    false
> ChunkRequestHandler;

typedef RequestHandler<
    KfsOp,
    ValueParserT<HexIntParser>,
    true
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

template <typename T>
static int
ParseCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
    const T& requestHandlers, char* tmpBuf)
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
    int               reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(tmpBuf, reqLen);
    assert(reqLen == len);
    *res = reqLen == len ? requestHandlers.Handle(buf, reqLen) : 0;
    return (*res ? 0 : -1);
}

// Main thread's buffer
static char sTempParseBuf[MAX_RPC_HEADER_LEN];

int
ParseMetaCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
     bool shortRpcFmtFlag)
{
    return (shortRpcFmtFlag ?
        ParseCommand(ioBuf, len, res, sMetaRequestHandlerShort, sTempParseBuf) :
        ParseCommand(ioBuf, len, res, sMetaRequestHandler,      sTempParseBuf)
    );     
}

int
ParseClientCommand(const IOBuffer& ioBuf, int len, KfsOp** res,
    bool shortRpcFmtFlag, char* tmpBuf)
{
    char* const buf = tmpBuf ? tmpBuf : sTempParseBuf;
    return (shortRpcFmtFlag ?
        ParseCommand(ioBuf, len, res, sClientRequestHandlerShort, buf) :
        ParseCommand(ioBuf, len, res, sClientRequestHandler,      buf)
    );
}

} // namespace KFS

