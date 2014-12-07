#include "MetaRequest.h"
#include "common/RequestParser.h"
#include "kfsio/IOBuffer.h"

namespace KFS
{

template<typename T>
static const T& MakeMetaRequestHandler(const T* inNullPtr = 0)
{
    static T sHandler;
    return sHandler
    .MakeParser("LOOKUP",
        static_cast<const MetaLookup*>(0))
    .MakeParser("LOOKUP_PATH",
        static_cast<const MetaLookupPath*>(0))
    .MakeParser("CREATE",
        static_cast<const MetaCreate*>(0))
    .MakeParser("MKDIR",
        static_cast<const MetaMkdir*>(0))
    .MakeParser("REMOVE",
        static_cast<const MetaRemove*>(0))
    .MakeParser("RMDIR",
        static_cast<const MetaRmdir*>(0))
    .MakeParser("READDIR",
        static_cast<const MetaReaddir*>(0))
    .MakeParser("READDIRPLUS",
        static_cast<const MetaReaddirPlus*>(0))
    .MakeParser("GETALLOC",
        static_cast<const MetaGetalloc*>(0))
    .MakeParser("GETLAYOUT",
        static_cast<const MetaGetlayout*>(0))
    .MakeParser("ALLOCATE",
        static_cast<const MetaAllocate*>(0))
    .MakeParser("TRUNCATE",
        static_cast<const MetaTruncate*>(0))
    .MakeParser("RENAME",
        static_cast<const MetaRename*>(0))
    .MakeParser("SET_MTIME",
        static_cast<const MetaSetMtime*>(0))
    .MakeParser("CHANGE_FILE_REPLICATION",
        static_cast<const MetaChangeFileReplication*>(0))
    .MakeParser("COALESCE_BLOCKS",
        static_cast<const MetaCoalesceBlocks*>(0))
    .MakeParser("RETIRE_CHUNKSERVER",
        static_cast<const MetaRetireChunkserver*>(0))

    // Meta server <-> Chunk server ops
    .MakeParser("HELLO",
        static_cast<const MetaHello*>(0))
    .MakeParser("CORRUPT_CHUNK",
        static_cast<const MetaChunkCorrupt*>(0))
    .MakeParser("EVACUATE_CHUNK",
        static_cast<const MetaChunkEvacuate*>(0))
    .MakeParser("AVAILABLE_CHUNK",
        static_cast<const MetaChunkAvailable*>(0))
    .MakeParser("CHUNKDIR_INFO",
        static_cast<const MetaChunkDirInfo*>(0))

    // Lease related ops
    .MakeParser("LEASE_ACQUIRE",
        static_cast<const MetaLeaseAcquire*>(0))
    .MakeParser("LEASE_RENEW",
        static_cast<const MetaLeaseRenew*>(0))
    .MakeParser("LEASE_RELINQUISH",
        static_cast<const MetaLeaseRelinquish*>(0))

    .MakeParser("CHECK_LEASES",
        static_cast<const MetaCheckLeases*>(0))
    .MakeParser("PING",
        static_cast<const MetaPing*>(0))
    .MakeParser("UPSERVERS",
        static_cast<const MetaUpServers*>(0))
    .MakeParser("TOGGLE_WORM",
        static_cast<const MetaToggleWORM*>(0))
    .MakeParser("STATS",
        static_cast<const MetaStats*>(0))
    .MakeParser("RECOMPUTE_DIRSIZE",
        static_cast<const MetaRecomputeDirsize*>(0))
    .MakeParser("DUMP_CHUNKTOSERVERMAP",
        static_cast<const MetaDumpChunkToServerMap*>(0))
    .MakeParser("DUMP_CHUNKREPLICATIONCANDIDATES",
        static_cast<const MetaDumpChunkReplicationCandidates*>(0))
    .MakeParser("FSCK",
        static_cast<const MetaFsck*>(0))
    .MakeParser("OPEN_FILES",
        static_cast<const MetaOpenFiles*>(0))
    .MakeParser("GET_CHUNK_SERVERS_COUNTERS",
        static_cast<const MetaGetChunkServersCounters*>(0))
    .MakeParser("GET_CHUNK_SERVER_DIRS_COUNTERS",
        static_cast<const MetaGetChunkServerDirsCounters*>(0))
    .MakeParser("SET_CHUNK_SERVERS_PROPERTIES",
        static_cast<const MetaSetChunkServersProperties*>(0))
    .MakeParser("GET_REQUEST_COUNTERS",
        static_cast<const MetaGetRequestCounters*>(0))
    .MakeParser("DISCONNECT",
        static_cast<const MetaDisconnect*>(0))
    .MakeParser("GETPATHNAME",
        static_cast<const MetaGetPathName*>(0))
    .MakeParser("CHOWN",
        static_cast<const MetaChown*>(0))
    .MakeParser("CHMOD",
        static_cast<const MetaChmod*>(0))
    .MakeParser("AUTHENTICATE",
        static_cast<const MetaAuthenticate*>(0))
    .MakeParser("DELEGATE",
        static_cast<const MetaDelegate*>(0))
    .MakeParser("DELEGATE_CANCEL",
        static_cast<const MetaDelegateCancel*>(0))
    .MakeParser("FORCE_REPLICATION",
        static_cast<const MetaForceChunkReplication*>(0))
    ;
}
typedef RequestHandler<
    MetaRequest,
    ValueParserT<DecIntParser>,
    false
> MetaRequestHandler;
static const MetaRequestHandler& sMetaRequestHandler =
    MakeMetaRequestHandler<MetaRequestHandler>();

typedef RequestHandler<
    MetaRequest,
    ValueParserT<HexIntParser>,
    true
> MetaRequestHandlerShortFmt;
static const MetaRequestHandlerShortFmt& sMetaRequestHandlerShortFmt =
    MakeMetaRequestHandler<MetaRequestHandlerShortFmt>();

/*!
 * \brief parse a command sent by a client
 *
 * Commands are of the form:
 * <COMMAND NAME> \r\n
 * {header: value \r\n}+\r\n
 *
 * @param[in] ioBuf: buffer containing the request sent by the client
 * @param[in] len: length of cmdBuf
 * @param[out] res: A piece of memory allocated by calling new that
 * contains the data for the request.  It is the caller's
 * responsibility to delete the memory returned in res.
 * @retval 0 on success;  -1 if there is an error
 */
int
ParseCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer, bool shortRpcFmtFlag)
{
    // Main thread's buffer
    static char tempBuf[MAX_RPC_HEADER_LEN];

    *res = 0;
    if (len <= 0 || MAX_RPC_HEADER_LEN < len) {
        return -1;
    }
    // Copy if request header spans two or more buffers.
    // Requests on average are over a magnitude shorter than single
    // io buffer (4K page), thus the copy should be infrequent, and
    // small enough. With modern cpu the copy should be take less
    // cpu cycles than buffer boundary handling logic (or one symbol
    // per call processing), besides the requests header are small
    // enough to fit into cpu cache.
    int               reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(
        threadParseBuffer ? threadParseBuffer : tempBuf, reqLen);
    assert(reqLen == len);
    *res = (reqLen == len) ? (shortRpcFmtFlag ?
        sMetaRequestHandlerShortFmt.Handle(buf, reqLen) :
        sMetaRequestHandler.Handle(buf, reqLen)) :
        0;
    return (*res ? 0 : -1);
}

} // Namespace KFS
