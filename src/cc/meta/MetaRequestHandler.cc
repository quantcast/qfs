#include "MetaRequest.h"
#include "common/RequestParser.h"
#include "kfsio/IOBuffer.h"

#include <ostream>

namespace KFS
{

using std::ostream;

template<typename T>
    static const T&
MakeMetaRequestHandler(
    const T* inNullPtr      = 0,
    bool     inIoParserFlag = false)
{
    static T sHandler;
    return sHandler
    .MakeIoOrRequestParser("LOOKUP", inIoParserFlag,
        META_LOOKUP,
        static_cast<const MetaLookup*>(0))
    .MakeIoOrRequestParser("LOOKUP_PATH", inIoParserFlag,
        META_LOOKUP_PATH,
        static_cast<const MetaLookupPath*>(0))
    .MakeIoOrRequestParser("CREATE", inIoParserFlag,
        META_CREATE,
        static_cast<const MetaCreate*>(0))
    .MakeIoOrRequestParser("MKDIR", inIoParserFlag,
        META_MKDIR,
        static_cast<const MetaMkdir*>(0))
    .MakeIoOrRequestParser("REMOVE", inIoParserFlag,
        META_REMOVE,
        static_cast<const MetaRemove*>(0))
    .MakeIoOrRequestParser("RMDIR", inIoParserFlag,
        META_RMDIR,
        static_cast<const MetaRmdir*>(0))
    .MakeIoOrRequestParser("READDIR", inIoParserFlag,
        META_READDIR,
        static_cast<const MetaReaddir*>(0))
    .MakeIoOrRequestParser("READDIRPLUS", inIoParserFlag,
        META_READDIRPLUS,
        static_cast<const MetaReaddirPlus*>(0))
    .MakeIoOrRequestParser("GETALLOC", inIoParserFlag,
        META_GETALLOC,
        static_cast<const MetaGetalloc*>(0))
    .MakeIoOrRequestParser("GETLAYOUT", inIoParserFlag,
        META_GETLAYOUT,
        static_cast<const MetaGetlayout*>(0))
    .MakeIoOrRequestParser("ALLOCATE", inIoParserFlag,
        META_ALLOCATE,
        static_cast<const MetaAllocate*>(0))
    .MakeIoOrRequestParser("TRUNCATE", inIoParserFlag,
        META_TRUNCATE,
        static_cast<const MetaTruncate*>(0))
    .MakeIoOrRequestParser("RENAME", inIoParserFlag,
        META_RENAME,
        static_cast<const MetaRename*>(0))
    .MakeIoOrRequestParser("SET_MTIME", inIoParserFlag,
        META_SETMTIME,
        static_cast<const MetaSetMtime*>(0))
    .MakeIoOrRequestParser("CHANGE_FILE_REPLICATION", inIoParserFlag,
        META_CHANGE_FILE_REPLICATION,
        static_cast<const MetaChangeFileReplication*>(0))
    .MakeIoOrRequestParser("COALESCE_BLOCKS", inIoParserFlag,
        META_COALESCE_BLOCKS,
        static_cast<const MetaCoalesceBlocks*>(0))
    .MakeIoOrRequestParser("RETIRE_CHUNKSERVER", inIoParserFlag,
        META_RETIRE_CHUNKSERVER,
        static_cast<const MetaRetireChunkserver*>(0))

    // Meta server <-> Chunk server ops
    .MakeIoOrRequestParser("HELLO", inIoParserFlag,
        META_HELLO,
        static_cast<const MetaHello*>(0))
    .MakeIoOrRequestParser("CORRUPT_CHUNK", inIoParserFlag,
        META_CHUNK_CORRUPT,
        static_cast<const MetaChunkCorrupt*>(0))
    .MakeIoOrRequestParser("EVACUATE_CHUNK", inIoParserFlag,
        META_CHUNK_EVACUATE,
        static_cast<const MetaChunkEvacuate*>(0))
    .MakeIoOrRequestParser("AVAILABLE_CHUNK", inIoParserFlag,
        META_CHUNK_AVAILABLE,
        static_cast<const MetaChunkAvailable*>(0))
    .MakeIoOrRequestParser("CHUNKDIR_INFO", inIoParserFlag,
        META_CHUNKDIR_INFO,
        static_cast<const MetaChunkDirInfo*>(0))

    // Lease related ops
    .MakeIoOrRequestParser("LEASE_ACQUIRE", inIoParserFlag,
        META_LEASE_ACQUIRE,
        static_cast<const MetaLeaseAcquire*>(0))
    .MakeIoOrRequestParser("LEASE_RENEW", inIoParserFlag,
        META_LEASE_RENEW,
        static_cast<const MetaLeaseRenew*>(0))
    .MakeIoOrRequestParser("LEASE_RELINQUISH", inIoParserFlag,
        META_LEASE_RELINQUISH,
        static_cast<const MetaLeaseRelinquish*>(0))

    .MakeIoOrRequestParser("CHECK_LEASES", inIoParserFlag,
        META_CHECK_LEASES,
        static_cast<const MetaCheckLeases*>(0))
    .MakeIoOrRequestParser("PING", inIoParserFlag,
        META_PING,
        static_cast<const MetaPing*>(0))
    .MakeIoOrRequestParser("UPSERVERS", inIoParserFlag,
        META_UPSERVERS,
        static_cast<const MetaUpServers*>(0))
    .MakeIoOrRequestParser("TOGGLE_WORM", inIoParserFlag,
        META_TOGGLE_WORM,
        static_cast<const MetaToggleWORM*>(0))
    .MakeIoOrRequestParser("STATS", inIoParserFlag,
        META_STATS,
        static_cast<const MetaStats*>(0))
    .MakeIoOrRequestParser("RECOMPUTE_DIRSIZE", inIoParserFlag,
        META_RECOMPUTE_DIRSIZE,
        static_cast<const MetaRecomputeDirsize*>(0))
    .MakeIoOrRequestParser("DUMP_CHUNKTOSERVERMAP", inIoParserFlag,
        META_DUMP_CHUNKTOSERVERMAP,
        static_cast<const MetaDumpChunkToServerMap*>(0))
    .MakeIoOrRequestParser("DUMP_CHUNKREPLICATIONCANDIDATES", inIoParserFlag,
        META_DUMP_CHUNKREPLICATIONCANDIDATES,
        static_cast<const MetaDumpChunkReplicationCandidates*>(0))
    .MakeIoOrRequestParser("FSCK", inIoParserFlag,
        META_FSCK,
        static_cast<const MetaFsck*>(0))
    .MakeIoOrRequestParser("OPEN_FILES", inIoParserFlag,
        META_OPEN_FILES,
        static_cast<const MetaOpenFiles*>(0))
    .MakeIoOrRequestParser("GET_CHUNK_SERVERS_COUNTERS", inIoParserFlag,
        META_GET_CHUNK_SERVERS_COUNTERS,
        static_cast<const MetaGetChunkServersCounters*>(0))
    .MakeIoOrRequestParser("GET_CHUNK_SERVER_DIRS_COUNTERS", inIoParserFlag,
        META_GET_CHUNK_SERVER_DIRS_COUNTERS,
        static_cast<const MetaGetChunkServerDirsCounters*>(0))
    .MakeIoOrRequestParser("SET_CHUNK_SERVERS_PROPERTIES", inIoParserFlag,
        META_SET_CHUNK_SERVERS_PROPERTIES,
        static_cast<const MetaSetChunkServersProperties*>(0))
    .MakeIoOrRequestParser("GET_REQUEST_COUNTERS", inIoParserFlag,
        META_GET_REQUEST_COUNTERS,
        static_cast<const MetaGetRequestCounters*>(0))
    .MakeIoOrRequestParser("DISCONNECT", inIoParserFlag,
        META_DISCONNECT,
        static_cast<const MetaDisconnect*>(0))
    .MakeIoOrRequestParser("GETPATHNAME", inIoParserFlag,
        META_GETPATHNAME,
        static_cast<const MetaGetPathName*>(0))
    .MakeIoOrRequestParser("CHOWN", inIoParserFlag,
        META_CHOWN,
        static_cast<const MetaChown*>(0))
    .MakeIoOrRequestParser("CHMOD", inIoParserFlag,
        META_CHMOD,
        static_cast<const MetaChmod*>(0))
    .MakeIoOrRequestParser("AUTHENTICATE", inIoParserFlag,
        META_AUTHENTICATE,
        static_cast<const MetaAuthenticate*>(0))
    .MakeIoOrRequestParser("DELEGATE", inIoParserFlag,
        META_DELEGATE,
        static_cast<const MetaDelegate*>(0))
    .MakeIoOrRequestParser("DELEGATE_CANCEL", inIoParserFlag,
        META_DELEGATE_CANCEL,
        static_cast<const MetaDelegateCancel*>(0))
    .MakeIoOrRequestParser("FORCE_REPLICATION", inIoParserFlag,
        META_FORCE_CHUNK_REPLICATION,
        static_cast<const MetaForceChunkReplication*>(0))
    .MakeIoOrRequestParser("ACK", inIoParserFlag,
        META_ACK,
        static_cast<const MetaAck*>(0))
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

typedef RequestHandler<
    MetaRequest,
    ValueParserT<HexIntParser>,
    true,
    PropertiesTokenizerT<':', '/'>,
    ostream,
    false, // Do not invoke Validate
    '/'
> MetaRequestIoHandler;
static const MetaRequestIoHandler& sMetaRequestIoHandler =
    MakeMetaRequestHandler<MetaRequestIoHandler>(0, true);

bool
MetaRequest::Write(ostream& os, bool omitDefaultsFlag) const
{
    if (sMetaRequestIoHandler.Write(os, this, op, omitDefaultsFlag, ':', '/')) {
        os << '\n';
        return true;
    }
    return false;
}

MetaRequest*
MetaRequest::Read(const char* buf, size_t len)
{
    return sMetaRequestIoHandler.Handle(buf, len);
}

// Main thread's buffer
static char sTempBuf[MAX_RPC_HEADER_LEN];

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
        threadParseBuffer ? threadParseBuffer : sTempBuf, reqLen);
    assert(reqLen == len);
    *res = (reqLen == len) ? (shortRpcFmtFlag ?
        sMetaRequestHandlerShortFmt.Handle(buf, reqLen) :
        sMetaRequestHandler.Handle(buf, reqLen)) :
        0;
    return (*res ? 0 : -1);
}

int
ParseFirstCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer, bool& shortRpcFmtFlag)
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
    // per call processing), besides the requests header are small
    // enough to fit into cpu cache.
    int               reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(
        threadParseBuffer ? threadParseBuffer : sTempBuf, reqLen);
    assert(reqLen == len);
    *res = (reqLen == len) ? (shortRpcFmtFlag ?
        sMetaRequestHandlerShortFmt.Handle(buf, reqLen) :
        sMetaRequestHandler.Handle(buf, reqLen)) :
        0;
    if (*res && 0 <= (*res)->seqno) {
        return 0;
    }
    MetaRequest* const req = shortRpcFmtFlag ?
        sMetaRequestHandler.Handle(buf, reqLen) :
        sMetaRequestHandlerShortFmt.Handle(buf, reqLen);
    if (req) {
        if (0 <= req->seqno) {
            MetaRequest::Release(*res);
            *res = req;
            shortRpcFmtFlag = ! shortRpcFmtFlag;
        } else {
            MetaRequest::Release(req);
        }
    }
    return (*res ? 0 : -1);
}

} // Namespace KFS
