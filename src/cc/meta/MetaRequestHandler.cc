#include "MetaRequest.h"
#include "common/RequestParser.h"
#include "kfsio/IOBuffer.h"

namespace KFS
{

typedef RequestHandler<MetaRequest> MetaRequestHandler;
static const MetaRequestHandler& MakeMetaRequestHandler()
{
    static MetaRequestHandler              sHandler;
    return sHandler
    .MakeParser<MetaLookup               >("LOOKUP")
    .MakeParser<MetaLookupPath           >("LOOKUP_PATH")
    .MakeParser<MetaCreate               >("CREATE")
    .MakeParser<MetaMkdir                >("MKDIR")
    .MakeParser<MetaRemove               >("REMOVE")
    .MakeParser<MetaRmdir                >("RMDIR")
    .MakeParser<MetaReaddir              >("READDIR")
    .MakeParser<MetaReaddirPlus          >("READDIRPLUS")
    .MakeParser<MetaGetalloc             >("GETALLOC")
    .MakeParser<MetaGetlayout            >("GETLAYOUT")
    .MakeParser<MetaAllocate             >("ALLOCATE")
    .MakeParser<MetaTruncate             >("TRUNCATE")
    .MakeParser<MetaRename               >("RENAME")
    .MakeParser<MetaSetMtime             >("SET_MTIME")
    .MakeParser<MetaChangeFileReplication>("CHANGE_FILE_REPLICATION")
    .MakeParser<MetaCoalesceBlocks       >("COALESCE_BLOCKS")
    .MakeParser<MetaRetireChunkserver    >("RETIRE_CHUNKSERVER")

    // Meta server <-> Chunk server ops
    .MakeParser<MetaHello                >("HELLO")
    .MakeParser<MetaChunkCorrupt         >("CORRUPT_CHUNK")
    .MakeParser<MetaChunkEvacuate        >("EVACUATE_CHUNK")
    .MakeParser<MetaChunkAvailable       >("AVAILABLE_CHUNK")
    .MakeParser<MetaChunkDirInfo         >("CHUNKDIR_INFO")

    // Lease related ops
    .MakeParser<MetaLeaseAcquire         >("LEASE_ACQUIRE")
    .MakeParser<MetaLeaseRenew           >("LEASE_RENEW")
    .MakeParser<MetaLeaseRelinquish      >("LEASE_RELINQUISH")

    .MakeParser<MetaCheckLeases          >("CHECK_LEASES")
    .MakeParser<MetaPing                 >("PING")
    .MakeParser<MetaUpServers            >("UPSERVERS")
    .MakeParser<MetaToggleWORM           >("TOGGLE_WORM")
    .MakeParser<MetaStats                >("STATS")
    .MakeParser<MetaRecomputeDirsize     >("RECOMPUTE_DIRSIZE")
    .MakeParser<MetaDumpChunkToServerMap >("DUMP_CHUNKTOSERVERMAP")
    .MakeParser<
      MetaDumpChunkReplicationCandidates >("DUMP_CHUNKREPLICATIONCANDIDATES")
    .MakeParser<MetaFsck                 >("FSCK")
    .MakeParser<MetaOpenFiles            >("OPEN_FILES")
    .MakeParser<
      MetaGetChunkServersCounters        >("GET_CHUNK_SERVERS_COUNTERS")
    .MakeParser<
      MetaGetChunkServerDirsCounters     >("GET_CHUNK_SERVER_DIRS_COUNTERS")
    .MakeParser<
      MetaSetChunkServersProperties      >("SET_CHUNK_SERVERS_PROPERTIES")
    .MakeParser<MetaGetRequestCounters   >("GET_REQUEST_COUNTERS")
    .MakeParser<MetaDisconnect           >("DISCONNECT")
    .MakeParser<MetaGetPathName          >("GETPATHNAME")
    .MakeParser<MetaChown                >("CHOWN")
    .MakeParser<MetaChmod                >("CHMOD")
    .MakeParser<MetaAuthenticate         >("AUTHENTICATE")
    .MakeParser<MetaDelegate             >("DELEGATE")
    .MakeParser<MetaDelegateCancel       >("DELEGATE_CANCEL")
    .MakeParser<MetaForceChunkReplication>("FORCE_REPLICATION")
    ;
}
static const MetaRequestHandler& sMetaRequestHandler = MakeMetaRequestHandler();

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
    char* threadParseBuffer /* = 0 */)
{
    // Main thread's buffer
    static char tempBuf[MAX_RPC_HEADER_LEN];

    *res = 0;
    if (len <= 0 || len > MAX_RPC_HEADER_LEN) {
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
    *res = reqLen == len ? sMetaRequestHandler.Handle(buf, reqLen) : 0;
    return (*res ? 0 : -1);
}

} // Namespace KFS
