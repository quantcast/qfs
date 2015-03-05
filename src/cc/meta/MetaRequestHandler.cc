#include "MetaRequest.h"
#include "common/RequestParser.h"
#include "kfsio/IOBuffer.h"

#include <ostream>
#include <string>
#include <vector>
#include <map>
#include <set>

namespace KFS
{

using std::ostream;
using std::string;
using std::vector;
using std::map;
using std::set;

template<typename T>
    static T&
MakeMetaRequestHandler(
    const T* inNullPtr = 0)
{
    static T sHandler;
    return sHandler
    .MakeParser("LOOKUP",
        META_LOOKUP,
        static_cast<const MetaLookup*>(0))
    .MakeParser("LOOKUP_PATH",
        META_LOOKUP_PATH,
        static_cast<const MetaLookupPath*>(0))
    .MakeParser("CREATE",
        META_CREATE,
        static_cast<const MetaCreate*>(0))
    .MakeParser("MKDIR",
        META_MKDIR,
        static_cast<const MetaMkdir*>(0))
    .MakeParser("REMOVE",
        META_REMOVE,
        static_cast<const MetaRemove*>(0))
    .MakeParser("RMDIR",
        META_RMDIR,
        static_cast<const MetaRmdir*>(0))
    .MakeParser("READDIR",
        META_READDIR,
        static_cast<const MetaReaddir*>(0))
    .MakeParser("READDIRPLUS",
        META_READDIRPLUS,
        static_cast<const MetaReaddirPlus*>(0))
    .MakeParser("GETALLOC",
        META_GETALLOC,
        static_cast<const MetaGetalloc*>(0))
    .MakeParser("GETLAYOUT",
        META_GETLAYOUT,
        static_cast<const MetaGetlayout*>(0))
    .MakeParser("ALLOCATE",
        META_ALLOCATE,
        static_cast<const MetaAllocate*>(0))
    .MakeParser("TRUNCATE",
        META_TRUNCATE,
        static_cast<const MetaTruncate*>(0))
    .MakeParser("RENAME",
        META_RENAME,
        static_cast<const MetaRename*>(0))
    .MakeParser("SET_MTIME",
        META_SETMTIME,
        static_cast<const MetaSetMtime*>(0))
    .MakeParser("CHANGE_FILE_REPLICATION",
        META_CHANGE_FILE_REPLICATION,
        static_cast<const MetaChangeFileReplication*>(0))
    .MakeParser("COALESCE_BLOCKS",
        META_COALESCE_BLOCKS,
        static_cast<const MetaCoalesceBlocks*>(0))
    .MakeParser("RETIRE_CHUNKSERVER",
        META_RETIRE_CHUNKSERVER,
        static_cast<const MetaRetireChunkserver*>(0))

    // Meta server <-> Chunk server ops
    .MakeParser("HELLO",
        META_HELLO,
        static_cast<const MetaHello*>(0))
    .MakeParser("CORRUPT_CHUNK",
        META_CHUNK_CORRUPT,
        static_cast<const MetaChunkCorrupt*>(0))
    .MakeParser("EVACUATE_CHUNK",
        META_CHUNK_EVACUATE,
        static_cast<const MetaChunkEvacuate*>(0))
    .MakeParser("AVAILABLE_CHUNK",
        META_CHUNK_AVAILABLE,
        static_cast<const MetaChunkAvailable*>(0))
    .MakeParser("CHUNKDIR_INFO",
        META_CHUNKDIR_INFO,
        static_cast<const MetaChunkDirInfo*>(0))

    // Lease related ops
    .MakeParser("LEASE_ACQUIRE",
        META_LEASE_ACQUIRE,
        static_cast<const MetaLeaseAcquire*>(0))
    .MakeParser("LEASE_RENEW",
        META_LEASE_RENEW,
        static_cast<const MetaLeaseRenew*>(0))
    .MakeParser("LEASE_RELINQUISH",
        META_LEASE_RELINQUISH,
        static_cast<const MetaLeaseRelinquish*>(0))

    .MakeParser("CHECK_LEASES",
        META_CHECK_LEASES,
        static_cast<const MetaCheckLeases*>(0))
    .MakeParser("PING",
        META_PING,
        static_cast<const MetaPing*>(0))
    .MakeParser("UPSERVERS",
        META_UPSERVERS,
        static_cast<const MetaUpServers*>(0))
    .MakeParser("TOGGLE_WORM",
        META_TOGGLE_WORM,
        static_cast<const MetaToggleWORM*>(0))
    .MakeParser("STATS",
        META_STATS,
        static_cast<const MetaStats*>(0))
    .MakeParser("RECOMPUTE_DIRSIZE",
        META_RECOMPUTE_DIRSIZE,
        static_cast<const MetaRecomputeDirsize*>(0))
    .MakeParser("DUMP_CHUNKTOSERVERMAP",
        META_DUMP_CHUNKTOSERVERMAP,
        static_cast<const MetaDumpChunkToServerMap*>(0))
    .MakeParser("DUMP_CHUNKREPLICATIONCANDIDATES",
        META_DUMP_CHUNKREPLICATIONCANDIDATES,
        static_cast<const MetaDumpChunkReplicationCandidates*>(0))
    .MakeParser("FSCK",
        META_FSCK,
        static_cast<const MetaFsck*>(0))
    .MakeParser("OPEN_FILES",
        META_OPEN_FILES,
        static_cast<const MetaOpenFiles*>(0))
    .MakeParser("GET_CHUNK_SERVERS_COUNTERS",
        META_GET_CHUNK_SERVERS_COUNTERS,
        static_cast<const MetaGetChunkServersCounters*>(0))
    .MakeParser("GET_CHUNK_SERVER_DIRS_COUNTERS",
        META_GET_CHUNK_SERVER_DIRS_COUNTERS,
        static_cast<const MetaGetChunkServerDirsCounters*>(0))
    .MakeParser("SET_CHUNK_SERVERS_PROPERTIES",
        META_SET_CHUNK_SERVERS_PROPERTIES,
        static_cast<const MetaSetChunkServersProperties*>(0))
    .MakeParser("GET_REQUEST_COUNTERS",
        META_GET_REQUEST_COUNTERS,
        static_cast<const MetaGetRequestCounters*>(0))
    .MakeParser("DISCONNECT",
        META_DISCONNECT,
        static_cast<const MetaDisconnect*>(0))
    .MakeParser("GETPATHNAME",
        META_GETPATHNAME,
        static_cast<const MetaGetPathName*>(0))
    .MakeParser("CHOWN",
        META_CHOWN,
        static_cast<const MetaChown*>(0))
    .MakeParser("CHMOD",
        META_CHMOD,
        static_cast<const MetaChmod*>(0))
    .MakeParser("AUTHENTICATE",
        META_AUTHENTICATE,
        static_cast<const MetaAuthenticate*>(0))
    .MakeParser("DELEGATE",
        META_DELEGATE,
        static_cast<const MetaDelegate*>(0))
    .MakeParser("DELEGATE_CANCEL",
        META_DELEGATE_CANCEL,
        static_cast<const MetaDelegateCancel*>(0))
    .MakeParser("FORCE_REPLICATION",
        META_FORCE_CHUNK_REPLICATION,
        static_cast<const MetaForceChunkReplication*>(0))
    .MakeParser("ACK",
        META_ACK,
        static_cast<const MetaAck*>(0))
    ;
}

class MetaRequestDeleter
{
public:
    static void Delete(
        MetaRequest* inReqPtr)
        {  MetaRequest::Release(inReqPtr); }
};

typedef RequestHandler<
    MetaRequest,
    ValueParserT<DecIntParser>,
    false, // Use long names / format.
    PropertiesTokenizer,
    NopOstream,
    true, // Invoke Validate
    MetaRequestDeleter
> MetaRequestHandler;
static const MetaRequestHandler& sMetaRequestHandler =
    MakeMetaRequestHandler<MetaRequestHandler>();

typedef RequestHandler<
    MetaRequest,
    ValueParserT<HexIntParser>,
    true, // Use short names / format.
    PropertiesTokenizer,
    NopOstream,
    true, // Invoke Validate
    MetaRequestDeleter
> MetaRequestHandlerShortFmt;
static const MetaRequestHandlerShortFmt& sMetaRequestHandlerShortFmt =
    MakeMetaRequestHandler<MetaRequestHandlerShortFmt>();

class StringEscapeIoParser
{
public:
    template<typename T>
    static void SetValue(
        const char* inPtr,
        size_t      inLen,
        const T&    inDefaultValue,
        T&          outValue)
    {
        ValueParserT<HexIntParser>::SetValue(
            inPtr, inLen, inDefaultValue, outValue);
    }
    static void SetValue(
        const char*      inPtr,
        size_t           inLen,
        const LeaseType& inDefaultValue,
        LeaseType&       outValue)
    {
        int theVal = inDefaultValue;
        ValueParserT<HexIntParser>::SetValue(
                inPtr, inLen, (int)inDefaultValue, theVal);
        switch (theVal) {
            case READ_LEASE:
                outValue = READ_LEASE;
            break;
            case WRITE_LEASE:
                outValue = WRITE_LEASE;
            break;
            default:
                outValue = inDefaultValue;
            break;
        }
    }
    static void SetValue(
        const char*   inPtr,
        size_t        inLen,
        const string& inDefaultValue,
        string&       outValue)
    {
        if (! Unescape(outValue, inPtr, inLen)) {
            outValue = inDefaultValue;
        }
    }
    template<size_t DEFAULT_CAPACITY>
    static void SetValue(
        const char*                         inPtr,
        size_t                              inLen,
        const StringBufT<DEFAULT_CAPACITY>& inDefaultValue,
        StringBufT<DEFAULT_CAPACITY>&       outValue)
    {
        if (! Unescape(outValue, inPtr, inLen)) {
            outValue = inDefaultValue;
        }
    }
    template<typename T, typename A>
    static void SetValue(
        const char*         inPtr,
        size_t              inLen,
        const vector<T, A>& inDefaultValue,
        vector<T, A>&       outValue)
        { ReadCollection(inPtr, inLen, inDefaultValue, (const T*)0, outValue); }
    template<typename T, typename C, typename A>
    static void SetValue(
        const char*         inPtr,
        size_t              inLen,
        const set<T, C, A>& inDefaultValue,
        set<T, C, A>&       outValue)
        { ReadCollection(inPtr, inLen, inDefaultValue, (const T*)0, outValue); }
private:
    template<typename T>
    static bool Unescape(
        T&          inStr,
        const char* inPtr,
        size_t      inLen)
    {
        inStr.clear();
        const char*       thePtr    = inPtr;
        const char* const theEndPtr = thePtr + inLen;
        const char*       theEPtr;
        while ((theEPtr = (const char*)memchr(
                thePtr, '%', theEndPtr - thePtr))) {
            inStr.append(thePtr, theEPtr - thePtr);
            if (theEndPtr < theEPtr + 3) {
                return false;
            }
            const int theFirst = sCharToHex[*++theEPtr & 0xFF];
            if (theFirst == 0xFF) {
                return false;
            }
            const int theSecond = sCharToHex[*++theEPtr & 0xFF];
            if (theSecond == 0xFF) {
                return false;
            }
            const char theSym = (char)((theFirst << 4) | theSecond);
            inStr.append(&theSym, 1);
            thePtr = theEPtr + 1;
        }
        inStr.append(thePtr, theEndPtr - thePtr);
        return true;
    }
    template<typename T, typename ET>
    static void ReadCollection(
        const char* inPtr,
        size_t      inLen,
        const T&    inDefaultValue,
        const ET*   inElemTypePtr,
        T&          outValue)
    {
        outValue.clear();
        if (inLen <= 0) {
            return;
        }
        const char* thePtr      = inPtr;
        const char* theEndPtr   = thePtr + inLen;
        const char* theStartPtr = thePtr;
        for (; ;) {
            if (theEndPtr <= thePtr || (*thePtr & 0xFF) == ',' ) {
                ET theVal;
                SetValue(theStartPtr, thePtr - theStartPtr,
                    ET(), theVal);
                outValue.insert(outValue.end(), theVal);
                if (theEndPtr <= thePtr) {
                    break;
                }
                ++thePtr;
                theStartPtr = thePtr;
            } else {
                ++thePtr;
            }
        }
    }
    static const unsigned char* const sCharToHex;
};
const unsigned char* const StringEscapeIoParser::sCharToHex =
    HexIntParser::GetChar2Hex();

class StringInsertEscapeOStream
{
public:
    StringInsertEscapeOStream(
        ostream& inStream)
        : mOStream(inStream),
          mFlags(inStream.flags())
    {
        mOStream.Get().flags(mFlags | ostream::hex);
        mOStream.Get().width(0);
    }
    ~StringInsertEscapeOStream()
        { mOStream.Get().flags(mFlags); }
    template<typename T>
    StringInsertEscapeOStream& WriteKeyVal(
            const char* inKeyPtr,
            size_t      inKeyLen,
            const T&    inVal,
            char        inSeparator,
            char        inDelimiter)
    {
        mOStream.write(inKeyPtr, inKeyLen);
        mOStream.put(inSeparator);
        WriteVal(inVal);
        mOStream.put(inDelimiter);
        return *this;
    }
    StringInsertEscapeOStream& WriteName(
        const char* inPtr,
        size_t      inLen,
        char        inDelimiter)
    {
        mOStream.write(inPtr, inLen);
        mOStream.put(inDelimiter);
        return *this;
    }
private:
    template<typename T>
    void WriteVal(
        const T& inVal)
        { mOStream << inVal; }
    template<typename T, typename A>
    void WriteVal(
        const vector<T, A>& inVal)
        { WriteCollection(inVal.begin(), inVal.end()); }
    template<typename T, typename C, typename A>
    void WriteVal(
        const set<T, C, A>& inVal)
        { WriteCollection(inVal.begin(), inVal.end()); }
    template<typename T>
    void WriteCollection(
        T inBegin,
        T inEnd)
    {
        if (inBegin == inEnd) {
            return;
        }
        for (; ;) {
            WriteVal(*inBegin);
            if (++inBegin == inEnd) {
                break;
            }
            mOStream.write(",", 1);
        }
    }
    // Treat characters as integers to correctly represent *int8_t for the
    // StringEscapeIoParser / ValueParserT<HexIntParser> the above. 
    void WriteVal(
        const char inVal)
        { mOStream << ((int)(inVal) & 0xFF); }
    void WriteVal(
        const signed char inVal)
        { mOStream << (signed int)inVal; }
    void WriteVal(
        const unsigned char inVal)
        { mOStream << (unsigned int)inVal; }
    void WriteVal(
        const string& inStr)
        { Escape(inStr.data(), inStr.size()); }
    template<size_t DEFAULT_CAPACITY>
    void WriteVal(
        StringBufT<DEFAULT_CAPACITY>& inStr)
        { Escape(inStr.data(), inStr.size()); }
    void WriteVal(
        const LeaseType inVal)
        { mOStream << (int)inVal; }
private:
    ReqOstream              mOStream;
    ostream::fmtflags const mFlags;

    void Escape(
        const char* inPtr,
        size_t      inLen)
    {
        const char* const kHexChars = "0123456789ABCDEF";
        const char*       thePtr    = inPtr;
        const char* const theEndPtr = thePtr + inLen;
        const char*       thePPtr   = thePtr;
        while (thePtr < theEndPtr) {
            const int theSym = *thePtr & 0xFF;
            if (theSym <= ' ' || 0xFF <= theSym || strchr("%:;/,", theSym)) {
                if (thePPtr < thePtr) {
                    mOStream.write(thePPtr, thePtr - thePPtr);
                }
                char theBuf[3];
                theBuf[0] = '%';
                theBuf[1] = kHexChars[(theSym >> 4) & 0xF];
                theBuf[2] = kHexChars[theSym & 0xF];
                mOStream.write(theBuf, 3);
                thePPtr = thePtr + 1;
            }
            ++thePtr;
        }
        if (thePPtr < theEndPtr) {
            mOStream.write(thePPtr, theEndPtr - thePPtr);
        }
    }
};

class IoDefinitionMethod
{
public:
    template<typename OBJ, typename PARSER>
    static PARSER& Define(
        PARSER&     inParser,
        const OBJ*  /* inNullPtr */)
        { return OBJ::IoParserDef(inParser); }
};

typedef RequestHandler<
    MetaRequest,
    StringEscapeIoParser,
    true,  // Use short names / format.
    PropertiesTokenizerT<':', ';'>,
    StringInsertEscapeOStream,
    false, // Do not invoke Validate
    MetaRequestDeleter,
    ':',
    IoDefinitionMethod
> MetaRequestIoHandler;
static const MetaRequestIoHandler& sMetaRequestIoHandler =
    MakeMetaRequestHandler<MetaRequestIoHandler>();

bool
MetaRequest::Write(ostream& os, bool omitDefaultsFlag) const
{
    StringInsertEscapeOStream theStream(os);
    return sMetaRequestIoHandler.Write(
            theStream, this, op, omitDefaultsFlag, ':', ';');
}

/* static */ MetaRequest*
MetaRequest::Read(const char* buf, size_t len)
{
    return sMetaRequestIoHandler.Handle(buf, len);
}

template<typename T>
    static const T&
MakeLogMetaRequestHandler(
    const T* inNullPtr = 0)
{
    return MakeMetaRequestHandler(inNullPtr)
    .MakeParser("LEASE_CLENAUP",
        META_LEASE_CLEANUP,
        static_cast<const MetaLeaseCleanup*>(0))
    ;
}

class LogIoDefinitionMethod
{
public:
    template<typename OBJ, typename PARSER>
    static PARSER& Define(
        PARSER&     inParser,
        const OBJ*  /* inNullPtr */)
        { return OBJ::LogIoDef(inParser); }
};

typedef RequestHandler<
    MetaRequest,
    StringEscapeIoParser,
    true,  // Use short names / format.
    PropertiesTokenizerT<':', ';'>,
    StringInsertEscapeOStream,
    false, // Do not invoke Validate
    MetaRequestDeleter,
    ':',
    LogIoDefinitionMethod
> MetaRequestLogIoHandler;
static const MetaRequestLogIoHandler& sMetaReplayIoHandler =
    MakeLogMetaRequestHandler<MetaRequestLogIoHandler>();

/* static */ bool
MetaRequest::Replay(const char* buf, size_t len)
{
    MetaRequest* req = sMetaReplayIoHandler.Handle(buf, len);
    if (! req) {
        return false;
    }
    req->replayFlag = true;
    req->handle();
    req->replayFlag = false;
    MetaRequest::Release(req);
    return true;
}

bool
MetaRequest::WriteLog(ostream& os, bool omitDefaultsFlag) const
{
    StringInsertEscapeOStream theStream(os);
    return sMetaReplayIoHandler.Write(
            theStream, this, op, omitDefaultsFlag, ':', ';');
}

/* static */ int
MetaRequest::GetId(const TokenValue& name)
{
    return sMetaRequestIoHandler.NameToObjId(name);
}

/* static */ TokenValue
MetaRequest::GetName(int id)
{
    return sMetaRequestIoHandler.ObjIdToName(id);
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
