//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/03/03
// Author: Mike Ovsiannikov
//
// Copyright 2014-2015 Quantcast Corp.
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
// Meta server requests serialization methods.
//
//
//----------------------------------------------------------------------------

#include "MetaRequest.h"
#include "LogWriter.h"

#include "common/RequestParser.h"
#include "common/CIdChecksum.h"
#include "kfsio/NetManager.h"
#include "kfsio/Globals.h"
#include "kfsio/IOBuffer.h"

#include <ostream>
#include <string>
#include <vector>
#include <set>
#include <sstream>

namespace KFS
{

using std::ostream;
using std::string;
using std::vector;
using std::map;
using std::set;
using std::ostringstream;

using libkfsio::globalNetManager;

template<typename T>
    T&
AddMetaRequestLog(
    T&   inHandler,
    bool inShortNamesFlag)
{
    return inHandler
    .MakeParser(
        inShortNamesFlag ? "C" : "CREATE",
        META_CREATE,
        static_cast<const MetaCreate*>(0))
    .MakeParser(
        inShortNamesFlag ? "MD" : "MKDIR",
        META_MKDIR,
        static_cast<const MetaMkdir*>(0))
    .MakeParser(
        inShortNamesFlag ? "RM" : "REMOVE",
        META_REMOVE,
        static_cast<const MetaRemove*>(0))
    .MakeParser(
        inShortNamesFlag ? "RD" : "RMDIR",
        META_RMDIR,
        static_cast<const MetaRmdir*>(0))
    .MakeParser(
        inShortNamesFlag ? "TR" : "TRUNCATE",
        META_TRUNCATE,
        static_cast<const MetaTruncate*>(0))
    .MakeParser(
        inShortNamesFlag ? "RN" : "RENAME",
        META_RENAME,
        static_cast<const MetaRename*>(0))
    .MakeParser(
        inShortNamesFlag ? "SM" : "SET_MTIME",
        META_SETMTIME,
        static_cast<const MetaSetMtime*>(0))
    .MakeParser(
        inShortNamesFlag ? "CR" : "CHANGE_FILE_REPLICATION",
        META_CHANGE_FILE_REPLICATION,
        static_cast<const MetaChangeFileReplication*>(0))
    .MakeParser(
        inShortNamesFlag ? "CB" : "COALESCE_BLOCKS",
        META_COALESCE_BLOCKS,
        static_cast<const MetaCoalesceBlocks*>(0))
    .MakeParser(
        inShortNamesFlag ? "CO" : "CHOWN",
        META_CHOWN,
        static_cast<const MetaChown*>(0))
    .MakeParser(
        inShortNamesFlag ? "CM" : "CHMOD",
        META_CHMOD,
        static_cast<const MetaChmod*>(0))
    .MakeParser(
        inShortNamesFlag ? "DC" : "DELEGATE_CANCEL",
        META_DELEGATE_CANCEL,
        static_cast<const MetaDelegateCancel*>(0))
    .MakeParser(
        inShortNamesFlag ? "A" : "ACK",
        META_ACK,
        static_cast<const MetaAck*>(0))
    ;
}

template<typename T>
    static const T&
MakeMetaRequestHandler(
    const T* inNullPtr = 0)
{
    const bool kShortNamesFlag = false;
    static T sHandler;
    return AddMetaRequestLog(sHandler, kShortNamesFlag)
    .MakeParser("LOOKUP",
        META_LOOKUP,
        static_cast<const MetaLookup*>(0))
    .MakeParser("LOOKUP_PATH",
        META_LOOKUP_PATH,
        static_cast<const MetaLookupPath*>(0))
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
    .MakeParser("AUTHENTICATE",
        META_AUTHENTICATE,
        static_cast<const MetaAuthenticate*>(0))
    .MakeParser("DELEGATE",
        META_DELEGATE,
        static_cast<const MetaDelegate*>(0))
    .MakeParser("FORCE_REPLICATION",
        META_FORCE_CHUNK_REPLICATION,
        static_cast<const MetaForceChunkReplication*>(0))
    .MakeParser("READ_META_DATA",
        META_READ_META_DATA,
        static_cast<const MetaReadMetaData*>(0))
    ;
}

template<typename T>
    static const T&
MakeMetaRequestLogRecvHandler(
    const T* inNullPtr = 0)
{
    static T sHandler;
    return sHandler
    .MakeParser("AUTHENTICATE",
        META_AUTHENTICATE,
        static_cast<const MetaAuthenticate*>(0))
    ;
}

class MetaRequestDeleter
{
public:
    static void Delete(
        MetaRequest* inReqPtr)
        {  MetaRequest::Release(inReqPtr); }
};

template<typename INT_PARSER_T>
class MetaReqValueParserT
{
public:
    template<typename T>
    static void SetValue(
        const char* inPtr,
        size_t      inLen,
        const T&    inDefaultValue,
        T&          outValue)
    {
        ValueParserT<INT_PARSER_T>::SetValue(
            inPtr, inLen, inDefaultValue, outValue);
    }
    static void SetValue(
        const char*        inPtr,
        size_t             inLen,
        const CIdChecksum& inDefaultValue,
        CIdChecksum&       outValue)
    {
        const char* thePtr = inPtr;
        if (! outValue.Parse<INT_PARSER_T>(thePtr, inLen)) {
            outValue = inDefaultValue;
        }
    }
};

template <typename SUPER, typename OBJ>
class MetaLongNamesClientRequestParser : public RequestParser<
    SUPER,
    OBJ,
    MetaReqValueParserT<DecIntParser>,
    false, // Use long names / format.
    PropertiesTokenizer,
    NopOstream,
    true,  // Invoke Validate
    MetaRequestDeleter,
    RequestParserLongNamesDictionary
> {};
typedef RequestHandler<
    MetaRequest,
    MetaLongNamesClientRequestParser
> MetaRequestHandler;
static const MetaRequestHandler& sMetaRequestHandler =
    MakeMetaRequestHandler<MetaRequestHandler>();

template <typename SUPER, typename OBJ>
class MetaShortNamesClientRequestParser : public RequestParser<
    SUPER,
    OBJ,
    MetaReqValueParserT<HexIntParser>,
    true, // Use short names / format.
    PropertiesTokenizer,
    NopOstream,
    true,  // Invoke Validate
    MetaRequestDeleter,
    RequestParserShortNamesDictionary
> {};
typedef RequestHandler<
    MetaRequest,
    MetaShortNamesClientRequestParser
> MetaRequestHandlerShortFmt;
static const MetaRequestHandlerShortFmt& sMetaRequestHandlerShortFmt =
    MakeMetaRequestHandler<MetaRequestHandlerShortFmt>();

typedef RequestHandler<
    MetaRequest,
    MetaShortNamesClientRequestParser
> MetaRequestLogRecvHandler;
static const MetaRequestLogRecvHandler& sMetaRequestLogRecvHandler =
    MakeMetaRequestLogRecvHandler<MetaRequestLogRecvHandler>();

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
        MetaReqValueParserT<HexIntParser>::SetValue(
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
    static void SetValue(
        const char*           inPtr,
        size_t                inLen,
        const ServerLocation& inDefaultValue,
        ServerLocation&       outValue)
    {
        const char* thePtr = inPtr + inLen - 1;
        while (inPtr < thePtr && (*thePtr != 'p')) {
            --thePtr;
        }
        if (thePtr <= inPtr || inPtr + inLen <= thePtr + 1) {
            outValue = inDefaultValue;
            return;
        }
        ++thePtr;
        if (! Unescape(outValue.hostname, inPtr, thePtr - inPtr - 1) ||
                ! HexIntParser::Parse(
                    thePtr, inPtr + inLen - thePtr, outValue.port)) {
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
        ostream&    inStream,
        const char* inPrefixPtr = 0,
        size_t      inPrefixLen = 0)
        : mOStream(inStream),
          mPrefixPtr(inPrefixPtr),
          mPrefixLen(inPrefixLen),
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
        if (mPrefixPtr) {
            // Write transaction log prefix.
            mOStream.write(mPrefixPtr, mPrefixLen);
            mPrefixPtr = 0;
        }
        mOStream.write(inPtr, inLen);
        mOStream.put(inDelimiter);
        return *this;
    }
    ReqOstream& GetOStream()
        { return mOStream; }
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
        const StringBufT<DEFAULT_CAPACITY>& inStr)
        { Escape(inStr.data(), inStr.size()); }
    void WriteVal(
        const LeaseType inVal)
        { mOStream << (int)inVal; }
    void WriteVal(
        const ServerLocation& inVal)
    {
        WriteVal(inVal.hostname);
        mOStream.write("p", 1);
        WriteVal(inVal.port);
    }
    ReqOstream              mOStream;
    const char*             mPrefixPtr;
    size_t                  mPrefixLen;
    ostream::fmtflags const mFlags;

    void Escape(int inSym)
    {
        const char* const kHexChars = "0123456789ABCDEF";
        char              theBuf[3];
        theBuf[0] = '%';
        theBuf[1] = kHexChars[(inSym >> 4) & 0xF];
        theBuf[2] = kHexChars[inSym & 0xF];
        mOStream.write(theBuf, 3);
    }
    void Escape(
        const char* inPtr,
        size_t      inLen)
    {
        if (inLen <= 0) {
            return;
        }
        // Always escape the first leading and the last trailing spaces, if any,
        // in order to ensure leading and trailing spaces are not discarded by
        // key value tokenizer.
        const int         kSpace           = ' ';
        const bool        theLastSpaceFlag =
            kSpace == (inPtr[inLen - 1] & 0xFF);
        const char*       thePtr           = inPtr;
        const char* const theEndPtr        = thePtr + inLen -
            (theLastSpaceFlag ? 1 : 0);
        if (thePtr < theEndPtr && kSpace == (*thePtr & 0xFF)) {
            Escape(kSpace);
            ++thePtr;
        }
        const char* thePPtr = thePtr;
        while (thePtr < theEndPtr) {
            const int theSym = *thePtr & 0xFF;
            if (theSym < kSpace || 0xFF <= theSym || strchr("%=;/,", theSym)) {
                if (thePPtr < thePtr) {
                    mOStream.write(thePPtr, thePtr - thePPtr);
                }
                Escape(theSym);
                thePPtr = thePtr + 1;
            }
            ++thePtr;
        }
        if (thePPtr < theEndPtr) {
            mOStream.write(thePPtr, theEndPtr - thePPtr);
        }
        if (theLastSpaceFlag) {
            Escape(kSpace);
        }
    }
};

template<typename T>
    static const T&
MakeIoMetaRequestHandler(
    const T* inNullPtr = 0)
{
    const bool kShortNamesFlag = true;
    static T sHandler;
    return AddMetaRequestLog(sHandler, kShortNamesFlag);
}

typedef PropertiesTokenizerT<'=', ';'> MetaIoPropertiesTokenizer;
class IoDefinitionMethod
{
public:
    template<typename OBJ, typename PARSER>
    static PARSER& Define(
        PARSER&     inParser,
        const OBJ*  /* inNullPtr */)
        { return OBJ::IoParserDef(inParser); }
};
template <typename SUPER, typename OBJ>
class MetaRequestIoRequestParser : public RequestParser<
    SUPER,
    OBJ,
    StringEscapeIoParser,
    true, // Use short names / format.
    MetaIoPropertiesTokenizer,
    StringInsertEscapeOStream,
    false, // Do not invoke Validate
    MetaRequestDeleter,
    RequestParserShortNamesDictionary
> {};
typedef RequestHandler<
    MetaRequest,
    MetaRequestIoRequestParser,
    IoDefinitionMethod,
    StringInsertEscapeOStream,
    MetaIoPropertiesTokenizer::kSeparator,
    MetaIoPropertiesTokenizer::Token,
    RequestParserShortNamesDictionary
> MetaRequestIoHandler;
static const MetaRequestIoHandler& sMetaRequestIoHandler =
    MakeIoMetaRequestHandler<MetaRequestIoHandler>();

bool
MetaRequest::Write(ostream& os, bool omitDefaultsFlag) const
{
    StringInsertEscapeOStream theStream(os);
    return sMetaRequestIoHandler.Write(
        theStream,
        this,
        op,
        omitDefaultsFlag,
        MetaIoPropertiesTokenizer::kSeparator,
        MetaIoPropertiesTokenizer::kDelimiter
    );
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
    const bool kShortNamesFlag = true;
    static T sHandler;
    return AddMetaRequestLog(sHandler, kShortNamesFlag)
    .MakeParser("CS",
        META_CHUNK_SIZE,
        static_cast<const MetaChunkSize*>(0))
    .MakeParser("XD",
        META_REMOVE_FROM_DUMPSTER,
        static_cast<const MetaRemoveFromDumpster*>(0))
    .MakeParser("LA",
        META_LOG_CHUNK_ALLOCATE,
        static_cast<const MetaLogChunkAllocate*>(0))
    .MakeParser("LS",
        META_LOG_MAKE_CHUNK_STABLE,
        static_cast<const MetaLogMakeChunkStable*>(0))
    .MakeParser("LD",
        META_LOG_MAKE_CHUNK_STABLE_DONE,
        static_cast<const MetaLogMakeChunkStableDone*>(0))
    .MakeParser("LV",
        META_LOG_CHUNK_VERSION_CHANGE,
        static_cast<const MetaLogChunkVersionChange*>(0))
    .MakeParser("XS",
        META_LOG_CLEAR_OBJ_STORE_DELETE,
        static_cast<const MetaLogClearObjStoreDelete*>(0))
    .MakeParser("CA",
        META_CHUNK_AVAILABLE,
        static_cast<const MetaChunkAvailable*>(0))
    .MakeParser("LC",
        META_CHUNK_OP_LOG_COMPLETION,
        static_cast<const MetaChunkLogCompletion*>(0))
    .MakeParser("CC",
        META_CHUNK_CORRUPT,
        static_cast<const MetaChunkCorrupt*>(0))
    .MakeParser("MB",
        META_BYE,
        static_cast<const MetaBye*>(0))
    .MakeParser("SR",
        META_RETIRE_CHUNKSERVER,
        static_cast<const MetaRetireChunkserver*>(0))
    .MakeParser("HP",
        META_HIBERNATED_PRUNE,
        static_cast<const MetaHibernatedPrune*>(0))
    .MakeParser("HR",
        META_HIBERNATED_REMOVE,
        static_cast<const MetaHibernatedRemove*>(0))
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
    MetaRequestIoRequestParser,
    LogIoDefinitionMethod,
    StringInsertEscapeOStream,
    MetaIoPropertiesTokenizer::kSeparator,
    MetaIoPropertiesTokenizer::Token,
    RequestParserShortNamesDictionary
> MetaRequestLogIoHandler;
static const MetaRequestLogIoHandler& sMetaReplayIoHandler =
    MakeLogMetaRequestHandler<MetaRequestLogIoHandler>();

template<typename T>
    static bool
ValidateHandler(
    const T& inHandler,
    ostream& inErrStream)
{
    string        theIn;
    ostringstream theOut;
    bool          theRet = true;
    for (int i = 0; i < META_NUM_OPS_COUNT; i++) {
        TokenValue theName = inHandler.ObjIdToName(i);
        if (theName.mLen <= 0) {
            continue;
        }
        theIn.assign(theName.mPtr, theName.mLen);
        theIn += (char)MetaIoPropertiesTokenizer::kSeparator;
        MetaRequest* const theReqPtr =
            inHandler.Handle(theIn.data(), theIn.size());
        if (! theReqPtr) {
            inErrStream << "failed to parse: " << theIn << "\n";
            theRet = false;
            continue;
        }
        theOut.str(string());
        const bool kOmitDefaultsFlag = true;
        StringInsertEscapeOStream theStream(theOut);
        if (! inHandler.Write(
                theStream,
                theReqPtr,
                theReqPtr->op,
                kOmitDefaultsFlag,
                MetaIoPropertiesTokenizer::kSeparator,
                MetaIoPropertiesTokenizer::kDelimiter
            )) {
            inErrStream << "failed to write: " << theReqPtr->Show() << "\n";
            theRet = false;
            continue;
        }
        if (theOut.str() != theIn) {
            inErrStream << "ctor / defaults mismatch: " << theReqPtr->Show() <<
                " in:  "  << theIn <<
                " out: "  << theOut.str() <<
            "\n";
            theRet = false;
            continue;
        }
    }
    return theRet;
}

bool
ValidateMetaReplayIoHandler(
    ostream& inErrStream)
{
    return (
        ValidateHandler(sMetaRequestIoHandler, inErrStream) &&
        ValidateHandler(sMetaReplayIoHandler, inErrStream)
    );
}

/* static */ MetaRequest*
MetaRequest::ReadReplay(const char* buf, size_t len)
{
    return sMetaReplayIoHandler.Handle(buf, len);
}

bool
MetaRequest::WriteLog(ostream& os, bool omitDefaultsFlag) const
{
    StringInsertEscapeOStream theStream(os, "a/", 2);
    ReqOstream& theReqOstream = theStream.GetOStream();
    if (! sMetaReplayIoHandler.Write(
            theStream,
            this,
            op,
            omitDefaultsFlag,
            MetaIoPropertiesTokenizer::kSeparator,
            MetaIoPropertiesTokenizer::kDelimiter
        )) {
        return log(os);
    }
    theReqOstream.write("\n", 1);
    return true;
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
    if (*res && 0 <= (*res)->opSeqno) {
        return 0;
    }
    MetaRequest* const req = shortRpcFmtFlag ?
        sMetaRequestHandler.Handle(buf, reqLen) :
        sMetaRequestHandlerShortFmt.Handle(buf, reqLen);
    if (req) {
        if (0 <= req->opSeqno) {
            MetaRequest::Release(*res);
            *res = req;
            shortRpcFmtFlag = ! shortRpcFmtFlag;
        } else {
            MetaRequest::Release(req);
        }
    }
    return (*res ? 0 : -1);
}

int
ParseLogRecvCommand(const IOBuffer& ioBuf, int len, MetaRequest **res,
    char* threadParseBuffer)
{
    *res = 0;
    if (len <= 0 || MAX_RPC_HEADER_LEN < len) {
        return -1;
    }
    int               reqLen = len;
    const char* const buf    = ioBuf.CopyOutOrGetBufPtr(
        threadParseBuffer ? threadParseBuffer : sTempBuf, reqLen);
    assert(reqLen == len);
    *res = (reqLen == len) ?
        sMetaRequestLogRecvHandler.Handle(buf, reqLen) :
        0;
    return (*res ? 0 : -1);
}

} // Namespace KFS
