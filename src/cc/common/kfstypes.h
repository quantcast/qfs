//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// \brief Common declarations for KFS (meta/chunk/client-lib)
//
// Created 2006/10/20
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
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

#ifndef COMMON_KFSTYPES_H
#define COMMON_KFSTYPES_H

#include <stdlib.h>
#include <stdint.h>

#include <cerrno>
#include <cstddef>
#include <cassert>

namespace KFS {

typedef int64_t seq_t;        //!< request sequence no. for logging
typedef int64_t seqid_t;      //!< sequence number id's for file/chunks
typedef seqid_t fid_t;        //!< file ID
typedef seqid_t chunkId_t;    //!< chunk ID
typedef int64_t chunkOff_t;   //!< chunk offset
const fid_t ROOTFID = 2;      //!< special fid for "/

//!< Every time we change the protocol, rev. this one. We can use this value to
//!< detect clients running old binaries.
const int KFS_CLIENT_PROTO_VERS = 114;
const int KFS_CLIENT_MIN_STRIPED_FILE_SUPPORT_PROTO_VERS = 110;

//!< Declarations as used in the Chunkserver/client-library
typedef int64_t kfsFileId_t;
typedef int64_t kfsChunkId_t;
typedef int64_t kfsSeq_t;

typedef uint32_t kfsUid_t;
typedef uint32_t kfsGid_t;
typedef uint16_t kfsMode_t;

const kfsUid_t  kKfsUserRoot  = 0;
const kfsUid_t  kKfsUserNone  = ~kfsUid_t(0);
const kfsGid_t  kKfsGroupRoot = 0;
const kfsGid_t  kKfsGroupNone = ~kfsGid_t(0);
const kfsMode_t kKfsModeUndef = ~kfsMode_t(0);

const size_t CHUNKSIZE = 64u << 20; //!< (64MB)
const int MAX_RPC_HEADER_LEN = 16 << 10; //!< Max length of header in RPC req/response
const size_t MAX_FILE_NAME_LENGTH = 4 << 10;
const size_t MAX_PATH_NAME_LENGTH = MAX_FILE_NAME_LENGTH * 3;
const short int NUM_REPLICAS_PER_FILE = 3; //!< default degree of replication
const short int MAX_REPLICAS_PER_FILE = 64; //!< max. replicas per chunk of file

//!< Default lease interval of 5 mins
const int LEASE_INTERVAL_SECS = 300;

//!< Error codes for KFS specific errors
// version # being presented by client doesn't match what the server has
const int EBADVERS = 1000;

// lease has expired
const int ELEASEEXPIRED = 1001;

// checksum for data on a server is bad; client should read from elsewhere
const int EBADCKSUM = 1002;

// data lives on chunkservers that are all non-reachable
const int EDATAUNAVAIL = 1003;

// an error to indicate a server is busy and can't take on new work
const int ESERVERBUSY = 1004;

// an error occurring during allocation; the client will see this error
// code and retry.
const int EALLOCFAILED = 1005;

// error to indicate that there is a cluster key mismatch between
// chunkserver and metaserver.
const int EBADCLUSTERKEY = 1006;

// invalid chunk size
const int EINVALCHUNKSIZE = 1007;

enum StripedFileType
{
    KFS_STRIPED_FILE_TYPE_UNKNOWN = 0,
    KFS_STRIPED_FILE_TYPE_NONE    = 1,
    KFS_STRIPED_FILE_TYPE_RS      = 2
};

const int KFS_STRIPE_ALIGNMENT = 4096;
const int KFS_MIN_STRIPE_SIZE  = KFS_STRIPE_ALIGNMENT;
const int KFS_MAX_STRIPE_SIZE  = (int)CHUNKSIZE;

}

#endif // COMMON_KFSTYPES_H
