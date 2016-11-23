//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2016/8/9
// Author: Mike Ovsiannikov
//
// Copyright 2014,2016 Quantcast Corporation. All rights reserved.
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
//
// Chunk / object store block's file name / key generation delcartion.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_BLOCK_NAME_H
#define KFSIO_BLOCK_NAME_H

#include "common/kfstypes.h"

#include <string>

namespace KFS
{
using std::string;

    bool
AppendChunkFileNameOrObjectStoreBlockKey(
    string&      inName,
    int64_t      inFileSystemId,
    kfsFileId_t  inFileId,
    kfsChunkId_t inId,
    kfsSeq_t     inVersion,
    string&      ioFileSystemIdSuffix);

} // namespace KFS

#endif /* KFSIO_BLOCK_NAME_H */
