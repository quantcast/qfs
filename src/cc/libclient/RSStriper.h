//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/07/27
// Author: Mike Ovsiannikov
//
// Copyright 2010-2012 Quantcast Corp.
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
//----------------------------------------------------------------------------

#ifndef KFS_LIBCLIENT_RSSTRIPER_H
#define KFS_LIBCLIENT_RSSTRIPER_H

#include "Writer.h"
#include "Reader.h"

namespace KFS
{
namespace client
{
using std::string;

Writer::Striper* RSStriperCreate(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    Writer::Striper::Offset  inFileSize,
    const string&            inLogPrefix,
    Writer::Striper::Impl&   inOuter,
    Writer::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg);

Reader::Striper* RSStriperCreate(
    int                      inType,
    int                      inStripeCount,
    int                      inRecoveryStripeCount,
    int                      inStripeSize,
    int                      inMaxAtomicReadRequestSize,
    bool                     inUseDefaultBufferAllocatorFlag,
    bool                     inFailShortReadsFlag,
    Reader::Striper::Offset  inRecoverChunkPos,
    Reader::Striper::Offset  inFileSize,
    Reader::Striper::SeqNum  inInitialSeqNum,
    const string&            inLogPrefix,
    Reader::Striper::Impl&   inOuter,
    Reader::Striper::Offset& outOpenChunkBlockSize,
    string&                  outErrMsg);

bool RSStriperValidate(
    int     inType,
    int     inStripeCount,
    int     inRecoveryStripeCount,
    int     inStripeSize,
    string* outErrMsgPtr);

}}

#endif /* KFS_LIBCLIENT_RSSTRIPER_H */
