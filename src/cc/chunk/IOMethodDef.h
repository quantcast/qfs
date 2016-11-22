//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/11
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
// Storage IO method definitions.
//
//----------------------------------------------------------------------------

#ifndef KFS_CHUNK_IO_METHOD_DEF_H
#define KFS_CHUNK_IO_METHOD_DEF_H

#include "IOMethod.h"

#define KFS_MAKE_REGISTERED_IO_METHOD_NAME(inType) \
    IOMethodRegistry_KFS_IO_METHOD_##inType
#define KFS_DECLARE_IO_METHOD(inType) \
    IOMethod* KFS_MAKE_REGISTERED_IO_METHOD_NAME(inType)( \
    const char*       inUrlPtr,          \
    const char*       inLogPrefixPtr,    \
    const char*       inParamsPrefixPtr, \
    const Properties& inParameters       \
    )
#define KFS_REGISTER_IO_METHOD(inType, inMethod) \
    extern KFS_DECLARE_IO_METHOD(inType); \
    KFS_DECLARE_IO_METHOD(inType) \
    { \
        return inMethod(       \
            inUrlPtr,          \
            inLogPrefixPtr,    \
            inParamsPrefixPtr, \
            inParameters       \
        ); \
    }

#endif /* KFS_CHUNK_IO_METHOD_DEF_H */
