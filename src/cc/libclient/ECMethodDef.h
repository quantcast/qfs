//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/08/18
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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
// Erasure code method definitions.
//
//----------------------------------------------------------------------------

#ifndef KFS_LIBCLIENT_ECMETHOD_DEF_H
#define KFS_LIBCLIENT_ECMETHOD_DEF_H

#include "ECMethod.h"

#define KFS_MAKE_REGISTERED_EC_METHOD_NAME(inType) \
    ECMethod_KFS_EC_METHOD_##inType
#define KFS_DECLARE_EC_METHOD_PTR(inType) \
    ECMethod* const KFS_MAKE_REGISTERED_EC_METHOD_NAME(inType)
#define KFS_REGISTER_EC_METHOD(inType, inMethodPtr) \
    extern KFS_DECLARE_EC_METHOD_PTR(inType); \
    KFS_DECLARE_EC_METHOD_PTR(inType) = inMethodPtr

#endif /* KFS_LIBCLIENT_ECMETHOD_DEF_H */
