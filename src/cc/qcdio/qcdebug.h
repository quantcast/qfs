//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2008/11/01
// Author: Mike Ovsiannikov
//
// Copyright 2008-2012 Quantcast Corp.
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
// Debug definitions.
// 
//----------------------------------------------------------------------------

#ifndef QCDEBUG_H
#define QCDEBUG_H

#include <assert.h>

#define QCASSERT(a) assert(a)

#ifdef NDEBUG
#   define QCVERIFY(a) if (!(a)) assert(0)
#else
#   define QCVERIFY(a) QCASSERT(a)
#endif

#endif /* QCDEBUG_H */
