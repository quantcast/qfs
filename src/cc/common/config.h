
//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/08/23
// Author: Sriram Rao
//
// Copyright 2008 Quantcast Corp.
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

#ifndef CC_CONFIG_H
#define CC_CONFIG_H

//----------------------------------------------------------------------------
// Attribute to disable "unused variable" warnings
//
// Example:
//   int UNUSED_ATTR r = aFunctionThatAlwaysAlwaysAlwaysReturnsZero();
//   assert(r == 0);
//
// Note, this doesn't break the variable when it actually *is* used,
// as in a debug build.  It just makes the compiler keep quiet about
// not using it in release builds.
//----------------------------------------------------------------------------
#if !defined(UNUSED_ATTR)
#if defined(__GNUC__)
#define UNUSED_ATTR __attribute__((unused))
#else
#define UNUSED_ATTR
#endif
#endif


#endif // CC_CONFIG_H
