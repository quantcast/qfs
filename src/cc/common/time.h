//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2010/10/27
// Author: Dan Adkins
//
// Copyright 2010 Quantcast Corp.
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
// \file time.cc
// \brief time related functions
//
//----------------------------------------------------------------------------

#ifndef COMMON_TIME_H
#define COMMON_TIME_H

#include <stdint.h>

namespace KFS {

extern int64_t microseconds(void);
extern int64_t cputime(int64_t *user, int64_t *sys);

} // namespace KFS

#endif // COMMON_TIME_H
