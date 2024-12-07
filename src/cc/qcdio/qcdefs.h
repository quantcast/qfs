//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2024/09/30
// Author: Mike Ovsiannikov
//
// Copyright 2024 Quantcast Corporation. All rights reserved.
//
// This file is part of Quantcast File System (QFS).
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

#ifndef QC_DEFS_H
#define QC_DEFS_H

#if !defined(QC_GCC_13_PRAGMA) && defined(__GNUC__) && !defined(__clang__)
#if 13 <= __GNUC__
#   define QC_GCC_13_PRAGMA(x) _Pragma(#x)
#endif
#endif

#ifndef QC_GCC_13_PRAGMA
#   define QC_GCC_13_PRAGMA(x)
#endif

#endif /* QC_DEFS_H */
