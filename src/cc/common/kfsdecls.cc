//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/7/28
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
// \file kfsdecls.cc
//
//----------------------------------------------------------------------------

#include "kfsdecls.h"

#include "IntToString.h"
#include "RequestParser.h"

namespace KFS
{

    string
ServerLocation::ToString() const
{
    string ret;
    ret.reserve(hostname.size() + 16);
    ret.assign(hostname.data(), hostname.size());
    ret.append(1, (char)' ');
    AppendDecIntToString(ret, port);
    return ret;
}

    bool
ServerLocation::FromString(const char* str, size_t len)
{
    const char*       ptr = str;
    const char* const end = ptr + len;
    while (ptr < end && (*ptr & 0xFF) <= ' ' && *ptr) {
        ++ptr;
    }
    const char* const sptr = ptr;
    while (ptr < end && (*ptr & 0xFF) > ' ') {
        ++ptr;
    }
    if (ptr <= sptr || *ptr == 0) {
        Reset(0, -1);
        return false;
    }
    hostname.assign(sptr, ptr - sptr);
    if (! DecIntParser::Parse(ptr, end - ptr, port) || port < 0) {
        Reset(0, -1);
        return false;
    }
    return true;
}

} // namespace KFS
