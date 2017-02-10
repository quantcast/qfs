//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/7/28
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
// \file kfsdecls.cc
//
//----------------------------------------------------------------------------

#include "kfsdecls.h"

#include "IntToString.h"
#include "RequestParser.h"

#include "qcdio/QCUtils.h"

namespace KFS
{

    string&
ServerLocation::AppendToString(string& str, bool hexFmtFlag) const
{
    str.reserve(str.size() + hostname.size() + 16);
    str.append(hostname.data(), hostname.size());
    str.append(1, (char)' ');
    if (hexFmtFlag) {
        AppendHexIntToString(str, port);
    } else {
        AppendDecIntToString(str, port);
    }
    return str;
}

    bool
ServerLocation::ParseString(const char*& ptr, size_t len, bool hexFormatFlag)
{
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
    port = -1;
    if (! (hexFormatFlag ?
            HexIntParser::Parse(ptr, end - ptr, port) :
            DecIntParser::Parse(ptr, end - ptr, port)) || port < 0) {
        Reset(0, -1);
        return false;
    }
    return true;
}

    string
ErrorCodeToString(int status)
{
    switch (-status) {
        case EBADVERS:        return "version mismatch";
        case ELEASEEXPIRED:   return "lease has expired";
        case EBADCKSUM:       return "checksum mismatch";
        case EDATAUNAVAIL:    return "data not available";
        case ESERVERBUSY:     return "server busy";
        case EALLOCFAILED:    return "chunk allocation failed";
        case EBADCLUSTERKEY:
            return "cluster key, or server md5, or FS ID mismatch";
        case EINVALCHUNKSIZE: return "invalid chunk size";
        case ELOGFAILED:
            return "meta server transaction log write failure";
        case EVRNOTPRIMARY:   return "meta server node is not primary";
        case EVRBACKUP:       return "meta server node is backup";
        case EKFSYSSERROR:    return "system error";
        case 0:               return "";
        default:              break;
    }
    return QCUtils::SysError(-status);
}

} // namespace KFS
