//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// \brief Common declarations of KFS structures
//
// Created 2006/10/20
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
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

#ifndef COMMON_KFSDECLS_H
#define COMMON_KFSDECLS_H

#include "kfstypes.h"

#include <cassert>
#include <cerrno>
#include <string>
#include <sstream>
#include <stdlib.h>

namespace KFS
{
using std::ostringstream;
using std::istringstream;
using std::ostream;
using std::istream;
using std::string;
using std::min;

///
/// Define a server process' location: hostname and the port at which
/// it is listening for incoming connections
///
struct ServerLocation {
    ServerLocation(): hostname(""), port(-1) { }
    ServerLocation(const ServerLocation &other):
        hostname(other.hostname), port(other.port) { }
    ServerLocation(const string &h, int p): hostname(h), port(p) { }
    ServerLocation & operator = (const ServerLocation &other) {
        hostname = other.hostname;
        port = other.port;
        return *this;
    }
    void Reset(const char *h, int p) {
        hostname = h;
        port = p;
    }
    bool operator == (const ServerLocation &other) const {
        return hostname == other.hostname && port == other.port;
    }
    bool operator != (const ServerLocation &other) const {
        return hostname != other.hostname || port != other.port;
    }
    bool operator < (const ServerLocation &other) const {
        const int res = hostname.compare(other.hostname);
        return (res < 0 || (res == 0 && port < other.port));
    }
    bool IsValid() const {
        // Hostname better be non-null and port better
        // be a positive number
        return (! hostname.empty() && port > 0);
    }

    // a proxy for distance between two hosts: take the difference
    // between their hostnames.  this will mostly work as long as all
    // machines in the cluster are named as nodeXXX, where XXX is a number
    int Distance(const string &otherhost) {
        int len = (int) min(hostname.size(), otherhost.size());
        int hosta = 0, hostb = 0;
        int scalefactor = 1;

        for (int i = len - 1; i >= 0; --i) {
            if (isdigit(hostname[i]))
                hosta += (hostname[i] - '0') * scalefactor;
            if (isdigit(otherhost[i]))
                hostb += (otherhost[i] - '0') * scalefactor;
            scalefactor *= 10;
        }
        return abs(hosta - hostb);
    }
    string ToString() const {
        ostringstream os;
        Display(os);
        return os.str();
    }
    ostream& Display(ostream& os) const {
        return (os << hostname << ' ' << port);
    }
    void FromString(const string &s) {
        istringstream is(s);
        is >> hostname;
        is >> port;
    }

    string hostname; //!< Location of the server: machine name/IP addr
    int port; //!< Location of the server: port to connect to
};

inline static ostream&
operator<<(ostream& os, const ServerLocation& loc) {
    return loc.Display(os);
}
inline static istream&
operator>>(istream& is, ServerLocation& loc) {
    return (is >> loc.hostname >> loc.port);
}

// I-node (file / directory) permissions.
class Permissions
{
public:
    enum PBits
    {
        kExec  = 1,
        kWrite = 2,
        kRead  = 4
    };
    enum { kStickyBit      = 1 << (3 * 3) };
    enum { kAccessModeMask = 0777 };
    enum { kFileModeMask   = kAccessModeMask };
    enum { kDirModeMask    = kStickyBit | kAccessModeMask };

    kfsUid_t  user;
    kfsGid_t  group;
    kfsMode_t mode;

    Permissions(
        kfsUid_t  u = kKfsUserNone,
        kfsGid_t  g = kKfsGroupNone,
        kfsMode_t m = kKfsModeUndef)
        : user(u),
          group(g),
          mode(m)
         {}
    kfsMode_t GetPermissions(kfsUid_t euser, kfsGid_t egroup) const
    {
        if (user == euser) {
            return ((mode >> 6) & 0x7);
        }
        if (group == egroup) {
            return ((mode >> 3) & 0x7);
        }
        return (mode & 0x7);
    }
    bool Access(kfsUid_t euser, kfsGid_t egroup, PBits perm) const
    {
        return (euser == kKfsUserRoot ||
            (GetPermissions(euser, egroup) & perm) != 0);
    }
    bool CanExec(kfsUid_t euser, kfsGid_t egroup) const
        { return Access(euser, egroup, kExec); }
    bool CanWrite(kfsUid_t euser, kfsGid_t egroup) const
        { return Access(euser, egroup, kWrite); }
    bool CanRead(kfsUid_t euser, kfsGid_t egroup) const
        { return Access(euser, egroup, kRead); }
    bool CanSearch(kfsUid_t euser, kfsGid_t egroup) const
        { return Access(euser, egroup, kExec); }
    bool IsAnyPermissionDefined() const
    {
        return (
            mode != kKfsModeUndef ||
            user != kKfsUserNone ||
            group != kKfsGroupNone
        );
    }
    bool IsPermissionValid() const
    {
        return (
            mode != kKfsModeUndef &&
            user != kKfsUserNone &&
            group != kKfsGroupNone
        );
    }
    bool IsSticky() const { return (mode & kStickyBit); }
    void SetSticky(bool flag)
    {
        if (flag) {
            mode |= kfsMode_t(kStickyBit);
        } else {
            mode &= ~kfsMode_t(kStickyBit);
        }
    }
};

}

#endif // COMMON_KFSDECLS_H
