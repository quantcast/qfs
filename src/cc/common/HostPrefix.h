//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2012/10/31
// Author: Mike Ovsiannikov
//
// Copyright 2012 Quantcast Corp.
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
// \file HostPrefix.h
// \brief Match host by [ip [v4]] prefix.
//
//----------------------------------------------------------------------------

#ifndef KFS_HOSTPREFIX_H
#define KFS_HOSTPREFIX_H

#include "kfsdecls.h"

#include <string.h>
#include <string>
#include <istream>
#include <ostream>
#include <sstream>

namespace KFS
{
using std::string;
using std::vector;
using std::istream;
using std::ostringstream;
using std::make_pair;

class HostPrefix
{
public:
    HostPrefix()
        : mLen(0),
          mMinLen(0)
        {}
    bool operator==(
        const HostPrefix& inOther) const
    {
        return (mLen == inOther.mLen &&
            mMinLen == inOther.mMinLen &&
            memcmp(mPrefix, inOther.mPrefix, mLen) == 0);
    }
    bool Match(
        const string& inHost) const
    {
        return (inHost.length() >= mMinLen &&
            memcmp(inHost.data(), mPrefix, mLen) == 0);

    }
    size_t Parse(
        const string& inPref)
    {
        // Allow to position prefix with trailing ??
        // For example: 10.6.34.2?
        mMinLen = min(sizeof(mPrefix) / sizeof(mPrefix[0]), inPref.length());
        mLen    = inPref.find('?');
        if (mLen == string::npos || mMinLen < mLen) {
            mLen = mMinLen;
        }
        memcpy(mPrefix, inPref.data(), mLen);
        return mMinLen;
    }
private:
    char   mPrefix[64];
    size_t mLen;
    size_t mMinLen;
};

template<typename T>
class HostPrefixMap
{
public:
    HostPrefixMap()
        : mHostPrefixes()
        {}
    void clear()
        { mHostPrefixes.clear(); }
    template<typename TValidator>
    void Load(
        istream&    inStream,
        TValidator* inValidatorPtr = 0,
        const T&    inDefault      = T())
    {
        string        thePref;
        T             theId = inDefault;
        HostPrefix    theHostPref;
        thePref.reserve(256);
        while ((inStream >> thePref >> theId)) {
            if (theHostPref.Parse(thePref) > 0 &&
                    (! inValidatorPtr || (*inValidatorPtr)(thePref, theId))) {
                mHostPrefixes.push_back(make_pair(theHostPref, theId));
            }
            theId = inDefault;
            thePref.clear();
        }
    }
    T GetId(
        const ServerLocation& inLocation,
        const T&              inDefault,
        bool                  inUsePortFlag) const
    {
        if (inUsePortFlag) {
            if (mHostPrefixes.empty()) {
                return inDefault;
            }
            ostringstream theStream;
            theStream << inLocation.hostname << ":" << inLocation.port;
            const string theName = theStream.str();
            return GetId(theName, inDefault);
        } else {
            return GetId(inLocation.hostname, inDefault);
        }
    }
    T GetId(
        const string& inName,
        const T&      inDefault) const
    {
        typename HostPrefixes::const_iterator theIt = mHostPrefixes.begin();
        while (theIt != mHostPrefixes.end()) {
            if (theIt->first.Match(inName)) {
                return theIt->second;
            }
            ++theIt;
        }
        return inDefault;
    }
private:
    typedef vector<pair<
        HostPrefix,
        T
    > > HostPrefixes;

    HostPrefixes mHostPrefixes;
};

}

#endif /* KFS_HOSTPREFIX_H */
