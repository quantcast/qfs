//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/09/25
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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
// Symmetric cryptographic keys table.
//
//----------------------------------------------------------------------------

#ifndef KFSIO_CRYPTO_KEYS_H
#define KFSIO_CRYPTO_KEYS_H

#include "common/kfstypes.h"

#include <istream>
#include <ostream>
#include <string>

#include <string.h>

class QCMutex;

namespace KFS
{

using std::istream;
using std::ostream;
using std::string;

class Properties;
class NetManager;

class CryptoKeys
{
private:
    class Impl;
public:
    typedef kfsKeyId_t KeyId;
    class Key
    {
    public:
        Key()
            { memset(mKey, 0, sizeof(mKey)); }
        Key(
            const char* inPtr)
            { memcpy(mKey, inPtr, sizeof(mKey)); }
        ~Key()
            { memset(mKey, 0, sizeof(mKey)); }
        Key(
            const Key& inKey)
        {
            if (this != &inKey) {
                memcpy(mKey, inKey.mKey, sizeof(mKey));
            }
        }
        Key& operator=(
            const Key& inKey)
        {
            memcpy(mKey, inKey.mKey, sizeof(mKey));
            return *this;
        }
        bool Parse(
            const char* inStrPtr,
            int         inStrLen);
        int ToString(
            char* inStrPtr,
            int   inMaxStrLen) const;
        ostream& Display(
            ostream& inStream) const;
        static int GetSize()
            { return kLength; }
        const char* GetPtr() const
            { return mKey; }
        char* WritePtr()
            { return mKey; }
        enum { kLength = 48 };
    private:
        char mKey[kLength];
        friend class Impl;
    };

    CryptoKeys(
        NetManager& inNetManager,
        QCMutex*    inMutexPtr);
    ~CryptoKeys();
    int SetParameters(
        const char*       inPrefixNamePtr,
        const Properties& inParameters,
        string&           outErrMsg);
    bool Find(
        KeyId inKeyId,
        Key&  outKey) const;
    int Read(
        istream& inStream);
    int Write(
        ostream&    inStream,
        const char* inDelimPtr = 0) const;
    bool GetCurrentKeyId(
        KeyId& outKeyId) const;
    bool GetCurrentKey(
        KeyId& outKeyId,
        Key&   outKey) const;
    bool GetCurrentKey(
        KeyId&    outKeyId,
        Key&      outKey,
        uint32_t& outKeyValidForSec) const;
    bool IsCurrentKeyValid() const
    {
        KeyId theId;
        return GetCurrentKeyId(theId);
    }
    static bool PseudoRand(
        void*  inPtr,
        size_t inLen);
private:
    Impl& mImpl;
private:
    CryptoKeys(
        const CryptoKeys& inKeys);
    CryptoKeys& operator=(
        const CryptoKeys& inKeys);
};

inline static ostream& operator<<(
    ostream&                inStream,
    const CryptoKeys::Key&  inKey)
{ return inKey.Display(inStream); }

} // namespace KFS

#endif /* KFSIO_CRYPTO_KEYS_H */
