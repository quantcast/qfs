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
#include <string.h>

class QCMutex;

namespace KFS
{

using std::istream;
using std::ostream;

class Properties;
class NetManager;

class CryptoKeys
{
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
        int GetSize() const
            { return kLength; }
        const char* GetPtr() const
            { return mKey; }
    private:
        enum { kLength = 48 };
        char mKey[kLength];
    };

    CryptoKeys(
        NetManager& inNetManager,
        QCMutex*    inMutexPtr);
    ~CryptoKeys();
    int SetParameters(
        const char* inPrefixNamePtr,
        Properties& inParameters);
    const Key* Find(
        KeyId inKeyId) const;
    istream& Read(
        istream& inStream);
    ostream& Write(
        ostream& inStream) const;
    kfsKeyId_t GetCurrentKeyId() const;
    kfsKeyId_t GetCurrentKey(
        Key& outKey) const;
private:
    class Impl;
    Impl& mImpl;
private:
    CryptoKeys(
        const CryptoKeys& inKeys);
    CryptoKeys& operator=(
        const CryptoKeys& inKeys);
};

} // namespace KFS

#endif /* KFSIO_CRYPTO_KEYS_H */
