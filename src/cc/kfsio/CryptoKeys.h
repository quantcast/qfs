//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/09/25
// Author: Mike Ovsiannikov
//
// Copyright 2013-2025 Quantcast Corporation. All rights reserved.
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
        class UrlSafeFmt
        {
        public:
            UrlSafeFmt(
                const Key& inKey)
                : mKey(inKey)
                {}
            ostream& Display(
                ostream& inStream) const
                { return mKey.Display(inStream, true); }
        private:
            const Key& mKey;
        private:
            UrlSafeFmt(
                const UrlSafeFmt& inFormat);
            UrlSafeFmt& operator=(
                const UrlSafeFmt& inFormat);
        };
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
        bool operator==(
            const Key& inRhs) const
            { return (memcmp(mKey, inRhs.mKey, sizeof(mKey)) == 0); }
        bool Parse(
            const char* inStrPtr,
            int         inStrLen,
            bool        inUrlSafeFmtFlag = false);
        int ToString(
            char* inStrPtr,
            int   inMaxStrLen,
            bool  inUrlSafeFmtFlag = false) const;
        ostream& Display(
            ostream& inStream,
            bool     inUrlSafeFmtFlag = false) const;
        KFS_CONSTEXPR static int GetSize()
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

    class KeyStore
    {
    public:
        KeyStore()
            : mActiveFlagPtr(0)
            {}
        bool IsActive() const
            { return (mActiveFlagPtr && *mActiveFlagPtr); }
        virtual bool NewKey(
            KeyId      inKeyId,
            const Key& inKey,
            int64_t    inKeyTime) = 0;
        virtual bool Expired(
            KeyId inKeyId) = 0;
        virtual void WriteKey(
            ostream*   inStreamPtr,
            KeyId      inKeyId,
            const Key& inKey,
            int64_t    inKeyTime) = 0;
    protected:
        const bool* mActiveFlagPtr;

        virtual ~KeyStore()
            {}
        KeyStore(
            const KeyStore& /* inStore */)
            {}
        KeyStore& operator=(
            const KeyStore& /* inStore */)
            { return *this; }
    };

    CryptoKeys(
        NetManager& inNetManager,
        KeyStore*   inKeyStorePtr = 0);
    ~CryptoKeys();
    int SetParameters(
        const char*       inPrefixNamePtr,
        const Properties& inParameters,
        string&           outErrMsg);
    int Start();
    void Stop();
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
    bool Add(
        KeyId      inKeyId,
        const Key& inKey,
        int64_t    inKeyTime);
    bool Remove(
        KeyId inKeyId,
        bool  inPendingFlag);
    void Clear();
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
    int LoadKeysFile(
        string& outErrMsg);
    bool EnsureHasCurrentKey();
    static bool PseudoRand(
        void*  inPtr,
        size_t inLen);
    static bool Rand(
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

inline static ostream& operator<<(
    ostream&                           inStream,
    const CryptoKeys::Key::UrlSafeFmt& inKeyFormat)
{ return inKeyFormat.Display(inStream); }

} // namespace KFS

#endif /* KFSIO_CRYPTO_KEYS_H */
