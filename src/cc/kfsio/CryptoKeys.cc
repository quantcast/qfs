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
//
//----------------------------------------------------------------------------

#include "CryptoKeys.h"
#include "NetManager.h"
#include "ITimeout.h"
#include "common/Properties.h"
#include "common/StdAllocator.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include <openssl/rand.h>

#include <istream>
#include <ostream>
#include <map>
#include <utility>
#include <functional>

namespace KFS
{

using std::istream;
using std::ostream;
using std::map;
using std::less;
using std::pair;

class CryptoKeys::Impl : public ITimeout
{
public:
    typedef CryptoKeys::Key   Key;
    typedef CryptoKeys::KeyId KeyId;

    Impl(
        NetManager& inNetManager,
        QCMutex*    inMutexPtr)
        : ITimeout(),
          mNetManager(inNetManager),
          mMutexPtr(inMutexPtr),
          mKeys(),
          mCurrentKeyId(),
          mCurrentKey(),
          mKeyValidTime(2 * 60 * 60),
          mKeyChangePeriod(mKeyValidTime / 2),
          mNextKeyGenTime(mNetManager.Now() -  mKeyValidTime)
    {
        mNetManager.RegisterTimeoutHandler(this);
        if (! GenKey(mCurrentKey)) {
        }
        if (! GenKeyId(mCurrentKeyId)) {
        }
    }
    virtual ~Impl()
        { mNetManager.UnRegisterTimeoutHandler(this); }
    int SetParameters(
        const char* inNamesPrefixPtr,
        Properties& inParameters,
        string&     outErrMsg)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        Properties::String theParamName;
        if (inNamesPrefixPtr) {
            theParamName.Append(inNamesPrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        const int theKeyValidTime = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "keyValidTimeSec"), mKeyValidTime);
        const int k10Min = 60 * 10;
        if (theKeyValidTime < k10Min) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: less or equal 10 minutes";
            return -EINVAL;
        }
        const int theKeyChangePeriod = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "keyChangePeriodSec"), mKeyChangePeriod);
        if (theKeyChangePeriod < k10Min / 2 ||
                 theKeyValidTime < (int64_t)theKeyChangePeriod * (10 << 10)) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: less than keyValidTimeSec / 10240";
            return -EINVAL;
        }
        return 0;
    }
    kfsKeyId_t GetCurrentKeyId() const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        return mCurrentKeyId;
    }
    kfsKeyId_t GetCurrentKey(
        Key& outKey) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        outKey = mCurrentKey;
        return mCurrentKeyId;
    }
    const Key* Find(
        KeyId inKeyId) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        return 0;
    }
    istream& Read(
        istream& inStream)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        return inStream;
    }
    ostream& Write(
        ostream& inStream) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        return inStream;
    }
    virtual void Timeout()
    {
        if (mNetManager.Now() < mNextKeyGenTime) {
            return;
        }
    }
private:
    typedef map<
        KeyId,
        Key,
        less<KeyId>,
        StdFastAllocator<
            pair<const KeyId, Key>
        >
    > Keys;
    NetManager&     mNetManager;
    QCMutex* const  mMutexPtr;
    Keys            mKeys;
    kfsKeyId_t      mCurrentKeyId;
    Key             mCurrentKey;
    int             mKeyValidTime;
    int             mKeyChangePeriod;
    volatile time_t mNextKeyGenTime;

    static bool GenKey(
        Key& outKey)
    {
        return RAND_bytes(
            reinterpret_cast<unsigned char*>(outKey.mKey), Key::kLength) > 0;
    }
    static bool GenKeyId(
        kfsKeyId_t& outKeyId)
    {
        return RAND_pseudo_bytes(
            reinterpret_cast<unsigned char*>(&outKeyId),
            (int)sizeof(outKeyId)) > 0;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

CryptoKeys::CryptoKeys(
    NetManager& inNetManager,
    QCMutex*    inMutexPtr)
    : mImpl(*(new Impl(inNetManager, inMutexPtr)))
{}

CryptoKeys::~CryptoKeys()
{
    delete &mImpl;
}

    int
CryptoKeys::SetParameters(
    const char* inNamesPrefixPtr,
    Properties& inParameters,
    string&     outErrMsg)
{
    return mImpl.SetParameters(inNamesPrefixPtr, inParameters, outErrMsg);
}

    const CryptoKeys::Key*
CryptoKeys::Find(
    CryptoKeys::KeyId inKeyId) const
{
    return mImpl.Find(inKeyId);
}

    istream&
CryptoKeys::Read(
    istream& inStream)
{
    return mImpl.Read(inStream);
}

    ostream&
CryptoKeys::Write(
    ostream& inStream) const
{
    return mImpl.Write(inStream);
}

    kfsKeyId_t
CryptoKeys::GetCurrentKeyId() const
{
    return mImpl.GetCurrentKeyId();
}

    kfsKeyId_t
CryptoKeys::GetCurrentKey(
    CryptoKeys::Key& outKey) const
{
    return mImpl.GetCurrentKey(outKey);
}

} // namespace KFS

