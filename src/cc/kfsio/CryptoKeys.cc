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
#include "Base64.h"
#include "common/Properties.h"
#include "common/StdAllocator.h"
#include "common/MsgLogger.h"
#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include <openssl/rand.h>
#include <openssl/err.h>

#include <istream>
#include <ostream>
#include <map>
#include <deque>
#include <utility>
#include <functional>

namespace KFS
{

using std::istream;
using std::ostream;
using std::map;
using std::less;
using std::pair;
using std::make_pair;
using std::deque;
using std::min;

class OpenSslError
{
public:
    OpenSslError(
        unsigned long inError)
        : mError(inError)
        {}
    ostream& Display(
        ostream& inStream) const
    {
        const size_t kBufSize = 127;
        char theBuf[kBufSize + 1];
        theBuf[0] = 0;
        theBuf[kBufSize] = 0;
        ERR_error_string_n(mError, theBuf, kBufSize);
        return (inStream << theBuf);
    }
public:
    unsigned long const mError;
};
inline static ostream& operator<<(
    ostream&             inStream,
    const OpenSslError&  inError)
{ return inError.Display(inStream); }

static string GetErrorMsg(
    unsigned long inError)
{
    const size_t kBufSize = 127;
    char theBuf[kBufSize + 1];
    theBuf[0] = 0;
    theBuf[kBufSize] = 0;
    ERR_error_string_n(inError, theBuf, kBufSize);
    return string(theBuf);
}

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
          mKeysExpirationQueue(),
          mCurrentKeyId(),
          mCurrentKey(),
          mKeyValidTime(2 * 60 * 60),
          mKeyChangePeriod(mKeyValidTime / 2),
          mNextKeyGenTime(mNetManager.Now() +  mKeyChangePeriod),
          mError(0)
    {
        mNetManager.RegisterTimeoutHandler(this);
        GenKey(mCurrentKey);
        GenKeyId(mCurrentKeyId);
    }
    virtual ~Impl()
        { mNetManager.UnRegisterTimeoutHandler(this); }
    int SetParameters(
        const char* inNamesPrefixPtr,
        Properties& inParameters,
        string&     outErrMsg)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mError) {
            mError = 0;
            GenKey(mCurrentKey);
            GenKeyId(mCurrentKeyId);
        }
        if (mError) {
            outErrMsg = GetErrorMsg(mError);
            return -EFAULT;
        }
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
        const bool theExpireKeysFlag = theKeyValidTime < mKeyValidTime;
        mKeyValidTime = theKeyValidTime;
        if (mKeyChangePeriod != theKeyChangePeriod) {
            mKeyChangePeriod = theKeyChangePeriod;
            mNextKeyGenTime  = min(
                time_t(mNextKeyGenTime), time(0) + mKeyChangePeriod);
        }
        if (theExpireKeysFlag) {
            ExpireKeys(time(0) - mKeyValidTime);
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
    bool Find(
        KeyId inKeyId,
        Key&  outKey) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (inKeyId == mCurrentKeyId) {
            outKey = mCurrentKey;
            return true;
        }
        Keys::const_iterator const theIt = mKeys.find(inKeyId);
        if (theIt == mKeys.end()) {
            return false;
        }
        outKey = theIt->second;
        return true;
    }
    int Read(
        istream& inStream)
    {
        int     theKeyCount         = -1;
        int64_t theFirstKeyTime     = -1;
        int     theKeysTimeInterval = -1;
        if (! (inStream >> theKeyCount >> theFirstKeyTime >> theKeysTimeInterval)
                || theKeysTimeInterval <= 0) {
            return -EINVAL;
        }
        KeyId               theKeyId;
        string              theKeyStr;
        Keys                theKeys;
        KeysExpirationQueue theExpQueue;
        Key                 theKey;
        time_t              theKeyTime = theFirstKeyTime;
        const time_t        theTimeNow = time(0);
        for (int i = 0; i < theKeyCount; i++) {
            if (! (inStream >> theKeyId >> theKeyStr)) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid [" << i << "] {id,key} tuple" <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (Base64::GetMaxDecodedLength(theKeyStr.size()) !=
                    Key::GetSize() ||
                    Base64::Decode(
                        theKeyStr.data(),
                        theKeyStr.size(),
                        theKey.mKey) != Key::GetSize()) {
                KFS_LOG_STREAM_ERROR <<
                    "invalid [" << i << "] key " << theKeyStr <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            if (theKeyTime < theTimeNow) {
                KFS_LOG_STREAM_INFO <<
                    "ignoring expired key " << i << " " << theKeyId <<
                KFS_LOG_EOM;
            }
            pair<Keys::iterator, bool> const theRes = theKeys.insert(
                make_pair(theKeyId, theKey));
            if (! theRes.second) {
                KFS_LOG_STREAM_ERROR <<
                    "duplicate key at index " << i <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
            theExpQueue.push_back(make_pair(theKeyTime, theRes.first));
        }
        QCStMutexLocker theLocker(mMutexPtr);
        mKeys.swap(theKeys);
        mKeysExpirationQueue.swap(theExpQueue);
        ExpireKeys(theTimeNow - mKeyValidTime);
        return (int)mKeys.size();
    }
    int Write(
        ostream& inStream) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        return 0;
    }
    virtual void Timeout()
    {
        if (mNetManager.Now() < mNextKeyGenTime) {
            return;
        }
        QCStMutexLocker theLocker(mMutexPtr);
        const time_t theTimeNow = mNetManager.Now();
        ExpireKeys(theTimeNow - mKeyValidTime);
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
    typedef deque<
        pair<time_t, Keys::iterator>
    > KeysExpirationQueue;
    NetManager&         mNetManager;
    QCMutex* const      mMutexPtr;
    Keys                mKeys;
    KeysExpirationQueue mKeysExpirationQueue;
    kfsKeyId_t          mCurrentKeyId;
    Key                 mCurrentKey;
    int                 mKeyValidTime;
    int                 mKeyChangePeriod;
    volatile time_t     mNextKeyGenTime;
    unsigned long       mError;

    bool GenKey(
        Key& outKey)
    {
        const bool theRet = RAND_bytes(
            reinterpret_cast<unsigned char*>(outKey.mKey), Key::kLength) > 0;
        if (! theRet) {
            mError = ERR_get_error();
            KFS_LOG_STREAM_ERROR << "RAND_bytes failure: " <<
                OpenSslError(mError) <<
            KFS_LOG_EOM;
        }
        return theRet;
    }
    bool GenKeyId(
        kfsKeyId_t& outKeyId)
    {
        const bool theRet = RAND_pseudo_bytes(
            reinterpret_cast<unsigned char*>(&outKeyId),
            (int)sizeof(outKeyId)) > 0;
        if (! theRet) {
            mError = ERR_get_error();
            KFS_LOG_STREAM_ERROR << "RAND_pseudo_bytes failure: " <<
                OpenSslError(mError) <<
            KFS_LOG_EOM;
        }
        return theRet;
    }
    void ExpireKeys(
        time_t inExpirationTime)
    {
        QCASSERT(! mMutexPtr || mMutexPtr->IsOwned());
        while (! mKeysExpirationQueue.empty()) {
            const KeysExpirationQueue::value_type& theCur =
                mKeysExpirationQueue.front();
            if (inExpirationTime < theCur.first) {
                break;
            }
            mKeys.erase(theCur.second);
            mKeysExpirationQueue.pop_front();
        }
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

    bool
CryptoKeys::Find(
    CryptoKeys::KeyId inKeyId,
    CryptoKeys::Key&  outKey) const
{
    return mImpl.Find(inKeyId, outKey);
}

    int
CryptoKeys::Read(
    istream& inStream)
{
    return mImpl.Read(inStream);
}

    int
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

