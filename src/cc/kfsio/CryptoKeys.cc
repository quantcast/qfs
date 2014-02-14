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
#include "qcdio/QCThread.h"
#include "qcdio/qcstutils.h"
#include "qcdio/QCUtils.h"

#include <openssl/rand.h>
#include <openssl/err.h>

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include <istream>
#include <ostream>
#include <map>
#include <deque>
#include <utility>
#include <vector>
#include <functional>
#include <fstream>
#include <iomanip>

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
using std::vector;
using std::fstream;
using std::hex;
using std::ios_base;
using std::max;

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

    bool
CryptoKeys::Key::Parse(
    const char* inStrPtr,
    int         inStrLen)
{
    if (! inStrPtr || inStrLen <= 0) {
        return false;
    }
    const int theLen = Base64::GetMaxDecodedLength(inStrLen);
    return (0 < theLen && theLen <= kLength &&
        Base64::Decode(inStrPtr, inStrLen, mKey) == kLength);
}

    int
CryptoKeys::Key::ToString(
    char* inStrPtr,
    int   inMaxStrLen) const
{
    if (inMaxStrLen < Base64::GetEncodedMaxBufSize(kLength)) {
        return -EINVAL;
    }
    return Base64::Encode(mKey, kLength, inStrPtr);
}

    ostream&
CryptoKeys::Key::Display(
    ostream& inStream) const
{
    const int theBufLen = Base64::GetEncodedMaxBufSize(kLength);
    char      theBuf[theBufLen];
    const int theLen = Base64::Encode(mKey, kLength, theBuf);
    QCRTASSERT(0 < theLen && theLen <= theBufLen);
    return inStream.write(theBuf, theLen);
}

class CryptoKeys::Impl :
    public ITimeout,
    public QCRunnable
{
public:
    typedef CryptoKeys::Key   Key;
    typedef CryptoKeys::KeyId KeyId;

    Impl(
        NetManager& inNetManager,
        QCMutex*    inMutexPtr)
        : ITimeout(),
          QCRunnable(),
          mNetManager(inNetManager),
          mMutexPtr(inMutexPtr ? inMutexPtr : new QCMutex()),
          mOwnsMutexFlag(! inMutexPtr),
          mKeys(),
          mKeysExpirationQueue(),
          mCurrentKeyId(),
          mCurrentKey(),
          mPendingCurrentKeyId(),
          mPendingCurrentKey(),
          mCurrentKeyTime(mNetManager.Now()),
          mCurrentKeyValidFlag(false),
          mPendingCurrentKeyFlag(false),
          mKeyValidTime(4 * 60 * 60 + 2 * 60),
          mKeyChangePeriod(mKeyValidTime / 2),
          mNextKeyGenTime(mCurrentKeyTime +  mKeyChangePeriod),
          mNextTimerRunTime(mCurrentKeyTime - 3600),
          mError(0),
          mThread(),
          mCond(),
          mRunFlag(false),
          mWriteFlag(false),
          mWritingFlag(false),
          mFileName(),
          mFStream()
    {
        mNetManager.RegisterTimeoutHandler(this);
        mCurrentKeyValidFlag = GenKey(mCurrentKey) && GenKeyId(mCurrentKeyId);
        if (! mCurrentKeyValidFlag) {
            mCurrentKeyTime -=  2 * mKeyChangePeriod;
        }
    }
    virtual ~Impl()
    {
        {
            QCStMutexLocker theLocker(mMutexPtr);
            if (mRunFlag) {
                mRunFlag = false;
                mCond.Notify();
                theLocker.Unlock();
                mThread.Join();
            }
        }
        mNetManager.UnRegisterTimeoutHandler(this);
        if (mOwnsMutexFlag) {
            delete mMutexPtr;
        }
    }
    int SetParameters(
        const char*       inNamesPrefixPtr,
        const Properties& inParameters,
        string&           outErrMsg)
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mError) {
            mError = 0;
            if (GenKey(mCurrentKey) && GenKeyId(mCurrentKeyId)) {
                mCurrentKeyTime = time(0);
            }
        }
        if (mError) {
            outErrMsg = GetErrorMsg(mError);
            return -EFAULT;
        }
        Properties::String theParamName;
        if (inNamesPrefixPtr) {
            theParamName.Append(inNamesPrefixPtr);
        }
        const size_t thePrefLen      = theParamName.GetSize();
        const int    theKeyValidTime = max(
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "keyValidTimeSec"), mKeyValidTime),
            inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "minKeyValidTimeSec"), -1)
        );
        const int k10Min = 60 * 10;
        if (theKeyValidTime < k10Min) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: less or equal 10 minutes";
            return -EINVAL;
        }
        const int theKeyChangePeriod = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "keyChangePeriodSec"), mKeyChangePeriod);
        if (theKeyValidTime < 3 * theKeyChangePeriod / 2) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: greater 2/3 of keyValidTimeSec";
            return -EINVAL;
        }
        if (theKeyChangePeriod < k10Min / 2 ||
                theKeyValidTime * (int64_t(10) << 10) < theKeyChangePeriod) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: greater than keyValidTimeSec * 10240";
            return -EINVAL;
        }
        const bool theExpireKeysFlag = theKeyValidTime < mKeyValidTime;
        mKeyValidTime = theKeyValidTime;
        if (mKeyChangePeriod != theKeyChangePeriod) {
            mKeyChangePeriod = theKeyChangePeriod;
            mNextKeyGenTime  = min(mNextKeyGenTime, time(0) + mKeyChangePeriod);
        }
        if (theExpireKeysFlag) {
            ExpireKeys(time(0) - mKeyValidTime);
        }
        UpdateNextTimerRunTime();
        mFileName = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "keysFileName"), mFileName);
        int theStatus = 0;
        if (! mRunFlag) {
            if (! mFileName.empty()) {
                mFStream.close();
                mFStream.clear();
                mFStream.open(mFileName.c_str(), fstream::in | fstream::binary);
                if (mFStream) {
                    const bool kRemoveIfCurrentKeyFlag = false;
                    if (0 < (theStatus = Read(
                            mFStream, kRemoveIfCurrentKeyFlag))) {
                        theStatus = 0;
                        if (! mKeysExpirationQueue.empty()) {
                            const KeysExpirationQueue::value_type& theLast =
                                mKeysExpirationQueue.back();
                            mCurrentKeyTime      = theLast.first;
                            mCurrentKeyId        = theLast.second->first;
                            mCurrentKey          = theLast.second->second;
                            mCurrentKeyValidFlag = true;
                            mNextKeyGenTime      =
                                mCurrentKeyTime + mKeyChangePeriod;
                            mKeys.erase(theLast.second);
                            mKeysExpirationQueue.pop_back();
                            UpdateNextTimerRunTime();
                        }
                    }
                    mFStream.close();
                } else {
                    theStatus = errno;
                    if (theStatus == ENOENT) {
                        theStatus = 0;
                    } else {
                        if (0 < theStatus) {
                            theStatus = -theStatus;
                        } else if (theStatus == 0) {
                            theStatus = -EFAULT;
                        }
                        outErrMsg = mFileName + ": " +
                            QCUtils::SysError(-theStatus);
                    }
                }
            }
            Timeout();
            if (theStatus == 0) {
                theStatus = mError;
                if (theStatus != 0) {
                    outErrMsg = "failed to create key: " +
                        QCUtils::SysError(-theStatus);
                }
            }
            if (! mFileName.empty()) {
                if (theStatus == 0) {
                    theStatus = Write(0, 0, "\n");
                    if (0 < theStatus) {
                        theStatus = 0;
                    } else {
                        outErrMsg = "failed to write keys " +
                            mFileName + ": " +
                            QCUtils::SysError(-theStatus);
                    }
                }
                // Start thread unconditionally, to handle the case when the
                // caller chooses to ignore the initial parameter load error.
                mRunFlag = true;
                const int kStackSize = 64 << 10;
                mThread.Start(this, kStackSize, "KeysWriteThread");
            }
        }
        return theStatus;
    }
    bool GetCurrentKeyId(
        KeyId& outKeyId) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mCurrentKeyValidFlag) {
            outKeyId = mCurrentKeyId;
        }
        return mCurrentKeyValidFlag;
    }
    bool GetCurrentKey(
         KeyId& outKeyId,
         Key&   outKey) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mCurrentKeyValidFlag) {
            outKeyId = mCurrentKeyId;
            outKey   = mCurrentKey;
        }
        return mCurrentKeyValidFlag;
    }
    bool GetCurrentKey(
         KeyId&    outKeyId,
         Key&      outKey,
         uint32_t& outKevValidForSec) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mCurrentKeyValidFlag) {
            outKeyId          = mCurrentKeyId;
            outKey            = mCurrentKey;
            outKevValidForSec = (uint32_t)(mKeyValidTime - mKeyChangePeriod);
        }
        return mCurrentKeyValidFlag;
    }
    bool Find(
        KeyId inKeyId,
        Key&  outKey) const
    {
        QCStMutexLocker theLocker(mMutexPtr);
        if (mCurrentKeyValidFlag && inKeyId == mCurrentKeyId) {
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
    class SaveAndRestoreStreamFlags
    {
    public:
        SaveAndRestoreStreamFlags(
            ios_base& inStream)
            : mStream(inStream),
              mFlags(inStream.flags())
            {}
        ~SaveAndRestoreStreamFlags()
            { mStream.flags(mFlags); }
    private:
        ios_base&                mStream;
        const ios_base::fmtflags mFlags;
    };
    int Read(
        istream& inStream,
        bool     inRemoveIfCurrentKeyFlag = true)
    {
        int                       theKeyCount         = -1;
        int64_t                   theFirstKeyTime     = -1;
        int                       theKeysTimeInterval = -1;
        SaveAndRestoreStreamFlags theSaveRestoreFlags(inStream);
        if (! (inStream >> hex >>
                    theKeyCount >> theFirstKeyTime >> theKeysTimeInterval)
                || theKeysTimeInterval <= 0) {
            return -EINVAL;
        }
        KeyId               theKeyId;
        string              theKeyStr;
        Keys                theKeys;
        KeysExpirationQueue theExpQueue;
        Key                 theKey;
        time_t              theKeyTime        = theFirstKeyTime;
        const time_t        theTimeNow        = time(0);
        const time_t        theExpirationTime = theTimeNow - mKeyValidTime;
        for (int i = 0; i < theKeyCount;
                i++, theKeyTime += theKeysTimeInterval) {
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
            if (theKeyTime <= theExpirationTime) {
                KFS_LOG_STREAM_INFO <<
                    "ignoring expired key at index: " << i << " " << theKeyId <<
                KFS_LOG_EOM;
                continue;
            }
            if (! PutKey(theKeys, theExpQueue, theKeyId, theKeyTime, theKey)) {
                KFS_LOG_STREAM_ERROR <<
                    "duplicate key at index " << i <<
                KFS_LOG_EOM;
                return -EINVAL;
            }
        }
        QCStMutexLocker theLocker(mMutexPtr);
        mKeys.swap(theKeys);
        mKeysExpirationQueue.swap(theExpQueue);
        ExpireKeys(theExpirationTime);
        if (inRemoveIfCurrentKeyFlag &&
                mCurrentKeyValidFlag && mError == 0 &&
                0 < mKeys.erase(mCurrentKeyId)) {
            KeysExpirationQueue::iterator theIt = mKeysExpirationQueue.begin();
            while (theIt != mKeysExpirationQueue.end() &&
                        theIt->second->first != mCurrentKeyId)
                {}
            if (theIt == mKeysExpirationQueue.end()) {
                KFS_LOG_STREAM_FATAL <<
                    "invalid key expiration queue" <<
                KFS_LOG_EOM;
                MsgLogger::Stop();
                abort();
            } else {
                mKeysExpirationQueue.erase(theIt);
            }
            KFS_LOG_STREAM_INFO <<
                "ignoring current key: " << mCurrentKeyId <<
            KFS_LOG_EOM;
        }
        mPendingCurrentKeyFlag = false;
        UpdateNextTimerRunTime();
        return (int)mKeys.size();
    }
    typedef vector<pair<KeyId, Key> > OutKeysTmp;
    int Write(
        ostream*    inStreamPtr,
        const char* inDelimPtr,
        const char* inKeysDelimPtr) const
    {
        OutKeysTmp theKeys;
        int64_t    theFirstKeyTime;
        int        theKeysTimeInterval;
        string     theFileName;
        {
            QCStMutexLocker theLocker(inStreamPtr ? mMutexPtr : 0);
            theKeysTimeInterval = mKeyChangePeriod;
            theKeys.reserve(mKeysExpirationQueue.size() + 1);
            KeysExpirationQueue::const_iterator theIt =
                mKeysExpirationQueue.begin();
            if (theIt == mKeysExpirationQueue.end()) {
                theFirstKeyTime = mCurrentKeyValidFlag ?
                    mCurrentKeyTime : time(0);
            } else {
                theFirstKeyTime = theIt->first;
            }
            while (theIt != mKeysExpirationQueue.end()) {
                theKeys.push_back(*(theIt->second));
                ++theIt;
            }
            if (mCurrentKeyValidFlag) {
                theKeys.push_back(make_pair(mCurrentKeyId, mCurrentKey));
            }
            if (! inStreamPtr && mPendingCurrentKeyFlag) {
                theKeys.push_back(
                    make_pair(mPendingCurrentKeyId, mPendingCurrentKey));
            }
            if (! inStreamPtr) {
                theFileName = mFileName;
            }
        }
        QCStMutexUnlocker theUnlocker(inStreamPtr ? 0 : mMutexPtr);
        string            theTmpFileName;
        fstream&          theFStream(const_cast<fstream&>(mFStream)); //mutable.
        if (! inStreamPtr) {
            if (theFileName.empty()) {
                return -EINVAL;
            }
            theTmpFileName = theFileName + ".tmp";
            theFStream.close();
            theFStream.clear();
            theFStream.open(
                theTmpFileName.c_str(),
                fstream::out | fstream::trunc | fstream::binary
            );
            if (! theFStream) {
                const int theRet = errno;
                return (0 < theRet ? -theRet :
                    (theRet == 0 ? -EFAULT : theRet));
            }
        }
        ostream& theStream = inStreamPtr ? *inStreamPtr : theFStream;
        SaveAndRestoreStreamFlags theSaveRestoreFlags(theStream);
        const char* const theDelimPtr     = inDelimPtr ? inDelimPtr : " ";
        const char* const theKeysDelimPtr =
            inKeysDelimPtr ? inKeysDelimPtr : theDelimPtr;
        theStream << hex << theKeys.size() <<
            theDelimPtr << theFirstKeyTime <<
            theDelimPtr << theKeysTimeInterval;
        const int theBufLen = Base64::GetEncodedMaxBufSize(Key::kLength);
        char      theBuf[theBufLen];
        for (OutKeysTmp::const_iterator theIt = theKeys.begin();
                theIt != theKeys.end();
                ++theIt) {
            theStream << theKeysDelimPtr << theIt->first << theDelimPtr;
            const int theLen = theIt->second.ToString(theBuf, theBufLen);
            QCRTASSERT(0 < theLen && theLen <= (int)sizeof(theBuf) - 1);
            theStream.write(theBuf, theLen);
        }
        if (inStreamPtr) {
            return (int)theKeys.size();
        }
        if (strcmp(theKeysDelimPtr, "\n") == 0) {
            theFStream << theKeysDelimPtr;
        }
        theFStream.close();
        if (theFStream) {
            if (rename(theTmpFileName.c_str(), theFileName.c_str()) == 0) {
                return (int)theKeys.size();
            }
        }
        const int theRet = errno;
        unlink(theTmpFileName.c_str());
        return (0 < theRet ? -theRet : (theRet == 0 ? -EFAULT : theRet));
    }
    virtual void Timeout()
    {
        const time_t theTimeNow = mNetManager.Now();
        if (theTimeNow < mNextTimerRunTime && ! mPendingCurrentKeyFlag) {
            return;
        }
        QCStMutexLocker theLocker(mMutexPtr);
        if (! mPendingCurrentKeyFlag) {
            if (theTimeNow < mNextKeyGenTime) {
                ExpireKeys(theTimeNow - mKeyValidTime);
                UpdateNextTimerRunTime();
                return;
            }
            mError = 0;
            Key theKey;
            if (! GenKey(theKey)) {
                return;
            }
            KeyId theKeyId;
            for (int i = 0; ;) {
                if (! GenKeyId(theKeyId)) {
                    return;
                }
                if (theKeyId == mCurrentKeyId ||
                        mKeys.find(theKeyId) != mKeys.end()) {
                    KFS_LOG_STREAM(i <= 0 ?
                        MsgLogger::kLogLevelCRIT :
                        MsgLogger::kLogLevelALERT) <<
                        "generated duplicate key id: " << theKeyId <<
                        " attempt: " << i <<
                    KFS_LOG_EOM;
                    if (8 <= ++i) {
                        return;
                    }
                } else {
                    break;
                }
            }
            if (mError != 0) {
                return;
            }
            mPendingCurrentKeyId   = theKeyId;
            mPendingCurrentKey     = theKey;
            mPendingCurrentKeyFlag = true;
            if (! mFileName.empty() && mRunFlag) {
                mWriteFlag   = true;
                mWritingFlag = true;
                mCond.Notify();
                return;
            }
            mWritingFlag = false;
            mWriteFlag   = false;
        }
        if (mWritingFlag) {
            return; // Wait until the keys are updated / written.
        }
        mPendingCurrentKeyFlag = false;
        if (mCurrentKeyValidFlag && ! PutKey(
                mKeys,
                mKeysExpirationQueue,
                mCurrentKeyId,
                mCurrentKeyTime,
                mCurrentKey)) {
            KFS_LOG_STREAM_FATAL <<
                "duplicate current key id: " << mPendingCurrentKeyId <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
        }
        ExpireKeys(theTimeNow - mKeyValidTime);
        mCurrentKeyValidFlag = true;
        mCurrentKeyId        = mPendingCurrentKeyId;
        mCurrentKey          = mPendingCurrentKey;
        mCurrentKeyTime      = theTimeNow;
        mNextKeyGenTime      = theTimeNow + mKeyChangePeriod;
        UpdateNextTimerRunTime();
    }
    virtual void Run()
    {
        QCStMutexLocker theLocker(mMutexPtr);
        for (; ;) {
            while (mRunFlag && ! mWriteFlag) {
                mCond.Wait(*mMutexPtr);
            }
            if (! mRunFlag) {
                break;
            }
            mWritingFlag = true;
            mWriteFlag   = false;
            if (! mFileName.empty()) {
                const string theFileName = mFileName;
                const int    theStatus   = Write(0, " ", "\n");
                if (theStatus < 0) {
                    KFS_LOG_STREAM_ERROR << "failed to write keys into"
                        " " << theFileName <<
                        ": " << QCUtils::SysError(-theStatus) <<
                    KFS_LOG_EOM;
                }
            }
            if (! mWriteFlag) {
                mWritingFlag = false;
                mNetManager.Wakeup();
            }
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
    typedef deque<
        pair<time_t, Keys::iterator>
    > KeysExpirationQueue;
    NetManager&         mNetManager;
    QCMutex* const      mMutexPtr;
    bool const          mOwnsMutexFlag;
    Keys                mKeys;
    KeysExpirationQueue mKeysExpirationQueue;
    kfsKeyId_t          mCurrentKeyId;
    Key                 mCurrentKey;
    kfsKeyId_t          mPendingCurrentKeyId;
    Key                 mPendingCurrentKey;
    time_t              mCurrentKeyTime;
    bool                mCurrentKeyValidFlag;
    bool                mPendingCurrentKeyFlag;
    int                 mKeyValidTime;
    int                 mKeyChangePeriod;
    time_t              mNextKeyGenTime;
    time_t              mNextTimerRunTime;
    unsigned long       mError;
    QCThread            mThread;
    QCCondVar           mCond;
    bool                mRunFlag;
    bool                mWriteFlag;
    bool                mWritingFlag;
    string              mFileName;
    fstream             mFStream;

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
    static bool PutKey(
        Keys&                inKeys,
        KeysExpirationQueue& inExpQueue,
        KeyId                inKeyId,
        time_t               inKeyTime,
        const Key&           inKey)
    {
        pair<Keys::iterator, bool> const theRes = inKeys.insert(
            make_pair(inKeyId, inKey));
        if (! theRes.second) {
            return false;
        }
        // Since time(0) isn't monotonic, do not enforce strict ordering by
        // time here.
        inExpQueue.push_back(make_pair(inKeyTime, theRes.first));
        return true;
    }
    void UpdateNextTimerRunTime()
    {
        if (mKeysExpirationQueue.empty()) {
            mNextTimerRunTime = mNextKeyGenTime;
        } else {
            mNextTimerRunTime = min(
                mNextKeyGenTime,
                mKeysExpirationQueue.front().first + mKeyValidTime
            );
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
    const char*       inNamesPrefixPtr,
    const Properties& inParameters,
    string&           outErrMsg)
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
    ostream&    inStream,
    const char* inDelimPtr) const
{
    return mImpl.Write(&inStream, inDelimPtr, 0);
}

    bool
CryptoKeys::GetCurrentKeyId(
    CryptoKeys::KeyId& outKeyId) const
{
    return mImpl.GetCurrentKeyId(outKeyId);
}

    bool
CryptoKeys::GetCurrentKey(
    CryptoKeys::KeyId& outKeyId,
    CryptoKeys::Key&   outKey) const
{
    return mImpl.GetCurrentKey(outKeyId, outKey);
}

    bool
CryptoKeys::GetCurrentKey(
    CryptoKeys::KeyId& outKeyId,
    CryptoKeys::Key&   outKey,
    uint32_t&          outKeyValidForSec) const
{
    return mImpl.GetCurrentKey(outKeyId, outKey, outKeyValidForSec);
}

    /* static */ bool
CryptoKeys::PseudoRand(
    void*  inPtr,
    size_t inLen)
{
    const bool theRet = RAND_pseudo_bytes(
        reinterpret_cast<unsigned char*>(inPtr), (int)inLen) > 0;
    if (! theRet) {
        KFS_LOG_STREAM_ERROR << "RAND_bytes failure: " <<
            OpenSslError(ERR_get_error()) <<
        KFS_LOG_EOM;
    }
    return theRet;
}

} // namespace KFS

