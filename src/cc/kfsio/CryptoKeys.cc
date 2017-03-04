//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/09/25
// Author: Mike Ovsiannikov
//
// Copyright 2013,2016 Quantcast Corporation. All rights reserved.
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
    int         inStrLen,
    bool        inUrlSafeFmtFlag)
{
    if (! inStrPtr || inStrLen <= 0) {
        return false;
    }
    const int theLen = Base64::GetMaxDecodedLength(inStrLen);
    return (0 < theLen && theLen <= kLength &&
        Base64::Decode(inStrPtr, inStrLen, mKey, inUrlSafeFmtFlag) == kLength);
}

    int
CryptoKeys::Key::ToString(
    char* inStrPtr,
    int   inMaxStrLen,
    bool  inUrlSafeFmtFlag) const
{
    if (inMaxStrLen < Base64::GetEncodedMaxBufSize(kLength)) {
        return -EINVAL;
    }
    return Base64::Encode(mKey, kLength, inStrPtr, inUrlSafeFmtFlag);
}

    ostream&
CryptoKeys::Key::Display(
    ostream& inStream,
    bool     inUrlSafeFmtFlag) const
{
    const int theBufLen = Base64::GetEncodedMaxBufSize(kLength);
    char      theBuf[theBufLen];
    const int theLen = Base64::Encode(mKey, kLength, theBuf, inUrlSafeFmtFlag);
    QCRTASSERT(0 < theLen && theLen <= theBufLen);
    return inStream.write(theBuf, theLen);
}

class CryptoKeys::Impl :
    public ITimeout,
    public QCRunnable
{
private:
    class MutexLocker : public QCStMutexLocker
    {
    public:
        MutexLocker(
            const QCMutex& inMutex)
            : QCStMutexLocker(const_cast<QCMutex&>(inMutex))
            {}
        MutexLocker(
            QCMutex& inMutex)
            : QCStMutexLocker(inMutex)
            {}
    private:
        MutexLocker(
            const MutexLocker& inLocker);
        MutexLocker& operator=(
            const MutexLocker& inLocker);
    };
public:
    typedef CryptoKeys::Key   Key;
    typedef CryptoKeys::KeyId KeyId;

    Impl(
        NetManager& inNetManager,
        KeyStore*   inKeyStorePtr)
        : ITimeout(),
          QCRunnable(),
          mNetManager(inNetManager),
          mMutex(),
          mKeyStorePtr(inKeyStorePtr),
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
          mStartedFlag(false),
          mFileName(),
          mFStream()
        {}
    int Start()
    {
        MutexLocker theLocker(mMutex);
        if (mStartedFlag) {
            return -EINVAL;
        }
        int theStatus = 0;
        if (! mRunFlag && ! mKeyStorePtr && ! mFileName.empty()) {
            theStatus = Write(0, 0, "\n");
            if (0 < theStatus) {
                theStatus = 0;
            } else if (theStatus < 0) {
                KFS_LOG_STREAM_ERROR <<
                    "failed to write keys " << mFileName <<
                     ": "                   << QCUtils::SysError(-theStatus) <<
                KFS_LOG_EOM;
            }
            // Start thread unconditionally, to handle the case when the
            // caller chooses to ignore the initial parameter load error.
            mRunFlag = true;
            const int kStackSize = 64 << 10;
            mThread.Start(this, kStackSize, "KeysWriteThread");
        }
        mStartedFlag = true;
        mNetManager.RegisterTimeoutHandler(this);
        if (! mCurrentKeyValidFlag && ! mKeyStorePtr) {
            if (! GenKey() && 0 == theStatus) {
                theStatus = -ENXIO;
            }
        }
        return theStatus;
    }
    virtual ~Impl()
        { Impl::Stop(); }
    void Stop()
    {
        MutexLocker theLocker(mMutex);
        if (mRunFlag) {
            mRunFlag = false;
            mCond.Notify();
            theLocker.Unlock();
            mThread.Join();
        } else {
            theLocker.Unlock();
        }
        if (mStartedFlag) {
            mNetManager.UnRegisterTimeoutHandler(this);
        }
    }
    int SetParameters(
        const char*       inNamesPrefixPtr,
        const Properties& inParameters,
        string&           outErrMsg)
    {
        MutexLocker theLocker(mMutex);
        if (mError) {
            mError = 0;
            if (GenKey(mCurrentKey) && GenKeyId(mCurrentKeyId)) {
                mCurrentKeyTime = mNetManager.Now();
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
        int       theKeyChangePeriod = mKeyChangePeriod;
        int       theStatus          = 0;
        const int k10Min             = 600;
        if (theKeyValidTime < k10Min) {
            outErrMsg = theParamName.GetPtr();
            outErrMsg += ": invalid: less than 600 seconds";
            theStatus = -EINVAL;
        } else {
            theKeyChangePeriod = inParameters.getValue(
                theParamName.Truncate(thePrefLen).Append(
                "keyChangePeriodSec"), mKeyChangePeriod);
            if (theKeyValidTime < 3 * theKeyChangePeriod / 2) {
                outErrMsg = theParamName.GetPtr();
                outErrMsg += ": invalid: greater 2/3 of keyValidTimeSec";
                theStatus = -EINVAL;
            } else {
                if (theKeyChangePeriod < k10Min / 2 ||
                        (int64_t(10) << 10) * theKeyChangePeriod <
                            theKeyValidTime) {
                    outErrMsg = theParamName.GetPtr();
                    outErrMsg += ": invalid: less than 300 sec"
                        " or will require storing more than 10240 keys";
                    theStatus = -EINVAL;
                }
            }
        }
        if (0 == theStatus) {
            const bool theExpireKeysFlag = theKeyValidTime < mKeyValidTime;
            mKeyValidTime = theKeyValidTime;
            if (mKeyChangePeriod != theKeyChangePeriod) {
                mKeyChangePeriod = theKeyChangePeriod;
                mNextKeyGenTime  = min(mNextKeyGenTime,
                    mNetManager.Now() + mKeyChangePeriod);
            }
            if (theExpireKeysFlag && ! mKeyStorePtr) {
                ExpireKeys(mNetManager.Now() - mKeyValidTime);
            }
            UpdateNextTimerRunTime();
        }
        mFileName = inParameters.getValue(
            theParamName.Truncate(thePrefLen).Append(
            "keysFileName"), mFileName);
        if (mRunFlag || mStartedFlag || mKeyStorePtr) {
            return theStatus;
        }
        string theErrMsg;
        const int theErr = LoadKeysFile(theErrMsg);
        if (0 != theErr && -ENOENT != theErr) {
            if (0 == theStatus) {
                theStatus = theErr;
            }
            if (! outErrMsg.empty()) {
                outErrMsg += " ";
            }
            outErrMsg += theErrMsg;
        }
        if (0 == theStatus && ! mCurrentKeyValidFlag) {
            GenKey();
        }
        return theStatus;
    }
    int LoadKeysFile(
        string& outErrMsg)
    {
        if (mRunFlag || mStartedFlag) {
            outErrMsg = "file load is not supported after start";
            return -EINVAL;
        }
        int theStatus = 0;
        if (mFileName.empty()) {
            return theStatus;
        }
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
            if (0 < theStatus) {
                theStatus = -theStatus;
            } else if (theStatus == 0) {
                theStatus = -EFAULT;
            }
            outErrMsg = mFileName + ": " + QCUtils::SysError(-theStatus);
        }
        if (theStatus == 0) {
            theStatus = mError;
            if (theStatus != 0) {
                outErrMsg = "failed to create key: " +
                    QCUtils::SysError(-theStatus);
            }
        }
        return theStatus;
    }
    bool GetCurrentKeyId(
        KeyId& outKeyId) const
    {
        MutexLocker theLocker(mMutex);
        if (mCurrentKeyValidFlag) {
            outKeyId = mCurrentKeyId;
        }
        return mCurrentKeyValidFlag;
    }
    bool GetCurrentKey(
         KeyId& outKeyId,
         Key&   outKey) const
    {
        MutexLocker theLocker(mMutex);
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
        MutexLocker theLocker(mMutex);
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
        MutexLocker theLocker(mMutex);
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
        MutexLocker theLocker(mMutex);
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
        if (mKeyStorePtr) {
            MutexLocker theLocker(mMutex);
            for (KeysExpirationQueue::const_iterator theIt =
                        mKeysExpirationQueue.begin();
                    theIt != mKeysExpirationQueue.end();
                    ++theIt) {
                mKeyStorePtr->WriteKey(
                    inStreamPtr,
                    theIt->second->first,
                    theIt->second->second,
                    theIt->first
                );
            }
            if (mCurrentKeyValidFlag) {
                mKeyStorePtr->WriteKey(
                    inStreamPtr,
                    mCurrentKeyId,
                    mCurrentKey,
                    mCurrentKeyTime
                );
            }
            return 0;
        }
        OutKeysTmp theKeys;
        int64_t    theFirstKeyTime;
        int        theKeysTimeInterval;
        string     theFileName;
        {
            MutexLocker theLocker(mMutex);
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
        string   theTmpFileName;
        fstream& theFStream(const_cast<fstream&>(mFStream)); //mutable.
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
    bool Add(
        KeyId      inKeyId,
        const Key& inKey,
        time_t     inKeyTime)
    {
        MutexLocker theLocker(mMutex);
        if (mPendingCurrentKeyFlag && inKeyId == mPendingCurrentKeyId) {
            mPendingCurrentKeyFlag = false;
            mWritingFlag           = false;
        }
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
        mCurrentKeyValidFlag = true;
        mCurrentKeyId        = inKeyId;
        mCurrentKey          = inKey;
        mCurrentKeyTime      = inKeyTime;
        mNextKeyGenTime      = inKeyTime + mKeyChangePeriod;
        UpdateNextTimerRunTime();
        return true;
    }
    bool Remove(
        KeyId inKeyId,
        bool  inPendingFlag)
    {
        MutexLocker theLocker(mMutex);
        if (inPendingFlag) {
            if (mPendingCurrentKeyFlag && inKeyId == mPendingCurrentKeyId) {
                mPendingCurrentKeyFlag = false;
                mWritingFlag           = false;
                UpdateNextTimerRunTime();
                return true;
            }
            return false;
        }
        if (mCurrentKeyId == inKeyId) {
            mCurrentKeyValidFlag = false;
            UpdateNextTimerRunTime();
            return true;
        }
        for (KeysExpirationQueue::iterator
                theIt = mKeysExpirationQueue.begin();
                mKeysExpirationQueue.end() != theIt;
                ++theIt) {
            if (inKeyId == theIt->second->first) {
                mKeys.erase(theIt->second);
                if (theIt == mKeysExpirationQueue.begin()) {
                    mKeysExpirationQueue.pop_front();
                } else {
                    mKeysExpirationQueue.erase(theIt);
                }
                UpdateNextTimerRunTime();
                return true;
            }
        }
        return false;
    }
    void Clear()
    {
        MutexLocker theLocker(mMutex);
        mKeys.clear();
        mKeysExpirationQueue.clear();
        mCurrentKeyValidFlag = false;
        mNextKeyGenTime = mNetManager.Now();
        UpdateNextTimerRunTime();
    }
    virtual void Timeout()
    {
        if (mKeyStorePtr && ! mKeyStorePtr->IsActive()) {
            return;
        }
        const time_t theTimeNow = mNetManager.Now();
        if (theTimeNow < mNextTimerRunTime && ! mPendingCurrentKeyFlag) {
            return;
        }
        MutexLocker theLocker(mMutex);
        if (! mPendingCurrentKeyFlag) {
            ExpireKeys(theTimeNow - mKeyValidTime);
            if (theTimeNow < mNextKeyGenTime) {
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
            if (mKeyStorePtr) {
                mWriteFlag = false;
                if ((mWritingFlag = mKeyStorePtr->NewKey(
                        mPendingCurrentKeyId,
                        mPendingCurrentKey,
                        theTimeNow))) {
                    return;
                }
            } else if (! mFileName.empty() && mRunFlag) {
                mWriteFlag   = true;
                mWritingFlag = true;
                mCond.Notify();
                return;
            }
            mWriteFlag = false;
        }
        if (mWritingFlag || mKeyStorePtr) {
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
    bool EnsureHasCurrentKey()
    {
        MutexLocker theLocker(mMutex);
        return (mCurrentKeyValidFlag || GenKey());
    }
    virtual void Run()
    {
        MutexLocker theLocker(mMutex);
        for (; ;) {
            while (mRunFlag && ! mWriteFlag) {
                mCond.Wait(mMutex);
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
    QCMutex             mMutex;
    KeyStore* const     mKeyStorePtr;
    Keys                mKeys;
    KeysExpirationQueue mKeysExpirationQueue;
    KeyId               mCurrentKeyId;
    Key                 mCurrentKey;
    KeyId               mPendingCurrentKeyId;
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
    bool                mStartedFlag;
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
        KeyId& outKeyId)
    {
        const bool theRet = RAND_bytes(
            reinterpret_cast<unsigned char*>(&outKeyId),
            (int)sizeof(outKeyId)) > 0;
        if (! theRet) {
            mError = ERR_get_error();
            KFS_LOG_STREAM_ERROR << "RAND_bytes failure: " <<
                OpenSslError(mError) <<
            KFS_LOG_EOM;
        }
        return theRet;
    }
    void ExpireKeys(
        time_t inExpirationTime)
    {
        QCASSERT(mMutex.IsOwned());
        int theCnt = 0;
        while (! mKeysExpirationQueue.empty()) {
            const KeysExpirationQueue::value_type& theCur =
                mKeysExpirationQueue.front();
            if (inExpirationTime < theCur.first) {
                break;
            }
            if (mKeyStorePtr && mKeyStorePtr->Expired(theCur.second->first)) {
                break;
            }
            theCnt++;
            mKeys.erase(theCur.second);
            mKeysExpirationQueue.pop_front();
        }
        if (0 < theCnt) {
            UpdateNextTimerRunTime();
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
    bool GenKey()
    {
        mCurrentKeyValidFlag = GenKey(mCurrentKey) && GenKeyId(mCurrentKeyId);
        if (mCurrentKeyValidFlag) {
            mCurrentKeyTime = mNetManager.Now();
            mNextKeyGenTime = mCurrentKeyTime + mKeyChangePeriod;
            UpdateNextTimerRunTime();
        }
        return mCurrentKeyValidFlag;
    }
private:
    Impl(
        const Impl& inImpl);
    Impl& operator=(
        const Impl& inImpl);
};

CryptoKeys::CryptoKeys(
    NetManager&           inNetManager,
    CryptoKeys::KeyStore* inKeyStorePtr)
    : mImpl(*(new Impl(inNetManager, inKeyStorePtr)))
{}

CryptoKeys::~CryptoKeys()
{
    delete &mImpl;
}

    int
CryptoKeys::Start()
{
    return mImpl.Start();
}

    void
CryptoKeys::Stop()
{
    mImpl.Stop();
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
CryptoKeys::Add(
    CryptoKeys::KeyId      inKeyId,
    const CryptoKeys::Key& inKey,
    int64_t                inKeyTime)
{
    return mImpl.Add(inKeyId, inKey, inKeyTime);
}

    void
CryptoKeys::Clear()
{
    mImpl.Clear();
}

    bool
CryptoKeys::Remove(
    CryptoKeys::KeyId inKeyId,
    bool              inPendingFlag)
{
    return mImpl.Remove(inKeyId, inPendingFlag);
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

    int
CryptoKeys::LoadKeysFile(
    string& outErrMsg)
{
    return mImpl.LoadKeysFile(outErrMsg);
}

    bool
CryptoKeys::EnsureHasCurrentKey()
{
    return mImpl.EnsureHasCurrentKey();
}

    /* static */ bool
CryptoKeys::PseudoRand(
    void*  inPtr,
    size_t inLen)
{
    const bool theRet = RAND_bytes(
        reinterpret_cast<unsigned char*>(inPtr), (int)inLen) > 0;
    if (! theRet) {
        KFS_LOG_STREAM_ERROR << "RAND_bytes failure: " <<
            OpenSslError(ERR_get_error()) <<
        KFS_LOG_EOM;
    }
    return theRet;
}

    /* static */ bool
CryptoKeys::Rand(
    void*  inPtr,
    size_t inLen)
{
    const bool theRet = RAND_bytes(
        reinterpret_cast<unsigned char*>(inPtr), (int)inLen) > 0;
    if (! theRet) {
        KFS_LOG_STREAM_ERROR << "RAND_bytes failure: " <<
            OpenSslError(ERR_get_error()) <<
        KFS_LOG_EOM;
    }
    return theRet;
}

} // namespace KFS

