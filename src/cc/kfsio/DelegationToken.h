//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/7
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

#ifndef KFSIO_DELEGATION_TOKEN_H
#define KFSIO_DELEGATION_TOKEN_H

#include "common/kfstypes.h"
#include "CryptoKeys.h"

#include <stddef.h>

#include <string>
#include <istream>
#include <ostream>

namespace KFS
{
using std::string;
using std::istream;
using std::ostream;

class IOBufferWriter;

class DelegationToken
{
public:
    typedef uint32_t TokenSeq;
    class ShowToken
    {
    public:
        ShowToken(
            const DelegationToken& inToken)
            : mToken(inToken)
            {}
        ostream& Display(
            ostream& inStream) const
            { return mToken.ShowSelf(inStream); }
    private:
        const DelegationToken& mToken;
    };
    class Subject
    {
    public:
        virtual int Get(
            const DelegationToken& inToken,
            const char*&           outPtr) = 0;
    protected:
        virtual ~Subject()
            {}
    };

    enum { kSignatureLength = 20 };
    enum {
        kAllowDelegationFlag = 0x1,
        kChunkServerFlag     = 0x2
    };

    DelegationToken()
        : mUid(kKfsUserNone),
          mSeq(0),
          mKeyId(-1),
          mIssuedTimeAndFlags(0),
          mValidForSec(0)
        { mSignature[0] = 0; }
    DelegationToken(
        kfsUid_t    inUid,
        TokenSeq    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssueTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0);
    ~DelegationToken()
        {}
    bool Init(
        kfsUid_t    inUid,
        TokenSeq    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssuedTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0);
    void Clear()
        { *this = DelegationToken(); }
    string ToString();
    bool FromString(
        const string& inString,
        const char*   inKeyPtr,
        int           inKeyLen,
        Subject*      inSubjectPtr = 0);
    bool FromString(
        const char* inPtr,
        int         inLen,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0);
    int Process(
        const char*       inPtr,
        int               inLen,
        int64_t           inTimeNowSec,
        const CryptoKeys& inKeys,
        char*             inSessionKeyPtr,
        int               inMaxSessionKeyLength,
        string*           outErrMsgPtr,
        Subject*          inSubjectPtr = 0);
    ostream& Display(
        ostream& inStream) const;
    istream& Parse(
        istream&    inStream,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0);
    kfsUid_t GetUid() const
        { return mUid; }
    TokenSeq GetSeq() const
        { return mSeq; }
    kfsKeyId_t GetKeyId() const
        { return mKeyId; }
    int64_t GetIssuedTimeAndFlags() const
        { return mIssuedTimeAndFlags; }
    int64_t GetIssuedTime() const
        { return (mIssuedTimeAndFlags >> kIssuedTimeShift); }
    uint16_t GetFlags() const
        { return (uint16_t)mIssuedTimeAndFlags; }
    uint32_t GetValidForSec() const
        { return mValidForSec; }
    bool Validate(
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0) const;
    string GetSessionKey(
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0) const;
    ShowToken Show() const
        { return ShowToken(*this); }
    ostream& ShowSelf(
        ostream& inStream) const;
    string CalcSessionKey(
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr = 0) const;
    static bool WriteToken(
        IOBufferWriter& inWriter,
        kfsUid_t        inUid,
        TokenSeq        inSeq,
        kfsKeyId_t      inKeyId,
        int64_t         inIssuedTime,
        uint16_t        inFlags,
        uint32_t        inValidForSec,
        const char*     inKeyPtr,
        int             inKeyLen,
        Subject*        inSubjectPtr          = 0,
        bool            inWriteSessionKeyFlag = false,
        kfsKeyId_t      inSessionKeyKeyId     = kfsKeyId_t(),
        const char*     inSessionKeyKeyPtr    = 0,
        int             inSessionKeyKeyLen    = 0);
    static bool WriteToken(
        ostream&    inStream,
        kfsUid_t    inUid,
        TokenSeq    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssuedTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr          = 0,
        bool        inWriteSessionKeyFlag = false,
        kfsKeyId_t  inSessionKeyKeyId     = kfsKeyId_t(),
        const char* inSessionKeyKeyPtr    = 0,
        int         inSessionKeyKeyLen    = 0);
    template<typename T>
    static bool WriteTokenAndSessionKey(
        T&          inWriter,
        kfsUid_t    inUid,
        TokenSeq    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssuedTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr       = 0,
        kfsKeyId_t  inSessionKeyKeyId  = kfsKeyId_t(),
        const char* inSessionKeyKeyPtr = 0,
        int         inSessionKeyKeyLen = 0)
    {
        return  WriteToken(
            inWriter,
            inUid,
            inSeq,
            inKeyId,
            inIssuedTime,
            inFlags,
            inValidForSec,
            inKeyPtr,
            inKeyLen,
            inSubjectPtr,
            true,
            inSessionKeyKeyId,
            inSessionKeyKeyPtr,
            inSessionKeyKeyLen
        );
    }
    static int DecryptSessionKeyFromString(
        const CryptoKeys& inKeys,
        const char*       inStrPtr,
        int               inStrLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr);
    static int DecryptSessionKey(
        const CryptoKeys& inKeys,
        const char*       inKeyPtr,
        int               inKeyLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr);
    static int DecryptSessionKeyFromString(
        const char*       inDecryptKeyPtr,
        int               inDecryptKeyLen,
        const char*       inStrPtr,
        int               inStrLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr);
    static int DecryptSessionKey(
        const char*       inDecryptKeyPtr,
        int               inDecryptKeyLen,
        const char*       inKeyPtr,
        int               inKeyLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr);

private:
    enum { kIssuedTimeShift = 16 };
    kfsUid_t   mUid;
    TokenSeq   mSeq;
    kfsKeyId_t mKeyId;
    int64_t    mIssuedTimeAndFlags;
    uint32_t   mValidForSec;
    char       mSignature[kSignatureLength];

    template<typename T>
    static bool WriteTokenSelf(
        T&          inWriter,
        kfsUid_t    inUid,
        TokenSeq    inSeq,
        kfsKeyId_t  inKeyId,
        int64_t     inIssuedTime,
        uint16_t    inFlags,
        uint32_t    inValidForSec,
        const char* inKeyPtr,
        int         inKeyLen,
        Subject*    inSubjectPtr,
        bool        inWriteSessionKeyFlag,
        kfsKeyId_t  inSessionKeyKeyId,
        const char* inSessionKeyKeyPtr,
        int         inSessionKeyKeyLen);
    class WorkBuf;
    friend class WorkBuf;
};

inline static ostream& operator << (
    ostream&                          inStream,
    const DelegationToken::ShowToken& inShowToken)
{ return inShowToken.Display(inStream); }

inline static ostream& operator << (
    ostream&               inStream,
    const DelegationToken& inToken)
{ return inToken.Display(inStream); }

inline static istream& operator >> (
    istream&         inStream,
    DelegationToken& inToken)
{ return inToken.Parse(inStream, 0, 0); }

} // namespace KFS

#endif /* KFSIO_DELEGATION_TOKEN_H */
