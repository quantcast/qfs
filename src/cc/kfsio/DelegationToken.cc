//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/9/8
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

#include "DelegationToken.h"
#include "Base64.h"
#include "CryptoKeys.h"
#include "IOBufferWriter.h"

#include "common/MsgLogger.h"
#include "qcdio/qcdebug.h"
#include "qcdio/QCUtils.h"

#include <openssl/hmac.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <string.h>

#include <ostream>
#include <istream>
#include <iomanip>
#include <string>
#include <algorithm>

namespace KFS
{
using std::ostream;
using std::istream;
using std::hex;
using std::noshowbase;
using std::setfill;
using std::setw;
using std::min;

namespace {

class EvpError
{
public:
    typedef unsigned long Error;
    EvpError(
        Error inErr)
        : mError(inErr)
        {}
    EvpError()
        : mError(ERR_get_error())
        {}
    ostream& Display(
        ostream& inStream) const
    {
        const int kBufSize = 127;
        char      theBuf[kBufSize + 1];
        theBuf[0] = 0;
        theBuf[kBufSize] = 0;
        ERR_error_string_n(mError, theBuf, kBufSize);
        return (inStream << theBuf);
    }
private:
    Error mError;
};

    void
EvpErrorStr(
    const char*   inPrefixPtr,
    string*       outErrMsgPtr,
    unsigned long inError)
{
    if (! outErrMsgPtr) {
        return;
    }
    const int kBufSize = 127;
    char      theBuf[kBufSize + 1];
    theBuf[0] = 0;
    theBuf[kBufSize] = 0;
    ERR_error_string_n(inError, theBuf, kBufSize);
    if (inPrefixPtr) {
        *outErrMsgPtr += inPrefixPtr;
    }
    *outErrMsgPtr += theBuf;
}

    void
EvpErrorStr(
    const char* inPrefixPtr,
    string*     outErrMsgPtr)
{
    return EvpErrorStr(inPrefixPtr, outErrMsgPtr, ERR_get_error());
}

static inline ostream& operator << (
    ostream&        inStream,
    const EvpError& inError)
{ return inError.Display(inStream); }

};

class DelegationToken::WorkBuf
{
public:
    WorkBuf()
        {}
    bool SerializeAndSign(
        const DelegationToken& inToken,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen,
        char*                  inSignBufPtr)
    {
        Serialize(inToken);
        return Sign(inKeyPtr, inKeyLen, inSubjectPtr, inSubjectLen,
            inSignBufPtr);
    }
    bool Sign(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inSubjectPtr,
        int         inSubjectLen,
        char*       inSignBufPtr,
        string*     inErrMsgPtr = 0)
    {
        HMAC_CTX theCtx;
        HMAC_CTX_init(&theCtx);
        unsigned int theLen = 0;
#if OPENSSL_VERSION_NUMBER < 0x1000000fL
        const bool theRetFlag = true;
        HMAC_Init_ex(&theCtx, inKeyPtr, inKeyLen, EVP_sha1(), 0);
        if (0 < inSubjectLen) {
            HMAC_Update(
                &theCtx,
                reinterpret_cast<const unsigned char*>(inSubjectPtr),
                inSubjectLen
            );
        }
        HMAC_Update(
            &theCtx,
            reinterpret_cast<const unsigned char*>(mBuffer),
            kTokenFiledsSize
        );
        HMAC_Final(
            &theCtx,
            reinterpret_cast<unsigned char*>(inSignBufPtr),
            &theLen
        );
#else
        const bool theRetFlag =
            HMAC_Init_ex(&theCtx, inKeyPtr, inKeyLen, EVP_sha1(), 0) &&
            (inSubjectLen <= 0 ||
                HMAC_Update(
                    &theCtx,
                    reinterpret_cast<const unsigned char*>(inSubjectPtr),
                    inSubjectLen
                )) &&
            HMAC_Update(
                &theCtx,
                reinterpret_cast<const unsigned char*>(mBuffer),
                kTokenFiledsSize
            ) &&
            HMAC_Final(
                &theCtx,
                reinterpret_cast<unsigned char*>(inSignBufPtr),
                &theLen
            );
#endif
        if (! theRetFlag) {
            if (inErrMsgPtr) {
                EvpErrorStr("HMAC failure: ", inErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "HMAC failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
        }
        QCRTASSERT(! theRetFlag || theLen == kSignatureLength);
        HMAC_CTX_cleanup(&theCtx);
        return theRetFlag;
    }
    void Serialize(
        const DelegationToken& inToken)
    {
        char* thePtr = mBuffer;
        Write(thePtr, inToken.GetUid());
        Write(thePtr, inToken.GetSeq());
        Write(thePtr, inToken.GetKeyId());
        Write(thePtr, inToken.GetIssuedTimeAndFlags());
        Write(thePtr, inToken.GetValidForSec());
        QCASSERT(mBuffer + kTokenFiledsSize == thePtr);
    }
    bool FromBase64(
        DelegationToken& inToken,
        const char*      inPtr,
        int              inLen)
    {
        const int   theLen = DecodeBase64(inPtr, inLen, mBuffer, kTokenSize);
        if (theLen != kTokenSize) {
            return false;
        }
        const char* thePtr = mBuffer;
        Read(thePtr, inToken.mUid);
        Read(thePtr, inToken.mSeq);
        Read(thePtr, inToken.mKeyId);
        Read(thePtr, inToken.mIssuedTimeAndFlags);
        Read(thePtr, inToken.mValidForSec);
        memcpy(inToken.mSignature, thePtr, kSignatureLength);
        return true;
    }
    int ToBase64(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        char*                  inBufPtr)
    {
        Serialize(inToken);
        memcpy(mBuffer + kTokenFiledsSize, inSignaturePtr, kSignatureLength);
        return Base64::Encode(mBuffer, kTokenSize, inBufPtr);
    }
    string ToString(
        const DelegationToken& inToken,
        const char*            inSignaturePtr)
    {
        char theBuf[Base64::GetEncodedMaxBufSize(kTokenSize)];
        const int theLen = ToBase64(inToken, inSignaturePtr, theBuf);
        QCRTASSERT(0 < theLen);
        if (theLen <= 0) {
            return string();
        }
        return string(theBuf, theLen);
    }
    ostream& Display(
        ostream&               inStream,
        const DelegationToken& inToken,
        const char*            inSignaturePtr)
    {
        char theBuf[Base64::GetEncodedMaxBufSize(kTokenSize)];
        const int theLen = ToBase64(inToken, inSignaturePtr, theBuf);
        QCRTASSERT(0 < theLen);
        if (theLen <= 0) {
            return inStream;
        }
        return inStream.write(theBuf, theLen);
    }
    template<typename T>
    bool Write(
        T&                     inWriter,
        const DelegationToken& inToken,
        const char*            inSignaturePtr)
    {
        char theBuf[Base64::GetEncodedMaxBufSize(kTokenSize)];
        const int theLen = ToBase64(inToken, inSignaturePtr, theBuf);
        QCRTASSERT(0 < theLen);
        if (theLen <= 0) {
            return false;
        }
        inWriter.Write(theBuf, theLen);
        return true;
    }
    int MakeSessionKey(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen,
        char*                  inKeyBufferPtr,
        int                    inMaxKeyLen,
        string*                outKeyPtr,
        string*                outErrMsgPtr)
    {
        Serialize(inToken);
        memcpy(mBuffer + kTokenFiledsSize, inSignaturePtr, kSignatureLength);
        return MakeSessionKey(
            inKeyPtr,
            inKeyLen,
            inSubjectPtr,
            inSubjectLen,
            inKeyBufferPtr,
            inMaxKeyLen,
            outKeyPtr,
            outErrMsgPtr
        );
    }
    int MakeSessionKey(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inSubjectPtr,
        int         inSubjectLen,
        char*       inKeyBufferPtr,
        int         inMaxKeyLen,
        string*     outKeyPtr,
        string*     outErrMsgPtr)
    {
        IOBufferWriter* const theWriterPtr = 0;
        return MakeSessionKey(
            inKeyPtr,
            inKeyLen,
            inSubjectPtr,
            inSubjectLen,
            kfsKeyId_t(),
            0,
            0,
            inKeyBufferPtr,
            inMaxKeyLen,
            outKeyPtr,
            outErrMsgPtr,
            theWriterPtr
        );
    }
    template<typename T>
    int MakeSessionKey(
        const char*     inKeyPtr,
        int             inKeyLen,
        const char*     inSubjectPtr,
        int             inSubjectLen,
        kfsKeyId_t      inSessionKeyKeyId,
        const char*     inSessionKeyKeyPtr,
        int             inSessionKeyKeyLen,
        char*           inKeyBufferPtr,
        int             inMaxKeyLen,
        string*         outKeyPtr,
        string*         outErrMsgPtr,
        T*              inWriterPtr)
    {
        if (! inKeyPtr || inKeyLen <= 0) {
            KFS_LOG_STREAM_ERROR <<
                "invalid key: " << (void*)inKeyPtr << " len: " << inKeyLen <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        if (0 < inSubjectLen && ! inSubjectPtr) {
            KFS_LOG_STREAM_ERROR <<
                "invalid subject: " << (void*)inSubjectPtr <<
                " len: " << inSubjectLen <<
            KFS_LOG_EOM;
            return -EINVAL;
        }
        EVP_MD_CTX theCtx;
        EVP_MD_CTX_init(&theCtx);
        if (! EVP_DigestInit_ex(&theCtx, EVP_sha384(), 0)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestInit_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestInit_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            return -EFAULT;
        }
        if ( (0 < inSubjectLen &&
                ! EVP_DigestUpdate(&theCtx, inSubjectPtr, inSubjectLen)) ||
                ! EVP_DigestUpdate(&theCtx, mBuffer, kTokenSize) ||
                ! EVP_DigestUpdate(&theCtx, inKeyPtr, inKeyLen)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestUpdate failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestUpdate failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_MD_CTX_cleanup(&theCtx);
            return -EFAULT;
        }
        char          theBuf[kKeyBufSize];
        unsigned char theMd[EVP_MAX_MD_SIZE];
        unsigned int  theLen   = 0;
        if (! EVP_DigestFinal_ex(&theCtx, theMd, &theLen) || theLen <= 0) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_DigestFinal_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_DigestFinal_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_MD_CTX_cleanup(&theCtx);
            return -EFAULT;
        }
        QCRTASSERT(theLen <= EVP_MAX_MD_SIZE);
        EVP_MD_CTX_cleanup(&theCtx);
        if (theLen <= 0) {
            return theLen;
        }
        const char* theKeyPtr;
        if (0 < inSessionKeyKeyLen) {
            char* thePtr = theBuf;
            Write(thePtr, inSessionKeyKeyId);
            int theRet = MakeIv(thePtr, outErrMsgPtr);
            if (theRet < 0) {
                return theRet;
            }
            const bool kEncryptFlag = true;
            theRet = Crypt(
                inSessionKeyKeyPtr,
                inSessionKeyKeyLen,
                thePtr,
                reinterpret_cast<const char*>(theMd),
                (int)theLen,
                &theBuf[kEncryptedKeyPrefixSize],
                kEncryptFlag,
                outErrMsgPtr
            );
            if (theRet < 0) {
                return theRet;
            }
            theKeyPtr = theBuf;
            theLen    = kEncryptedKeyPrefixSize + theRet;
        } else {
            theKeyPtr = reinterpret_cast<const char*>(theMd);
        }
        if (inWriterPtr) {
            char      theBase64Buf[Base64::GetEncodedMaxBufSize(kKeyBufSize)];
            const int theBase64Len = Base64::Encode(
                theKeyPtr, theLen, theBase64Buf);
            if (0 < theBase64Len) {
                inWriterPtr->Write(theBase64Buf, theBase64Len);
            }
            return theBase64Len;
        }
        if (outKeyPtr) {
            outKeyPtr->assign(theKeyPtr, theLen);
            return theLen;
        }
        if (inMaxKeyLen < (int)theLen || ! inKeyBufferPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr += "insufficent key buffer size";
            } else {
                KFS_LOG_STREAM_ERROR <<
                    " insufficent key buffer size: " << inMaxKeyLen <<
                    " required: "                    << theLen <<
                KFS_LOG_EOM;
            }
            return -EINVAL;
        }
        memcpy(inKeyBufferPtr, theBuf, theLen);
        return theLen;
    }
    string GetSessionKey(
        const DelegationToken& inToken,
        const char*            inSignaturePtr,
        const char*            inKeyPtr,
        int                    inKeyLen,
        const char*            inSubjectPtr,
        int                    inSubjectLen)
    {
        string theRet;
        MakeSessionKey(
            inToken, inSignaturePtr, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, 0, 0, &theRet, 0);
        return theRet;
    }
    static int MakeIv(
        char*   inIvPtr,
        string* outErrMsgPtr)
    {
        if (RAND_bytes(
                reinterpret_cast<unsigned char*>(inIvPtr), kCryptIvLen) <= 0) {
            if (outErrMsgPtr) {
                EvpErrorStr("RAND_bytes failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "RAND_bytes failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            return -EFAULT;
        }
        return 0;
    }
    static int Crypt(
        const char* inKeyPtr,
        int         inKeyLen,
        const char* inIvPtr,
        const char* inPtr,
        int         inLen,
        char*       inOutPtr,
        bool        inEncryptFlag,
        string*     outErrMsgPtr)
    {
        if (inLen <= 0) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "invalid empty or null input buffer";
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "invalid empty or null encryption buffer" <<
                KFS_LOG_EOM;
            }
            return -EINVAL;
        }
        const int kMinKeyLength = 8;
        if (inKeyLen <= kMinKeyLength || ! inKeyPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "invalid or null encryption key";
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "invalid or null encryption key" <<
                KFS_LOG_EOM;
            }
            return -EINVAL;
        }
        if (! inEncryptFlag && inLen % kCryptBlockLen != 0) {
            if (outErrMsgPtr) {
                *outErrMsgPtr =
                    "input buffer length is not multiple of cipher block size";
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "input buffer length is not multiple of cipher block"
                    " size" <<
                KFS_LOG_EOM;
            }
            return -EINVAL;
        }
        const unsigned char* theKeyPtr;
        unsigned char        theMd[EVP_MAX_MD_SIZE];
        if (inKeyLen < kCryptKeyLen) {
            EVP_MD_CTX theCtx;
            EVP_MD_CTX_init(&theCtx);
            if (! EVP_DigestInit_ex(&theCtx, EVP_sha256(), 0)) {
                if (outErrMsgPtr) {
                    EvpErrorStr("EVP_DigestInit_ex failure: ", outErrMsgPtr);
                } else {
                    KFS_LOG_STREAM_ERROR <<
                        "EVP_DigestInit_ex failure: " << EvpError() <<
                    KFS_LOG_EOM;
                }
                return -EFAULT;
            }
            if (! EVP_DigestUpdate(&theCtx, inKeyPtr, inKeyLen)) {
                if (outErrMsgPtr) {
                    EvpErrorStr("EVP_DigestUpdate failure: ", outErrMsgPtr);
                } else {
                    KFS_LOG_STREAM_ERROR <<
                        "EVP_DigestUpdate failure: " << EvpError() <<
                    KFS_LOG_EOM;
                }
                EVP_MD_CTX_cleanup(&theCtx);
                return -EFAULT;
            }
            unsigned int theLen = 0;
            if (! EVP_DigestFinal_ex(&theCtx, theMd, &theLen) || theLen <= 0) {
                if (outErrMsgPtr) {
                    EvpErrorStr("EVP_DigestFinal_ex failure: ", outErrMsgPtr);
                } else {
                    KFS_LOG_STREAM_ERROR <<
                        "EVP_DigestFinal_ex failure: " << EvpError() <<
                    KFS_LOG_EOM;
                }
                EVP_MD_CTX_cleanup(&theCtx);
                return -EFAULT;
            }
            QCRTASSERT(theLen <= EVP_MAX_MD_SIZE);
            EVP_MD_CTX_cleanup(&theCtx);
            theKeyPtr = theMd;
        } else {
            theKeyPtr = reinterpret_cast<const unsigned char*>(inKeyPtr);
        }
        EVP_CIPHER_CTX theCtx;
        EVP_CIPHER_CTX_init(&theCtx);
        if (! EVP_CipherInit_ex(&theCtx, EVP_aes_256_cbc(), 0,
                theKeyPtr,
                reinterpret_cast<const unsigned char*>(inIvPtr),
                inEncryptFlag ? 1 : 0)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_CipherInit_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_CipherInit_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            return -EFAULT;
        }
        if (EVP_CIPHER_CTX_block_size(&theCtx) != kCryptBlockLen ||
                EVP_CIPHER_CTX_key_length(&theCtx) != kCryptKeyLen ||
                EVP_CIPHER_CTX_iv_length(&theCtx) != kCryptIvLen) {
            KFS_LOG_STREAM_FATAL <<
                "invalid cipher parameters:" <<
                    " block: " << EVP_CIPHER_CTX_block_size(&theCtx) <<
                    " key: "   << EVP_CIPHER_CTX_key_length(&theCtx) <<
                    " iv: "    << EVP_CIPHER_CTX_iv_length(&theCtx)  <<
            KFS_LOG_EOM;
            MsgLogger::Stop();
            abort();
            return -EFAULT;
        }
        int theLen = 0;
        if (! EVP_CipherUpdate(&theCtx,
                reinterpret_cast<unsigned char*>(inOutPtr), &theLen,
                reinterpret_cast<const unsigned char*>(inPtr), inLen)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_CipherUpdate failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_CipherUpdate failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_CIPHER_CTX_cleanup(&theCtx);
            return -EFAULT;
        }
        int theRemLen = 0;
        if (! EVP_CipherFinal_ex(&theCtx,
                reinterpret_cast<unsigned char*>(inOutPtr) + theLen,
                &theRemLen)) {
            if (outErrMsgPtr) {
                EvpErrorStr("EVP_CipherFinal_ex failure: ", outErrMsgPtr);
            } else {
                KFS_LOG_STREAM_ERROR <<
                    "EVP_CipherFinal_ex failure: " << EvpError() <<
                KFS_LOG_EOM;
            }
            EVP_CIPHER_CTX_cleanup(&theCtx);
            return -EINVAL; // Possible invalid key.
        }
        theLen += theRemLen;
        EVP_CIPHER_CTX_cleanup(&theCtx);
        QCASSERT(theLen <= inLen + (inEncryptFlag ? EVP_MAX_BLOCK_LENGTH : 0));
        return theLen;
    }
    static int DecryptSessionKeyFromString(
        const CryptoKeys& inKeys,
        const char*       inStrPtr,
        int               inStrLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr)
    {
        char theBuf[kMaxEncryptedKeyLen];
        int  theLen = DecodeBase64(
            inStrPtr, inStrLen, theBuf, kKeyBufSize, outErrMsgPtr);
        if (theLen < 0) {
            if (theLen == -ERANGE) {
                theLen        = -EINVAL;
                *outErrMsgPtr = "invalid key string size";
            }
            return theLen;
        }
        return DecryptSessionKey(inKeys, theBuf, theLen, outKey, outErrMsgPtr);
    }
    static int DecryptSessionKey(
        const CryptoKeys& inKeys,
        const char*       inKeyPtr,
        int               inKeyLen,
        CryptoKeys::Key&  outKey,
        string*           outErrMsgPtr)
    {
        if (inKeyLen < kMinEncryptedKeyLen || kMaxEncryptedKeyLen < inKeyLen) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "invalid key buffer size";
            }
            return -EINVAL;
        }
        const char* thePtr = inKeyPtr;
        kfsKeyId_t  theId;
        Read(thePtr, theId);
        CryptoKeys::Key theKey;
        if (! inKeys.Find(theId, theKey)) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "no key found";
            }
            return -EPERM;
        }
        const bool kEncryptFlag = false;
        const int  theLen       = Crypt(
            theKey.GetPtr(),
            theKey.GetSize(),
            thePtr,
            thePtr + kCryptIvLen,
            inKeyLen - kEncryptedKeyPrefixSize,
            outKey.WritePtr(),
            kEncryptFlag,
            outErrMsgPtr
        );
        QCRTASSERT(theLen <= theKey.GetSize());
        if (theLen < 0) {
            return theLen;
        }
        if (CryptoKeys::Key::kLength != theLen) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "decryption failure: invalid result size";
            }
            return -EPERM;
        }
        return 0;
    }
private:
    enum {
        kTokenFiledsSize =
            sizeof(kfsUid_t) +
            sizeof(TokenSeq) +
            sizeof(kfsKeyId_t) +
            sizeof(int64_t) +
            sizeof(uint32_t),
        kTokenSize = kTokenFiledsSize + kSignatureLength
    };
    // 256 bit AES with standard 128 bit blocks.
    enum { kCryptBlockLen  = 128 / 8 };
    enum { kCryptIvLen     = kCryptBlockLen };
    enum { kCryptKeyLen    = 256 / 8 };
    enum {
        kEncryptedKeyPrefixSize = sizeof(kfsKeyId_t) + kCryptIvLen,
        kKeyBufSize             = kEncryptedKeyPrefixSize +
            EVP_MAX_MD_SIZE + kCryptBlockLen,
        kMinEncryptedKeyLen        =
            kEncryptedKeyPrefixSize + CryptoKeys::Key::kLength;
        kMaxEncryptedKeyLen        = kMinEncryptedKeyLen + kCryptBlockLen;
    };

    char mBuffer[kTokenSize + 1];

    template<typename T>
    static void Write(
        char*& ioPtr,
        T      inVal)
    {
        char* const theStartPtr = ioPtr;
        ioPtr += sizeof(inVal);
        char*       thePtr      = ioPtr;
        while (theStartPtr <= --thePtr) {
            *thePtr = (char)(inVal & 0xFF);
            inVal >>= 8;
        }
    }
    template<typename T>
    static void Read(
        const char*& ioPtr,
        T&           outVal)
    {
        const char*       thePtr    = ioPtr;
        ioPtr += sizeof(outVal);
        const char* const theEndPtr = ioPtr;
        outVal = 0;
        while (thePtr < theEndPtr) {
            outVal <<= 8;
            outVal |= (*thePtr++ & 0xFF);
        }
    }
    static int DecodeBase64(
        const char* inPtr,
        int         inLen,
        char*       inOutPtr,
        int         inOutMaxLen,
        string*     outErrMsgPtr = 0)
    {
        if (! inPtr || inLen <= 0) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "null buffer or length";
            }
            return -EINVAL;
        }
        const char* thePtr    = inPtr;
        const char* theEndPtr = thePtr + inLen;
        while (thePtr < theEndPtr && (*thePtr & 0xFF) <= ' ' && *thePtr != 0) {
            thePtr++;
        }
        while (thePtr < theEndPtr && (*theEndPtr & 0xFF) <= ' ') {
            theEndPtr--;
        }
        int theLen = (int)(theEndPtr - thePtr);
        if (theLen <= 0 || inOutMaxLen < Base64::GetMaxDecodedLength(theLen)) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "insufficient output buffer space";
            }
            return -ERANGE;
        }
        theLen = Base64::Decode(thePtr, theLen, inOutPtr);
        QCRTASSERT(theLen <= inOutMaxLen);
        return theLen;
    }
};

DelegationToken::DelegationToken(
    kfsUid_t    inUid,
    TokenSeq    inSeq,
    kfsKeyId_t  inKeyId,
    int64_t     inIssuedTime,
    uint16_t    inFlags,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
    : mUid(inUid),
      mSeq(inSeq),
      mKeyId(inKeyId),
      mIssuedTimeAndFlags((inIssuedTime << kIssuedTimeShift) | inFlags),
      mValidForSec(inValidForSec)
{
    if (! inKeyPtr || inKeyLen <= 0) {
        return;
    }
    WorkBuf theBuf;
    if (! theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, mSignature)) {
        mValidForSec = 0;
    }
}

    bool
DelegationToken::Init(
    kfsUid_t    inUid,
    TokenSeq    inSeq,
    kfsKeyId_t  inKeyId,
    int64_t     inIssueTime,
    uint16_t    inFlags,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    mUid                = inUid;
    mSeq                = inSeq;
    mKeyId              = inKeyId;
    mIssuedTimeAndFlags = (inIssueTime << kIssuedTimeShift) | inFlags;
    mValidForSec        = inValidForSec;
    if (! inKeyPtr || inKeyLen <= 0) {
        return false;
    }
    WorkBuf theBuf;
    return theBuf.SerializeAndSign(*this, inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen, mSignature);
}

    string
DelegationToken::ToString()
{
    WorkBuf theBuf;
    return theBuf.ToString(*this, mSignature);
}

    bool
DelegationToken::FromString(
    const string& inString,
    const char*   inKeyPtr,
    int           inKeyLen,
    const char*   inSubjectPtr,
    int           inSubjectLen)
{
    return FromString(
        inString.data(), (int)inString.size(), inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen
    );
}

    bool
DelegationToken::FromString(
    const char* inPtr,
    int         inLen,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    WorkBuf theBuf;
    if (! theBuf.FromBase64(*this, inPtr, inLen)) {
        return false;
    }
    char theSignature[kSignatureLength];
    return (! inKeyPtr ||
        (theBuf.Sign(inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, theSignature) &&
        memcmp(theSignature, mSignature, kSignatureLength) == 0)
    );
}

    int
DelegationToken::Process(
    const char*       inPtr,
    int               inLen,
    int64_t           inTimeNowSec,
    const CryptoKeys& inKeys,
    char*             inSessionKeyPtr,
    int               inMaxSessionKeyLength,
    string*           outErrMsgPtr,
    const char*       inSubjectPtr,
    int               inSubjectLen)
{
    WorkBuf theBuf;
    if (! theBuf.FromBase64(*this, inPtr, inLen)) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "invalid format";
        }
        return false;
    }
    const uint32_t theValidForSec = GetValidForSec();
    if (theValidForSec <= 0) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "expired: 0 valid for time";
        }
        return false;
    }
    const uint32_t theMaxClockSkewSec = min(uint32_t(5 * 60), theValidForSec);
    if (inTimeNowSec + theMaxClockSkewSec < GetIssuedTime()) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "issue time is in the future";
        }
        return false;
    }
    if (GetIssuedTime() + theValidForSec < inTimeNowSec) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "exired";
        }
        return false;
    }
    CryptoKeys::Key theKey;
    if (! inKeys.Find(GetKeyId(), theKey)) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "no key found";
        }
        return false;
    }
    char theSignature[kSignatureLength];
    if (! theBuf.Sign(
            theKey.GetPtr(),
            theKey.GetSize(),
            inSubjectPtr,
            inSubjectLen,
            theSignature,
            outErrMsgPtr)) {
        return false;
    }
    if (memcmp(theSignature, mSignature, kSignatureLength) != 0) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "invalid signature";
        }
        return false;
    }
    return (inMaxSessionKeyLength < 0 || theBuf.MakeSessionKey(
        theKey.GetPtr(),
        theKey.GetSize(),
        inSubjectPtr,
        inSubjectLen,
        inSessionKeyPtr,
        inMaxSessionKeyLength,
        0,
        outErrMsgPtr
    ));
}

    ostream&
DelegationToken::Display(
    ostream& inStream) const
{
    WorkBuf theBuf;
    return theBuf.Display(inStream, *this, mSignature);
}

    istream&
DelegationToken::Parse(
    istream&    inStream,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen)
{
    string theStr;
    if ((inStream >> theStr) && ! FromString(theStr, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen)) {
        inStream.setstate(ostream::failbit);
    }
    return inStream;
}

    bool
DelegationToken::Validate(
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen) const
{
    char theSignBuf[kSignatureLength];
    WorkBuf theBuf;
    theBuf.Sign(inKeyPtr, inKeyLen, inSubjectPtr, inSubjectLen, theSignBuf);
    return (memcmp(mSignature, theSignBuf, kSignatureLength) == 0);
}

    string
DelegationToken::CalcSessionKey(
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen) const
{
    WorkBuf theBuf;
    return theBuf.GetSessionKey(*this, mSignature, inKeyPtr, inKeyLen,
        inSubjectPtr, inSubjectLen);
}

    ostream&
DelegationToken:: ShowSelf(
    ostream& inStream) const
{
    const ostream::fmtflags theFlags = inStream.flags();
    inStream <<
    "uid: "   << mUid <<
    " seq: "   << mSeq <<
    " keyId: " << mKeyId <<
    " time:  " << GetIssuedTime() << "+" << mValidForSec <<
    " flags: 0x" << hex << GetFlags() <<
    " sign: ";
    if (0u < mValidForSec) {
        inStream << hex << noshowbase;
        for (int i = 0; i < kSignatureLength; i++) {
            inStream << setfill('0') << setw(2) <<
                ((int)mSignature[i] & 0xFF);
        }
    }
    inStream.setf(theFlags);
    return inStream;
}

    /* static */ template<typename T> bool
DelegationToken::WriteTokenSelf(
    T&              inWriter,
    kfsUid_t        inUid,
    TokenSeq        inSeq,
    kfsKeyId_t      inKeyId,
    int64_t         inIssuedTime,
    uint16_t        inFlags,
    uint32_t        inValidForSec,
    const char*     inKeyPtr,
    int             inKeyLen,
    const char*     inSubjectPtr,
    int             inSubjectLen,
    bool            inWriteSessionKeyFlag,
    kfsKeyId_t      inSessionKeyKeyId,
    const char*     inSessionKeyKeyPtr,
    int             inSessionKeyKeyLen)
{
    DelegationToken theToken(
        inUid,
        inSeq,
        inKeyId,
        inIssuedTime,
        inFlags,
        inValidForSec,
        0,
        0
    );
    if (! inKeyPtr || inKeyLen <= 0) {
        return false;
    }
    WorkBuf theBuf;
    if (! theBuf.SerializeAndSign(theToken, inKeyPtr, inKeyLen,
            inSubjectPtr, inSubjectLen, theToken.mSignature)) {
        theToken.mValidForSec = 0;
        return false;
    }
    if (! theBuf.Write(inWriter, theToken, theToken.mSignature)) {
        return false;
    }
    if (! inWriteSessionKeyFlag) {
        return true;
    }
    inWriter.Write(" ", 1);
    char* const   theKeyBufferPtr = 0;
    int const     theMaxKeyLen    = 0;
    string* const theKeyPtr       = 0;
    string* const theErrMsgPtr    = 0;
    return (theBuf.MakeSessionKey(
        inKeyPtr,
        inKeyLen,
        inSubjectPtr,
        inSubjectLen,
        inSessionKeyKeyId,
        inSessionKeyKeyPtr,
        inSessionKeyKeyLen,
        theKeyBufferPtr,
        theMaxKeyLen,
        theKeyPtr,
        theErrMsgPtr,
        &inWriter
    ) > 0);
}

    /* static */ bool
DelegationToken::WriteToken(
    IOBufferWriter& inWriter,
    kfsUid_t        inUid,
    TokenSeq        inSeq,
    kfsKeyId_t      inKeyId,
    int64_t         inIssuedTime,
    uint16_t        inFlags,
    uint32_t        inValidForSec,
    const char*     inKeyPtr,
    int             inKeyLen,
    const char*     inSubjectPtr,
    int             inSubjectLen,
    bool            inWriteSessionKeyFlag,
    kfsKeyId_t      inSessionKeyKeyId,
    const char*     inSessionKeyKeyPtr,
    int             inSessionKeyKeyLen)
{
    return WriteTokenSelf(
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
        inSubjectLen,
        inWriteSessionKeyFlag,
        inSessionKeyKeyId,
        inSessionKeyKeyPtr,
        inSessionKeyKeyLen
    );
}

namespace {
class OstreamWriter
{
public:
    OstreamWriter(
        ostream& inStream)
        : mStream(inStream)
        {}
    void Write(
        const char* inDataPtr,
        size_t      inLength) const
    {
        mStream.write(inDataPtr, inLength);
    }
private:
    ostream& mStream;
};
}

    /* static */ bool
DelegationToken::WriteToken(
    ostream&    inStream,
    kfsUid_t    inUid,
    TokenSeq    inSeq,
    kfsKeyId_t  inKeyId,
    int64_t     inIssuedTime,
    uint16_t    inFlags,
    uint32_t    inValidForSec,
    const char* inKeyPtr,
    int         inKeyLen,
    const char* inSubjectPtr,
    int         inSubjectLen,
    bool        inWriteSessionKeyFlag,
    kfsKeyId_t  inSessionKeyKeyId,
    const char* inSessionKeyKeyPtr,
    int         inSessionKeyKeyLen)
{
    OstreamWriter theWriter(inStream);
    return WriteTokenSelf(
        theWriter,
        inUid,
        inSeq,
        inKeyId,
        inIssuedTime,
        inFlags,
        inValidForSec,
        inKeyPtr,
        inKeyLen,
        inSubjectPtr,
        inSubjectLen,
        inWriteSessionKeyFlag,
        inSessionKeyKeyId,
        inSessionKeyKeyPtr,
        inSessionKeyKeyLen
    );
}

}
