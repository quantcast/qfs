//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/06/24
// Author:  Mike Ovsiannikov 
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
// \brief Ssl socket layer implementation.
//
//----------------------------------------------------------------------------

#include "SslFilter.h"

#include "IOBuffer.h"
#include "Globals.h"
#include "qcdio/QCMutex.h"
#include "common/Properties.h"

#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/engine.h>

#include <errno.h>

#include <stdlib.h>
#include <string>
#include <algorithm>

namespace KFS
{
using std::string;
using std::max;
using namespace KFS::libkfsio;

class SslFilter::Impl : private IOBuffer::Reader
{
public:
    typedef SslFilter::Ctx       Ctx;
    typedef SslFilter::Error     Error;
    typedef SslFilter::ServerPsk ServerPsk;

    static Error Initialize()
    {
        if (sOpenSslInitPtr) {
            return 0;
        }
        static OpenSslInit sOpenSslInit;
        sOpenSslInitPtr = &sOpenSslInit;
#if OPENSSL_VERSION_NUMBER < 0x1000000fL
        CRYPTO_set_id_callback(&ThreadIdCB);
#else
        // Ensure that the CRYPTO_THREADID_* is present, and use the default
        // implementation.
        CRYPTO_THREADID_get_callback();
#endif
        CRYPTO_set_locking_callback(&LockingCB);
        OpenSSL_add_all_algorithms();
        SSL_load_error_strings();
        ERR_load_crypto_strings();
        ENGINE_load_builtin_engines();
        SSL_library_init();
        sOpenSslInitPtr->mExDataIdx =
            SSL_get_ex_new_index(0, (void*)"SslFilter::Impl", 0, 0, 0);
        if (sOpenSslInitPtr->mExDataIdx < 0) {
            const Error theErr = GetAndClearErr();
            Cleanup();
            return theErr;
        }
        return 0;
    }
    static Error Cleanup()
    {
        if (! sOpenSslInitPtr) {
            return 0;
        }
        ENGINE_cleanup();
        EVP_cleanup();
        CRYPTO_cleanup_all_ex_data();
        ERR_remove_state(0);
        ERR_free_strings();
        CRYPTO_set_locking_callback(0);
        sOpenSslInitPtr = 0;
        return 0;
    }
    static string GetErrorMsg(
        Error inError)
    {
        const size_t kBufSize = 127;
        char theBuf[kBufSize + 1];
        theBuf[0] = 0;
        theBuf[kBufSize] = 0;
        ERR_error_string_n(inError, theBuf, kBufSize);
        return string(theBuf);
    }
    static Ctx* CreateCtx(
        const bool        inServerFlag,
        const bool        inPskOnlyFlag,
        int               inSessionCacheSize,
        const char*       inParamsPrefixPtr,
        const Properties& inParams,
        string*           inErrMsgPtr)
    {
        SSL_CTX* const theRetPtr = SSL_CTX_new(
            inServerFlag ? TLSv1_server_method() : TLSv1_client_method());
        SSL_CTX_set_mode(theRetPtr, SSL_MODE_ENABLE_PARTIAL_WRITE);
        Properties::String theParamName;
        if (inParamsPrefixPtr) {
            theParamName.Append(inParamsPrefixPtr);
        }
        const size_t thePrefLen = theParamName.GetSize();
        if (! SSL_CTX_set_cipher_list(
            theRetPtr,
            inParams.getValue(
                theParamName.Truncate(thePrefLen).Append(
                    inPskOnlyFlag ? "cipherpsk" : "cipher"),
                inPskOnlyFlag ?
                    "!ADH:!AECDH:!MD5:HIGH:@STRENGTH" :
                    "!ADH:!AECDH:!MD5:!3DES:PSK:@STRENGTH"
                ))) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = GetErrorMsg(GetAndClearErr());
            }
            SSL_CTX_free(theRetPtr);
            return 0;
        }
        SSL_CTX_set_options(
            theRetPtr,
            inParams.getValue(
                theParamName.Truncate(thePrefLen).Append("options"),
                long(SSL_OP_NO_COMPRESSION))
        );
        if (inPskOnlyFlag) {
            return reinterpret_cast<Ctx*>(theRetPtr);
        }
        if (inParams.getValue(
                theParamName.Truncate(thePrefLen).Append("verifyPeer"),
                0) != 0) {
            SSL_CTX_set_verify(theRetPtr, SSL_VERIFY_PEER, 0);
        }
        const char* const kNullStrPtr  = 0;
        const char* const theCAFilePtr = inParams.getValue(
            theParamName.Truncate(thePrefLen).Append("CAFile"), kNullStrPtr);
        const char* const theCADirPtr = inParams.getValue(
            theParamName.Truncate(thePrefLen).Append("CADir"),  kNullStrPtr);
        if ((theCAFilePtr || theCADirPtr) &&
                ! SSL_CTX_load_verify_locations(
                    theRetPtr, theCAFilePtr, theCADirPtr)) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = GetErrorMsg(GetAndClearErr());
            }
            SSL_CTX_free(theRetPtr);
            return 0;
        }
        return reinterpret_cast<Ctx*>(theRetPtr);
    }
    static void FreeCtx(
        Ctx* inCtxPtr)
    {
        if (inCtxPtr) {
            SSL_CTX_free(reinterpret_cast<SSL_CTX*>(inCtxPtr));
        }
    }
    Impl(
        Ctx&        inCtx,
        const char* inPskDataPtr,
        size_t      inPskDataLen,
        const char* inPskCliIdendityPtr,
        ServerPsk*  inServerPskPtr)
        : Reader(),
          mSslPtr(SSL_new(reinterpret_cast<SSL_CTX*>(&inCtx))),
          mError(mSslPtr ? 0 : GetAndClearErr()),
          mPskData(
            inPskDataPtr ? inPskDataPtr : "",
            inPskDataPtr ? inPskDataLen : 0),
          mPskCliIdendity(inPskCliIdendityPtr ? inPskCliIdendityPtr : ""),
          mServerPskPtr(inServerPskPtr)
    {
        if (mSslPtr &&
                ! SSL_set_ex_data(mSslPtr, sOpenSslInitPtr->mExDataIdx, this)) {
            mError = GetAndClearErr();
            SSL_free(mSslPtr);
            mSslPtr = 0;
        }
        SetPskCB();
    }
    ~Impl()
    {
        if (mSslPtr) {
            SSL_free(mSslPtr);
        }
    }
    Error GetError() const
        { return mError; }
    void SetPsk(
        const char* inPskDataPtr,
        size_t      inPskDataLen)
    {
        mPskData.assign(inPskDataPtr, inPskDataLen);
        SetPskCB();
    }
    bool WantRead(
        const NetConnection& inConnection) const
    {
        return (
            mSslPtr && mError == 0 &&
            (SSL_want_read(mSslPtr) ||
            (inConnection.IsReadReady() && SSL_is_init_finished(mSslPtr)))
        );
    }
    bool WantWrite(
        const NetConnection& inConnection) const
    {
        return (
            mSslPtr && mError == 0 &&
            (SSL_want_write(mSslPtr) ||
            (inConnection.IsWriteReady() && SSL_is_init_finished(mSslPtr)))
        );
    }
    int Read(
        NetConnection& inConnection,
        TcpSocket&     inSocket,
        IOBuffer&      inIoBuffer,
        int            inMaxRead)
    {
        if (! mSslPtr || SSL_get_fd(mSslPtr) != inSocket.GetFd()) {
            return -EINVAL;
        }
        const int theRet = DoHandshake();
        if (theRet) {
            return theRet;
        }
        return inIoBuffer.Read(-1, inMaxRead, this);
    }
    int Write(
        NetConnection& inConnection,
        TcpSocket&     inSocket,
        IOBuffer&      inIoBuffer)
    {
        if (! mSslPtr || SSL_get_fd(mSslPtr) != inSocket.GetFd()) {
            return -EINVAL;
        }
        const int theRet = DoHandshake();
        if (theRet) {
            return theRet;
        }
        int theWrCnt = 0;
        for (IOBuffer::iterator theIt = inIoBuffer.begin();
                theIt != inIoBuffer.end();
                ) {
            const int theNWr = theIt->BytesConsumable();
            if (theNWr <= 0) {
                ++theIt;
                continue;
            }
            const int theRet = SSL_write(mSslPtr, theIt->Consumer(), theNWr);
            if (theRet <= 0) {
                break;
            }
            theWrCnt += theRet;
            inIoBuffer.Consume(theRet);
            theIt = inIoBuffer.begin();
        }
        if (0 < theWrCnt) {
            globals().ctrNetBytesWritten.Update(theWrCnt);
        }
        bool theEofFlag = false;
        const int theErr = SslRetToErr(theRet, theEofFlag);
        if (theWrCnt <= 0 || (theErr != -EAGAIN && theErr != -EINTR)) {
            return theErr;
        }
        return theWrCnt;
    }
    void Close(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr)
    {
        if (mSslPtr && inSocketPtr &&
                SSL_get_fd(mSslPtr) == inSocketPtr->GetFd()) {
            SSL_shutdown(mSslPtr);
        }
        inConnection.SetFilter(0);
        delete this;
        inConnection.Close();
    }
    void Attach(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr)
    {
        if (! inSocketPtr || ! inSocketPtr->IsGood() || mSslPtr) {
            return;
        }
        SSL_set_fd(mSslPtr, inSocketPtr->GetFd());
    }
    void Detach(
        NetConnection& inConnection,
        TcpSocket*     /* inSocketPtr */)
    {
        if (mSslPtr) {
            return;
        }
        SSL_set_fd(mSslPtr, -1);
    }
    virtual int Read(
        int   /* inFd */,
        void* inBufPtr,
        int   inNumRead)
    {
        if (inNumRead <= 0) {
            return 0;
        }
        if (! inBufPtr) {
            return -EINVAL;
        }
        const int theRet = SSL_read(mSslPtr, inBufPtr, inNumRead);
        if (theRet > 0) {
            return theRet;
        }
        bool theEofFlag = false;
        const int theErr = SslRetToErr(theRet, theEofFlag);
        return (theEofFlag ? 0 : theErr);
    }
private:
    SSL*             mSslPtr;
    unsigned long    mError;
    string           mPskData;
    string           mPskCliIdendity;
    ServerPsk* const mServerPskPtr;

    struct OpenSslInit
    {
        OpenSslInit()
            : mLockCount(max(0, CRYPTO_num_locks())),
              mLocksPtr(mLockCount > 0 ? new QCMutex[mLockCount] : 0),
              mExDataIdx(-1),
              mErrFileNamePtr(0),
              mErrLine(-1)
            {}
        ~OpenSslInit()
        {
            delete [] mLocksPtr;
        }
        int      const mLockCount;
        QCMutex* const mLocksPtr;
        int            mExDataIdx;
        const char*    mErrFileNamePtr;
        int            mErrLine;
    };
    static OpenSslInit* volatile sOpenSslInitPtr;

    static Error GetAndClearErr()
    {
        const Error theRet = ERR_get_error();
        ClearError();
        return theRet;
    }
    static void ClearError()
    {
        while (ERR_get_error())
            {}
    }
    static void LockingCB(
        int         inMode,
        int         inType,
        const char* inFileNamePtr,
        int         inLine)
    {
        if (! sOpenSslInitPtr ||
                inType < 0 ||
                inType >= sOpenSslInitPtr->mLockCount ||
                ! sOpenSslInitPtr->mLocksPtr) {
            abort();
        }
        QCMutex& theMutex = sOpenSslInitPtr->mLocksPtr[inType];
        if (! (((inMode & CRYPTO_LOCK) != 0) ?
                theMutex.Lock() : 
                theMutex.Unlock())) {
            sOpenSslInitPtr->mErrFileNamePtr = inFileNamePtr;
            sOpenSslInitPtr->mErrLine        = inLine;
            abort();
        }
    }
    static unsigned long ThreadIdCB()
    {
        return ((unsigned long)&errno);
    }
    static unsigned int PskServerCB(
        SSL*           inSslPtr,
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen)
    {
        if (! inSslPtr || ! inPskBufferPtr || ! sOpenSslInitPtr) {
            return 0;
        }
        Impl* const thePtr = reinterpret_cast<Impl*>(SSL_get_ex_data(
            inSslPtr, sOpenSslInitPtr->mExDataIdx));
        if (! thePtr || thePtr->mSslPtr != inSslPtr) {
            abort();
            return 0;
        }
        return thePtr->PskSetServer(
            inIdentityPtr, inPskBufferPtr, inPskBufferLen);
    }
    static unsigned int PskClientCB(
        SSL*           inSslPtr,
        const char*    inHintPtr,
        char*          inIdentityBufferPtr,
	unsigned int   inIdentityBufferLen,
        unsigned char* inPskBufferPtr,
	unsigned int   inPskBufferLen)
    {
        if (! inSslPtr || ! inPskBufferPtr || ! sOpenSslInitPtr) {
            return 0;
        }
        Impl* const thePtr = reinterpret_cast<Impl*>(SSL_get_ex_data(
            inSslPtr, sOpenSslInitPtr->mExDataIdx));
        if (! thePtr || thePtr->mSslPtr != inSslPtr) {
            abort();
            return 0;
        }
        return thePtr->PskSetClient(
            inHintPtr,
            inIdentityBufferPtr, inIdentityBufferLen,
            inPskBufferPtr, inPskBufferLen
        );
    }
    unsigned int PskSetServer(
        const char*    inIdentityPtr,
	unsigned char* inPskBufferPtr,
        unsigned int   inPskBufferLen)
    {
        if (mServerPskPtr) {
            return mServerPskPtr->GetPsk(
                inIdentityPtr, inPskBufferPtr, inPskBufferLen);
        }
        if (inPskBufferLen < mPskData.size()) {
            return 0;
        }
        memcpy(inPskBufferPtr, mPskData.data(), mPskData.size());
        return (unsigned int)mPskData.size();
    }
    unsigned int PskSetClient(
        const char*    inHintPtr,
        char*          inIdentityBufferPtr,
	unsigned int   inIdentityBufferLen,
        unsigned char* inPskBufferPtr,
	unsigned int   inPskBufferLen)
    {
        if (inPskBufferLen < mPskData.size()) {
            return 0;
        }
        if (inIdentityBufferLen < mPskCliIdendity.size() + 1) {
            return 0;
        }
        memcpy(inIdentityBufferPtr,
            mPskCliIdendity.c_str(), mPskCliIdendity.size() + 1);
        memcpy(inPskBufferPtr, mPskData.data(), mPskData.size());
        return (unsigned int)mPskData.size();
    }
    int DoHandshake()
    {
        if (SSL_is_init_finished(mSslPtr)) {
            return 0;
        }
        const int theRet = SSL_do_handshake(mSslPtr);
        if (0 < theRet) {
            return 0;
        }
        bool theEofFlag = false;
        const int theErr = SslRetToErr(theRet, theEofFlag);
        if (theErr) {
            return theErr;
        }
        return theRet;
    }
    int SslRetToErr(
        int   inRet,
        bool& outEofFlag)
    {
        outEofFlag = false;
        switch (SSL_get_error(mSslPtr, inRet))
        {
            case SSL_ERROR_NONE:
                break;
            case SSL_ERROR_ZERO_RETURN:
                outEofFlag = true;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
            case SSL_ERROR_WANT_CONNECT:
            case SSL_ERROR_WANT_ACCEPT:
            case SSL_ERROR_WANT_X509_LOOKUP:
                return -EAGAIN;
            case SSL_ERROR_SYSCALL:
                mError = GetAndClearErr();
                if (mError == 0) {
                    if (inRet < 0) {
                        const int theErr = errno;
                        if (theErr > 0) {
                            return -theErr;
                        }
                        if (theErr == 0) {
                            return inRet;
                        }
                        return theErr;
                    }
                }
                return -EINVAL;
            case SSL_ERROR_SSL:
                mError = GetAndClearErr();
                return -EINVAL;
            default:
                return -EINVAL;
        }
        return 0;
    }
    void SetPskCB()
    {
        if (! mPskData.empty() || mServerPskPtr) {
#if OPENSSL_VERSION_NUMBER < 0x1000000fL || defined(OPENSSL_NO_PSK)
            mError = SSL_R_UNSUPPORTED_CIPHER;
#else
            SSL_set_psk_server_callback(mSslPtr, &PskServerCB);
#endif
        }
        if (! mPskData.empty() && ! mServerPskPtr) {
#if OPENSSL_VERSION_NUMBER < 0x1000000fL || defined(OPENSSL_NO_PSK)
            mError = SSL_R_UNSUPPORTED_CIPHER;
#else
            SSL_set_psk_client_callback(mSslPtr, &PskClientCB);
#endif
        }
    }
};
SslFilter::Impl::OpenSslInit* volatile SslFilter::Impl::sOpenSslInitPtr = 0;

    /* static */ SslFilter::Error
SslFilter::Initialize()
{
    return Impl::Initialize();
}
    /* static */ SslFilter::Error
SslFilter::Cleanup()
{
    return Impl::Cleanup();
}

    /* static */ string
SslFilter::GetErrorMsg(
    SslFilter::Error inError)
{
    return Impl::GetErrorMsg(inError);
}

    /* static */ SslFilter::Ctx*
SslFilter::CreateCtx(
    const bool        inServerFlag,
    const bool        inPskOnlyFlag,
    int               inSessionCacheSize,
    const char*       inParamsPrefixPtr,
    const Properties& inParams,
    string*           inErrMsgPtr)
{
    return Impl::CreateCtx(
        inServerFlag, inPskOnlyFlag,
        inSessionCacheSize, inParamsPrefixPtr, inParams, inErrMsgPtr);
}

    /* static */ void
SslFilter::FreeCtx(
    SslFilter::Ctx* inCtxPtr)
{
    Impl::FreeCtx(inCtxPtr);
}

SslFilter::SslFilter(
    Ctx&                  inCtx,
    const char*           inPskDataPtr,
    size_t                inPskDataLen,
    const char*           inPskCliIdendityPtr,
    SslFilter::ServerPsk* inServerPskPtr)
    : mImpl(*(new Impl(
        inCtx,
        inPskDataPtr,
        inPskDataLen,
        inPskCliIdendityPtr,
        inServerPskPtr
    )))
    {}

    /* virtual */
SslFilter::~SslFilter()
{
    delete &mImpl;
}

    void
SslFilter::SetPsk(
    const char* inPskDataPtr,
    size_t      inPskDataLen)
{
    mImpl.SetPsk(inPskDataPtr, inPskDataLen);
}

    SslFilter::Error
SslFilter::GetError() const
{
    return mImpl.GetError();
}

    /* virtual */ bool
SslFilter::WantRead(
    const NetConnection& inConnection) const
{
    return mImpl.WantRead(inConnection);
}

    /* virtual */ bool
SslFilter::WantWrite(
    const NetConnection& inConnection) const
{
    return mImpl.WantWrite(inConnection);
}

    /* virtual */ int
SslFilter::Read(
    NetConnection& inConnection,
    TcpSocket&     inSocket,
    IOBuffer&      inIoBuffer,
    int            inMaxRead)
{
    return mImpl.Read(inConnection, inSocket, inIoBuffer, inMaxRead);
}

    /* virtual */ int
SslFilter::Write(
    NetConnection& inConnection,
    TcpSocket&     inSocket,
    IOBuffer&      inIoBuffer)
{
    return mImpl.Write(inConnection, inSocket, inIoBuffer);
}

    /* virtual */ void
SslFilter::Close(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr)
{
    mImpl.Close(inConnection, inSocketPtr);
}

    /* virtual */ void
SslFilter::Attach(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr)
{
    mImpl.Attach(inConnection, inSocketPtr);
}

    /* virtual */ void
SslFilter::Detach(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr)
{
    mImpl.Detach(inConnection, inSocketPtr);
}

}
