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
#include "qcdio/QCUtils.h"
#include "qcdio/qcstutils.h"
#include "common/Properties.h"
#include "common/MsgLogger.h"

#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/engine.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <string>
#include <algorithm>

namespace KFS
{
using std::string;
using std::max;
using namespace KFS::libkfsio;

class SslFilter::Impl : private IOBuffer::Reader
{
private:
    static void SslCtxSessionFree(
        void*           /* inSslCtx */,
        void*           inSessionPtr,
        CRYPTO_EX_DATA* /* inDataPtr */,
        int             /* inIdx */,
        long            /* inArgLong */,
        void*           /* inArgPtr */)
    {
        if (inSessionPtr) {
            SSL_SESSION_free(reinterpret_cast<SSL_SESSION*>(inSessionPtr));
        }
    }

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
#if OPENSSL_VERSION_NUMBER < 0x10000000L
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
        sOpenSslInitPtr->mAES256CbcCypherDebugPtr = EVP_aes_256_cbc();
        sOpenSslInitPtr->mExDataIdx =
            SSL_get_ex_new_index(0, (void*)"SslFilter::Impl", 0, 0, 0);
        if (sOpenSslInitPtr->mExDataIdx < 0) {
            const Error theErr = GetAndClearErr();
            Cleanup();
            return theErr;
        }
        sOpenSslInitPtr->mExDataSessionIdx =
            SSL_CTX_get_ex_new_index(0, (void*)"SSL_SESSION", 0, 0,
                &SslCtxSessionFree);
        if (sOpenSslInitPtr->mExDataSessionIdx < 0) {
            const Error theErr = GetAndClearErr();
            Cleanup();
            return theErr;
        }
        // Create ssl cts to ensure that all ssl libs static / globals are
        // properly initialized, to help to avoid any possible races.
        SSL_CTX* const theCtxPtr = SSL_CTX_new(TLSv1_method());
        if (theCtxPtr) {
            SSL_free(SSL_new(theCtxPtr));
            SSL_CTX_free(theCtxPtr);
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
#if OPENSSL_VERSION_NUMBER < 0x10000000L || defined(OPENSSL_NO_PSK)
        if (inError == SSL_R_UNSUPPORTED_CIPHER) {
            return string(
                "please re-compile with openssl library version 1.0 or greater"
                " with PSK enabled"
            );
        }
#endif
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
        const char*       inParamsPrefixPtr,
        const Properties& inParams,
        string*           inErrMsgPtr)
    {
        SSL_CTX* const theRetPtr = SSL_CTX_new(
            inServerFlag ? TLSv1_server_method() : TLSv1_client_method());
        if (! theRetPtr) {
            return 0;
        }
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
                    "!ADH:!AECDH:!MD5:!3DES:PSK:@STRENGTH" :
                    "!ADH:!AECDH:!MD5:HIGH:@STRENGTH"
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
                long(0)
#ifdef SSL_OP_NO_COMPRESSION
                | long(SSL_OP_NO_COMPRESSION)
#endif
#ifdef SSL_OP_NO_TICKET
                | (inPskOnlyFlag ? long(SSL_OP_NO_TICKET) : long(0))
#endif
        ));
        SSL_CTX_set_timeout(
                theRetPtr,
                inParams.getValue(
                    theParamName.Truncate(thePrefLen).Append(
                    "session.timeout"), long(4) * 60 * 60)
        );
        const char* const theSessCtxIdPtr = inPskOnlyFlag ?
            "QFS_SSL_PSK_CACHE" : "QFS_SSL_CACHE";
        if (! SSL_CTX_set_session_id_context(theRetPtr,
                reinterpret_cast<const unsigned char*>(theSessCtxIdPtr),
                strlen(theSessCtxIdPtr))) {
            SSL_CTX_free(theRetPtr);
            return 0;
        }
        if (inPskOnlyFlag) {
            SSL_CTX_set_verify(theRetPtr, SSL_VERIFY_NONE, 0);
            SSL_CTX_set_session_cache_mode(theRetPtr, SSL_SESS_CACHE_OFF);
            return reinterpret_cast<Ctx*>(theRetPtr);
        }
        if (inParams.getValue(
                theParamName.Truncate(thePrefLen).Append("verifyPeer"),
                1) != 0) {
            SSL_CTX_set_verify(theRetPtr,
                SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                &VerifyCB);
        } else {
            SSL_CTX_set_verify(theRetPtr, SSL_VERIFY_NONE, 0);
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
        const char* const theX509FileNamePtr = inParams.getValue(
            theParamName.Truncate(thePrefLen).Append("X509PemFile"),
            kNullStrPtr);
        if (theX509FileNamePtr && ! SSL_CTX_use_certificate_file(
                theRetPtr, theX509FileNamePtr, SSL_FILETYPE_PEM)) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = GetErrorMsg(GetAndClearErr());
            }
            SSL_CTX_free(theRetPtr);
            return 0;
        }
        const char* const thePKeyFileNamePtr = inParams.getValue(
            theParamName.Truncate(thePrefLen).Append("PKeyPemFile"),
            kNullStrPtr);
        if (thePKeyFileNamePtr && ! SSL_CTX_use_PrivateKey_file(
                theRetPtr, thePKeyFileNamePtr, SSL_FILETYPE_PEM)) {
            if (inErrMsgPtr) {
                *inErrMsgPtr = GetErrorMsg(GetAndClearErr());
            }
            SSL_CTX_free(theRetPtr);
            return 0;
        }
        return reinterpret_cast<Ctx*>(theRetPtr);
    }
    static long GetSessionTimeout(
        Ctx& inCtx)
        { return SSL_CTX_get_timeout(reinterpret_cast<SSL_CTX*>(&inCtx)); }
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
        ServerPsk*  inServerPskPtr,
        VerifyPeer* inVerifyPeerPtr,
        bool        inDeleteOnCloseFlag,
        bool&       inReadPendingFlag)
        : Reader(),
          mSslPtr(SSL_new(reinterpret_cast<SSL_CTX*>(&inCtx))),
          mError(mSslPtr ? 0 : GetAndClearErr()),
          mPskData(
            inPskDataPtr ? inPskDataPtr : "",
            inPskDataPtr ? inPskDataLen : 0),
          mPskCliIdendity(inPskCliIdendityPtr ? inPskCliIdendityPtr : ""),
          mAuthName(),
          mServerPskPtr(inServerPskPtr),
          mVerifyPeerPtr(inVerifyPeerPtr),
          mReadPendingFlag(inReadPendingFlag),
          mDeleteOnCloseFlag(inDeleteOnCloseFlag),
          mSessionStoredFlag(false),
          mShutdownInitiatedFlag(false),
          mServerFlag(false),
          mSslEofFlag(false),
          mSslErrorFlag(false),
          mShutdownCompleteFlag(false),
          mVerifyOrGetPskInvokedFlag(false),
          mRenegotiationPendingFlag(false)
    {
        if (! mSslPtr) {
            return;
        }
        if (SSL_set_ex_data(mSslPtr, sOpenSslInitPtr->mExDataIdx, this)) {
            SetPskCB();
            SSL_set_read_ahead(mSslPtr, 1);
            mServerFlag = ! SSL_in_connect_init(mSslPtr);
        } else {
            mError = GetAndClearErr();
            SSL_free(mSslPtr);
            mSslPtr = 0;
        }
    }
    ~Impl()
    {
        if (mSslPtr) {
            // Set session to 0 to prevent session invalidation with no graceful
            // shutdown. Though with session tickets on cache should not matter.
            SSL_set_session(mSslPtr, 0);
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
            (! mShutdownInitiatedFlag &&
                inConnection.IsReadReady() && SSL_is_init_finished(mSslPtr)))
        );
    }
    bool WantWrite(
        const NetConnection& inConnection) const
    {
        return (
            mSslPtr && mError == 0 &&
            (SSL_want_write(mSslPtr) ||
            (! mShutdownInitiatedFlag &&
                inConnection.IsWriteReady() && SSL_is_init_finished(mSslPtr)))
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
        mReadPendingFlag = false;
        char theByte;
        int  theRet = DoHandshake();
        if (theRet) {
            return theRet;
        }
        if (mShutdownInitiatedFlag) {
            return ShutdownSelf(inConnection);
        }
        if (inMaxRead == 0) {
            // Don't want to read, just complete handshake.
            theRet = mSslEofFlag ? 0 :
                SSL_peek(mSslPtr, &theByte, sizeof(theByte));
            mReadPendingFlag = 0 < theRet;
            if (theRet < 0) {
                theRet = SslRetToErr(theRet);
            }
            return (0 <= theRet ? -EAGAIN : theRet);
        }
        theRet = inIoBuffer.Read(-1, inMaxRead, this);
        mReadPendingFlag = 0 < inMaxRead && inMaxRead <= theRet &&
            0 < SSL_peek(mSslPtr, &theByte, sizeof(theByte));
        return theRet;
    }
    int Write(
        NetConnection& inConnection,
        TcpSocket&     inSocket,
        IOBuffer&      inIoBuffer,
        bool&          outForceInvokeErrHandlerFlag)
    {
        outForceInvokeErrHandlerFlag = false;
        if (! mSslPtr || SSL_get_fd(mSslPtr) != inSocket.GetFd()) {
            return -EINVAL;
        }
        int theRet = DoHandshake();
        if (theRet) {
            return theRet;
        }
        if (mShutdownInitiatedFlag) {
            const int theRet = ShutdownSelf(inConnection);
            // On successful shutdown completion read handler to be invoked in
            // order to let the caller know that shutdown is now complete.
            outForceInvokeErrHandlerFlag = theRet == 0;
            return theRet;
        }
        if (inIoBuffer.IsEmpty()) {
            return 0;
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
            if ((theRet = SSL_write(mSslPtr, theIt->Consumer(), theNWr)) <= 0) {
                break;
            }
            theWrCnt += theRet;
            inIoBuffer.Consume(theRet);
            theIt = inIoBuffer.begin();
        }
        if (0 < theWrCnt) {
            globals().ctrNetBytesWritten.Update(theWrCnt);
            return theWrCnt;
        }
        return SslRetToErr(theRet);
    }
    void Close(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr)
    {
        if (mSslPtr && inSocketPtr &&
                SSL_get_fd(mSslPtr) == inSocketPtr->GetFd()) {
            SSL_shutdown(mSslPtr);
        }
        inConnection.SetFilter(0, 0);
        if (mDeleteOnCloseFlag) {
            delete this;
        }
        inConnection.Close();
    }
    int Attach(
        NetConnection& inConnection,
        TcpSocket*     inSocketPtr,
        string*        outErrMsgPtr)
    {
        if (! inSocketPtr || ! inSocketPtr->IsGood() || ! mSslPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "no tcp socket, or ssl context";
            }
            return -EINVAL;
        }
        errno = 0;
        int theRet = 0;
        if (! SSL_set_fd(mSslPtr, inSocketPtr->GetFd())) {
            mSslErrorFlag = false;
            theRet        = errno;
            mError        = GetAndClearErr();
            if (theRet > 0) {
                theRet = -theRet;
            } else if (theRet == 0) {
                theRet = -ENOMEM;
            }
        }
        if (theRet == 0 && SSL_in_before(mSslPtr)) {
            mError                     = 0;
            mSslEofFlag                = false;
            mSslErrorFlag              = false;
            mVerifyOrGetPskInvokedFlag = false;
            mAuthName.clear();
            mServerFlag = ! SSL_in_connect_init(mSslPtr);
            SetStoredClientSession();
            const int theSslRet = mServerFlag ?
                SSL_accept(mSslPtr) : SSL_connect(mSslPtr);
            if (theSslRet <= 0) {
                theRet = SslRetToErr(theSslRet);
                if (mError == 0 && (
                        theRet == -EAGAIN ||
                        theRet == -EINTR ||
                        theRet == -EWOULDBLOCK)) {
                    theRet = 0;
                }
            }
        }
        if (mError) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = GetErrorMsg(mError);
            }
            if (theRet == 0) {
                theRet = -EINVAL;
            }
        } else if (theRet != 0 && outErrMsgPtr) {
            *outErrMsgPtr = QCUtils::SysError(theRet < 0 ? -theRet : theRet);
        }
        if (theRet == 0) {
            inConnection.Update();
        } else if (mError == 0) {
            mError = 1;
        }
        return theRet;
    }
    void Detach(
        NetConnection& inConnection,
        TcpSocket*     /* inSocketPtr */)
    {
        if (! mSslPtr) {
            return;
        }
        SSL_set_fd(mSslPtr, -1);
    }
    virtual int Read(
        int   /* inFd */,
        void* inBufPtr,
        int   inNumRead)
    {
        if (inNumRead <= 0 || mSslEofFlag) {
            return 0;
        }
        if (! inBufPtr || ! mSslPtr) {
            return -EINVAL;
        }
        char*       thePtr      = reinterpret_cast<char*>(inBufPtr);
        char* const theStartPtr = thePtr;
        char* const theEndPtr   = thePtr + inNumRead;
        int         theRet      = 0;
        while (thePtr < theEndPtr &&
                0 < (theRet = SSL_read(
                    mSslPtr, thePtr, (int)(theEndPtr - thePtr)))) {
            thePtr += theRet;
        }
        if (theStartPtr < thePtr) {
            return (int)(thePtr - theStartPtr);
        }
        const int theErr = SslRetToErr(theRet);
        return (mSslEofFlag ? 0 : theErr);
    }
    virtual time_t GetSessionExpirationTime() const
    {
        if (! mSslPtr || mError != 0 || ! SSL_is_init_finished(mSslPtr)) {
            return (time(0) - 1);
        }
        SSL_SESSION* const theCurSessionPtr = SSL_get_session(mSslPtr);
        if (! theCurSessionPtr) {
            return (time(0) - 1);
        }
        return ((time_t)SSL_SESSION_get_time(theCurSessionPtr) +
                SSL_SESSION_get_timeout(theCurSessionPtr));
    }
    virtual bool RenewSession()
    {
        if (! mSslPtr || mError != 0) {
            return false;
        }
        mRenegotiationPendingFlag =
            mRenegotiationPendingFlag || SSL_renegotiate(mSslPtr) != 0;
        return mRenegotiationPendingFlag;
    }
    bool IsHandshakeDone() const
        { return (mSslPtr && mError == 0 && SSL_is_init_finished(mSslPtr)); }
    string GetAuthName() const
    {
        return (
            (mError == 0 && mSslPtr && IsHandshakeDone()) ?
            mAuthName : string()
        );
    }
    bool IsAuthFailure() const
    {
        return (
            mSslErrorFlag && mSslPtr &&
            ! SSL_is_init_finished(mSslPtr)
        );
    }
    int GetErrorCode() const
        { return (mError ? -EFAULT : 0); }
    string GetErrorMsg() const
        { return (mError ? GetErrorMsg(mError) : string()); }
    int Shutdown(
        NetConnection& inConnection,
        TcpSocket&     inSocket)
    {
        if (! mSslPtr || SSL_get_fd(mSslPtr) != inSocket.GetFd()) {
            return -EINVAL;
        }
        const int theRet = ShutdownSelf(inConnection);
        inConnection.Update();
        return theRet;
    }
    bool IsShutdownReceived() const
        { return (mSslEofFlag && mSslPtr != 0); }
private:
    SSL*              mSslPtr;
    unsigned long     mError;
    string            mPskData;
    string            mPskCliIdendity;
    string            mAuthName;
    string            mPeerName;
    ServerPsk* const  mServerPskPtr;
    VerifyPeer* const mVerifyPeerPtr;
    bool&             mReadPendingFlag;
    const bool        mDeleteOnCloseFlag:1;
    bool              mSessionStoredFlag:1;
    bool              mShutdownInitiatedFlag:1;
    bool              mServerFlag:1;
    bool              mSslEofFlag:1;
    bool              mSslErrorFlag:1;
    bool              mShutdownCompleteFlag:1;
    bool              mVerifyOrGetPskInvokedFlag:1;
    bool              mRenegotiationPendingFlag:1;

    struct OpenSslInit
    {
        OpenSslInit()
            : mLockCount(max(0, CRYPTO_num_locks())),
              mLocksPtr(mLockCount > 0 ? new QCMutex[mLockCount] : 0),
              mSessionUpdateMutex(),
              mExDataIdx(-1),
              mExDataSessionIdx(-1),
              mErrFileNamePtr(0),
              mErrLine(-1),
              mAES256CbcCypherDebugPtr(0)
            {}
        ~OpenSslInit()
        {
            delete [] mLocksPtr;
        }
        int      const    mLockCount;
        QCMutex* const    mLocksPtr;
        QCMutex           mSessionUpdateMutex;
        int               mExDataIdx;
        int               mExDataSessionIdx;
        const char*       mErrFileNamePtr;
        int               mErrLine;
        // To simplify tracking down using core file if aes-ni is engaged or
        // not.
        const EVP_CIPHER* mAES256CbcCypherDebugPtr;

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
        { return ((unsigned long)&errno); }
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
        mVerifyOrGetPskInvokedFlag = true;
        if (mServerPskPtr) {
            mAuthName.clear();
            return mServerPskPtr->GetPsk(
                inIdentityPtr, inPskBufferPtr, inPskBufferLen,
                mAuthName);
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
        mVerifyOrGetPskInvokedFlag = true;
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
    bool VeifyPeer(
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName)
    {
        mVerifyOrGetPskInvokedFlag = true;
        if (mVerifyPeerPtr) {
            return mVerifyPeerPtr->Verify(
                mAuthName, inPreverifyOkFlag, inCurCertDepth, inPeerName);
        }
        if (! inPreverifyOkFlag) {
            mAuthName.clear();
            return false;
        }
        if (mServerPskPtr && inPeerName.empty() && ! mAuthName.empty()) {
            return true;
        }
        if (inCurCertDepth == 0) {
            mAuthName = inPeerName;
        }
        return true;
    }
    static int VerifyCB(
        int             inPreverifyOkFlag,
        X509_STORE_CTX* inX509CtxPtr)
    {
        if (! sOpenSslInitPtr) {
            return 0;
        }
        SSL* const theSslPtr = reinterpret_cast<SSL*>(
            X509_STORE_CTX_get_ex_data(
                inX509CtxPtr, SSL_get_ex_data_X509_STORE_CTX_idx())
        );
        Impl* const thePtr = reinterpret_cast<Impl*>(SSL_get_ex_data(
            theSslPtr, sOpenSslInitPtr->mExDataIdx));
        if (! thePtr || thePtr->mSslPtr != theSslPtr) {
            abort();
            return 0;
        }
        X509* const theCertPtr = X509_STORE_CTX_get_current_cert(inX509CtxPtr);
        return (thePtr->VeifyPeer(
            inPreverifyOkFlag != 0,
            X509_STORE_CTX_get_error_depth(inX509CtxPtr),
            GetCommonName(theCertPtr ? X509_get_subject_name(theCertPtr) : 0)
        ) ? 1 : 0);
    }
    static string GetCommonName(
       X509_NAME* inNamePtr)
    {
        if (! inNamePtr) {
            return string();
        }
        ASN1_STRING* const theStrPtr = X509_NAME_ENTRY_get_data(
            X509_NAME_get_entry(
                inNamePtr,
                X509_NAME_get_index_by_NID(inNamePtr, NID_commonName, -1)
            )
        );
        int theLen;
        if (! theStrPtr || (theLen = M_ASN1_STRING_length(theStrPtr)) <= 0) {
            return string();
        }
        return string(
            reinterpret_cast<const char*>(M_ASN1_STRING_data(theStrPtr)),
            theLen
        );
    }
    bool VerifyPeerIfNeeded()
    {
        if (mVerifyOrGetPskInvokedFlag) {
            return true;
        }
        // This is invoked in the case of ssl session resume.
        string      thePeerName;
        X509* const theCertPtr = SSL_get_peer_certificate(mSslPtr);
        if (theCertPtr) {
            thePeerName = GetCommonName(X509_get_subject_name(theCertPtr));
            X509_free(theCertPtr);
        }
        const bool kPreverifyOkFlag = true;
        const int  kCurCertDepth    = 0;
        return VeifyPeer(kPreverifyOkFlag, kCurCertDepth, thePeerName);
    }
    int DoHandshake()
    {
        if (SSL_is_init_finished(mSslPtr)) {
            if (! VerifyPeerIfNeeded()) {
                return -EINVAL;
            }
            if (! mServerFlag && ! mSessionStoredFlag) {
                StoreClientSession();
            }
            return 0;
        }
        if (mRenegotiationPendingFlag) {
            mVerifyOrGetPskInvokedFlag = false;
        }
        mRenegotiationPendingFlag = false;
        const int theRet = SSL_do_handshake(mSslPtr);
        if (0 < theRet) {
            if (! VerifyPeerIfNeeded()) {
                return -EINVAL;
            }
            // Try to update in case of renegotiation.
            StoreClientSession();
            return 0;
        }
        const int theErr = SslRetToErr(theRet);
        if (theErr) {
            return theErr;
        }
        return theRet;
    }
    int SslRetToErr(
        int inRet)
    {
        mSslErrorFlag = false;
        switch (SSL_get_error(mSslPtr, inRet))
        {
            case SSL_ERROR_NONE:
                break;
            case SSL_ERROR_ZERO_RETURN:
                mSslEofFlag = true;
                break;
            case SSL_ERROR_WANT_READ:
            case SSL_ERROR_WANT_WRITE:
            case SSL_ERROR_WANT_CONNECT:
            case SSL_ERROR_WANT_ACCEPT:
            case SSL_ERROR_WANT_X509_LOOKUP:
                return -EAGAIN;
            case SSL_ERROR_SYSCALL: {
                mError = GetAndClearErr();
                if (mError) {
                    return -EINVAL;
                }
                const int theErr = errno;
                if (theErr > 0) {
                    return -theErr;
                }
                if (theErr == 0) {
                    return -EINVAL;
                }
                return theErr;
            }
            case SSL_ERROR_SSL:
                mError        = GetAndClearErr();
                mSslErrorFlag = true;
                return -EINVAL;
            default:
                return -EINVAL;
        }
        return 0;
    }
    void StoreClientSession()
    {
        if (mServerFlag) {
            return;
        }
        QCASSERT(mSslPtr && sOpenSslInitPtr);
        mSessionStoredFlag = true;
        if (SSL_get_verify_result(mSslPtr) != X509_V_OK) {
            return;
        }
        SSL_SESSION* const theCurSessionPtr = SSL_get_session(mSslPtr);
        if (! theCurSessionPtr || ! theCurSessionPtr->peer) {
            // Do not store session with no peer certificate, i.e. PSK sessions.
            return;
        }
        SSL_CTX* const theCtxPtr = SSL_get_SSL_CTX(mSslPtr);
        SSL_SESSION* const theStoredSessionPtr = reinterpret_cast<SSL_SESSION*>(
            SSL_CTX_get_ex_data(theCtxPtr, sOpenSslInitPtr->mExDataSessionIdx));
        if (theStoredSessionPtr == theCurSessionPtr) {
            return;
        }
        QCStMutexLocker theLock(sOpenSslInitPtr->mSessionUpdateMutex);
        {
            SSL_SESSION* const thePtr = reinterpret_cast<SSL_SESSION*>(
                SSL_CTX_get_ex_data(
                    theCtxPtr,
                    sOpenSslInitPtr->mExDataSessionIdx)
            );
            if (thePtr == theCurSessionPtr) {
                return;
            }
            SSL_SESSION* const theCurPtr = SSL_get1_session(mSslPtr);
            if (SSL_CTX_set_ex_data(
                    theCtxPtr,
                    sOpenSslInitPtr->mExDataSessionIdx,
                    theCurPtr)) {
                if (thePtr) {
                    SSL_SESSION_free(thePtr);
                }
            } else {
                if (theCurPtr) {
                    SSL_SESSION_free(theCurPtr);
                }
                ClearError();
            }
        }
    }
    void SetStoredClientSession()
    {
        if (mServerFlag) {
            return;
        }
        QCASSERT(mSslPtr && sOpenSslInitPtr);
        QCStMutexLocker theLock(sOpenSslInitPtr->mSessionUpdateMutex);
        SSL_SESSION* const thePtr = reinterpret_cast<SSL_SESSION*>(
            SSL_CTX_get_ex_data(
                SSL_get_SSL_CTX(mSslPtr),
                sOpenSslInitPtr->mExDataSessionIdx)
        );
        if (thePtr) {
            SSL_set_session(mSslPtr, thePtr);
        }
    }
    void SetPskCB()
    {
        if (! mSslPtr) {
            return;
        }
        if (! mPskData.empty() || mServerPskPtr) {
#if OPENSSL_VERSION_NUMBER < 0x10000000L || defined(OPENSSL_NO_PSK)
            mError = SSL_R_UNSUPPORTED_CIPHER;
#else
            SSL_set_psk_server_callback(mSslPtr, &PskServerCB);
#endif
        }
        if (! mPskData.empty() && ! mServerPskPtr) {
#if OPENSSL_VERSION_NUMBER < 0x10000000L || defined(OPENSSL_NO_PSK)
            mError = SSL_R_UNSUPPORTED_CIPHER;
#else
            SSL_set_psk_client_callback(mSslPtr, &PskClientCB);
#endif
        }
    }
    int ShutdownSelf(
        NetConnection& inConnection)
    {
        mShutdownInitiatedFlag = true;
        if (mShutdownCompleteFlag) {
            return 0;
        }
        if (mError) {
            return -EFAULT;
        }
        if (! SSL_is_init_finished(mSslPtr)) {
            // Always run full handshake.
            // Wait for handshake to complete, then issue shutdown.
            return 0;
        }
        int theRet = SSL_shutdown(mSslPtr);
        if (theRet == 0) {
            // Call shutdown again to initiate read state, if the shutdown call
            // above successfully dispatch the ssl shutdown alert.
            // The second call to ssl shutdown should not return 0.
            theRet = SSL_shutdown(mSslPtr);
        }
        if (theRet <= 0) {
            return SslRetToErr(theRet);
        }
        mShutdownCompleteFlag = true;
        inConnection.SetFilter(0, 0);
        if (mDeleteOnCloseFlag) {
            delete this;
        }
        return 0;
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
    const char*       inParamsPrefixPtr,
    const Properties& inParams,
    string*           inErrMsgPtr)
{
    return Impl::CreateCtx(
        inServerFlag, inPskOnlyFlag,inParamsPrefixPtr, inParams, inErrMsgPtr);
}

    /* static */ long
SslFilter::GetSessionTimeout(
    SslFilter::Ctx& inCtx)
{
    return Impl::GetSessionTimeout(inCtx);
}

    /* static */ void
SslFilter::FreeCtx(
    SslFilter::Ctx* inCtxPtr)
{
    Impl::FreeCtx(inCtxPtr);
}

SslFilter::SslFilter(
    SslFilter::Ctx&        inCtx,
    const char*            inPskDataPtr,
    size_t                 inPskDataLen,
    const char*            inPskCliIdendityPtr,
    SslFilter::ServerPsk*  inServerPskPtr,
    SslFilter::VerifyPeer* inVerifyPeerPtr,
    bool                   inDeleteOnCloseFlag)
    : NetConnection::Filter(),
      mImpl(*(new Impl(
        inCtx,
        inPskDataPtr,
        inPskDataLen,
        inPskCliIdendityPtr,
        inServerPskPtr,
        inVerifyPeerPtr,
        inDeleteOnCloseFlag,
        mReadPendingFlag
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
    IOBuffer&      inIoBuffer,
    bool&          outForceInvokeErrHandlerFlag)
{
    return mImpl.Write(inConnection, inSocket, inIoBuffer,
        outForceInvokeErrHandlerFlag);
}

    /* virtual */ void
SslFilter::Close(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr)
{
    mImpl.Close(inConnection, inSocketPtr);
}

    /* virtual */ int
SslFilter::Shutdown(
    NetConnection& inConnection,
    TcpSocket&     inSocket)
{
    return mImpl.Shutdown(inConnection, inSocket);
}

    /* virtual */ int
SslFilter::Attach(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr,
    string*        outErrMsgPtr)
{
    return mImpl.Attach(inConnection, inSocketPtr, outErrMsgPtr);
}

    /* virtual */ void
SslFilter::Detach(
    NetConnection& inConnection,
    TcpSocket*     inSocketPtr)
{
    mImpl.Detach(inConnection, inSocketPtr);
}

    bool
SslFilter::IsHandshakeDone() const
{
    return mImpl.IsHandshakeDone();
}

    string
SslFilter::GetAuthName() const
{
    return mImpl.GetAuthName();
}
    bool
SslFilter::IsAuthFailure() const
{
    return mImpl.IsAuthFailure();
}
    string
SslFilter::GetErrorMsg() const
{
    return mImpl.GetErrorMsg();
}

    int
SslFilter::GetErrorCode() const
{
    return mImpl.GetErrorCode();
}

    bool
SslFilter::IsShutdownReceived() const
{
    return mImpl.IsShutdownReceived();
}

class SslFilterPeerVerify :
    private SslFilter::VerifyPeer,
    public  SslFilter
{
public:
    SslFilterPeerVerify(
        Ctx&        inCtx,
        const char* inPskDataPtr,
        size_t      inPskDataLen,
        const char* inPskCliIdendityPtr,
        ServerPsk*  inServerPskPtr,
        VerifyPeer* inVerifyPeerPtr,
        const char* inExpectedPeerNamePtr,
        bool        inDeleteOnCloseFlag)
        : VerifyPeer(),
          SslFilter(
            inCtx,
            inPskDataPtr,
            inPskDataLen,
            inPskCliIdendityPtr,
            inServerPskPtr,
            this,
            inDeleteOnCloseFlag),
            mVerifyPeerPtr(inVerifyPeerPtr),
            mExpectedPeerName(
                inExpectedPeerNamePtr ? inExpectedPeerNamePtr : "")
        {}
    virtual bool Verify(
	string&       ioFilterAuthName,
        bool          inPreverifyOkFlag,
        int           inCurCertDepth,
        const string& inPeerName)
    {
        if (mVerifyPeerPtr) {
            return mVerifyPeerPtr->Verify(
                ioFilterAuthName,
                inPreverifyOkFlag &&
                    (inCurCertDepth != 0 || inPeerName == mExpectedPeerName),
                inCurCertDepth,
                inPeerName
            );
        }
        if (! inPreverifyOkFlag ||
                (inCurCertDepth == 0 && inPeerName != mExpectedPeerName)) {
            KFS_LOG_STREAM_ERROR <<
                "peer verify failure:"
                " peer: "      << inPeerName <<
                " expected: "  << mExpectedPeerName <<
                " prev name: " << ioFilterAuthName <<
                " preverify: " << inPreverifyOkFlag <<
                " depth: "     << inCurCertDepth <<
            KFS_LOG_EOM;
            ioFilterAuthName.clear();
            return false;
        }
        KFS_LOG_STREAM_DEBUG <<
            "peer verify ok:"
            " peer: "      << inPeerName <<
            " expected: "  << mExpectedPeerName <<
            " prev name: " << ioFilterAuthName <<
            " preverify: " << inPreverifyOkFlag <<
            " depth: "     << inCurCertDepth <<
        KFS_LOG_EOM;
        if (inCurCertDepth == 0) {
            ioFilterAuthName = inPeerName;
        }
        return true;
    }
private:
    VerifyPeer* const mVerifyPeerPtr;
    string const      mExpectedPeerName;
};

    SslFilter&
SslFilter::Create(
    SslFilter::Ctx&        inCtx,
    const char*            inPskDataPtr          /* = 0 */,
    size_t                 inPskDataLen          /* = 0 */,
    const char*            inPskCliIdendityPtr   /* = 0 */,
    SslFilter::ServerPsk*  inServerPskPtr        /* = 0 */,
    SslFilter::VerifyPeer* inVerifyPeerPtr       /* = 0 */,
    const char*            inExpectedPeerNamePtr /* = 0 */,
    bool                   inDeleteOnCloseFlag   /* = true */)
{
    if (inExpectedPeerNamePtr) {
        return *(new SslFilterPeerVerify(
            inCtx,
            inPskDataPtr,
            inPskDataLen,
            inPskCliIdendityPtr,
            inServerPskPtr,
            inVerifyPeerPtr,
            inExpectedPeerNamePtr,
            inDeleteOnCloseFlag
        ));
    }
    return *(new SslFilter(
        inCtx,
        inPskDataPtr,
        inPskDataLen,
        inPskCliIdendityPtr,
        inServerPskPtr,
        inVerifyPeerPtr,
        inDeleteOnCloseFlag
    ));
}

}
