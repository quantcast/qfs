//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/08/09
// Author: Mike Ovsiannikov
//
// Copyright 2014 Quantcast Corp.
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

#include "ECMethod.h"

#include "qcrs/rs.h"

#include "common/kfstypes.h"

#include "qcdio/QCUtils.h"

namespace KFS
{
namespace client
{

class QCECMethod : public ECMethod
{
public:
    static ECMethod* GetMethod()
    {
        static QCECMethod sMethod;
        return &sMethod;
    }
protected:
    QCECMethod()
        : ECMethod()
        {}
    virtual ~QCECMethod()
    {
        QCECMethod::Unregister(KFS_STRIPED_FILE_TYPE_RS);
    }
    virtual bool Init(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS);
        return (inMethodType == KFS_STRIPED_FILE_TYPE_RS);
    }
    void Release(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS);
    }
    virtual Encoder* GetEncoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS);
        if (! Validate(inMethodType, inStripeCount, inRecoveryStripeCount,
                outErrMsgPtr)) {
            return 0;
        }
        return &mEncoder;
    }
    virtual Decoder* GetDecoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS);
        if (! Validate(inMethodType, inStripeCount, inRecoveryStripeCount,
                outErrMsgPtr)) {
            return 0;
        }
        return &mDecoder;
    };
    virtual bool Validate(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        if (inMethodType != KFS_STRIPED_FILE_TYPE_RS) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "QCRS: invalid method type";
            }
            return false;
        }
        if (inStripeCount <= 0 || RS_LIB_MAX_DATA_BLOCKS < inStripeCount) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "QCRS: invalid data stripe count";
            }
            return false;
        }
        if (inRecoveryStripeCount != RS_LIB_MAX_RECOVERY_BLOCKS) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "QCRS: invalid recovery stripe count";
            }
            return false;
        }
        return true;
    }
private:
    class QCRSEncoder : public ECMethod::Encoder
    {
    public:
        QCRSEncoder()
            : ECMethod::Encoder()
            {}
        virtual ~QCRSEncoder()
            {}
        virtual int Encode(
            int    inStripeCount,
            int    inRecoveryStripeCount,
            int    inLength,
            void** inBuffersPtr)
        {
            rs_encode(inStripeCount + inRecoveryStripeCount,
                inLength, inBuffersPtr);
            return 0;
        }
        virtual void Release()
            {}
    };
    class QCRSDecoder : public ECMethod::Decoder
    {
    public:
        QCRSDecoder()
            : ECMethod::Decoder()
            {}
        virtual ~QCRSDecoder()
            {}
        virtual int Decode(
            int        inStripeCount,
            int        inRecoveryStripeCount,
            int        inLength,
            void**     inBuffersPtr,
            int const* inMissingStripesIdx)
        {
            rs_decode3(
                inStripeCount + inRecoveryStripeCount,
                inLength,
                inMissingStripesIdx[0],
                inMissingStripesIdx[1],
                inMissingStripesIdx[2],
                inBuffersPtr
            );
            return 0;
        }
        virtual void Release()
            {}
    };
    QCRSEncoder mEncoder;
    QCRSDecoder mDecoder;
};

KFS_REGISTER_EC_METHOD(STRIPED_FILE_TYPE_RS, QCECMethod::GetMethod());

}} /* namespace client KFS */
