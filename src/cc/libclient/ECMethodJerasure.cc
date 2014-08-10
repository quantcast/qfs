//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2014/08/10
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

#include "common/kfstypes.h"

#include "qcdio/QCUtils.h"

namespace KFS
{
namespace client
{

class QCECMethodJerasure : public ECMethod
{
public:
    static ECMethod* GetMethod()
    {
        static QCECMethodJerasure sMethod;
        return &sMethod;
    }
protected:
    QCECMethodJerasure()
        : ECMethod()
        {}
    virtual ~QCECMethodJerasure()
    {
        QCECMethodJerasure::Unregister(KFS_STRIPED_FILE_TYPE_RS_JERASURE);
    }
    virtual bool Init(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
        return (inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
    }
    void Release(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
    }
    virtual Encoder* GetEncoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
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
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
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
        if (inMethodType != KFS_STRIPED_FILE_TYPE_RS_JERASURE) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "Jerasure: invalid method type";
            }
            return false;
        }
        if (inStripeCount <= 0 || inRecoveryStripeCount <= 0) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "Jerasure: invalid data stripe count";
            }
            return false;
        }
        return true;
    }
private:
    class JEncoder : public ECMethod::Encoder
    {
    public:
        JEncoder()
            : ECMethod::Encoder()
            {}
        virtual ~JEncoder()
            {}
        virtual int Encode(
            int    inStripeCount,
            int    inRecoveryStripeCount,
            int    inLength,
            void** inBuffersPtr)
        {
            return 0;
        }
        virtual void Release()
            {}
    };
    class JDecoder : public ECMethod::Decoder
    {
    public:
        JDecoder()
            : ECMethod::Decoder()
            {}
        virtual ~JDecoder()
            {}
        virtual int Decode(
            int        inStripeCount,
            int        inRecoveryStripeCount,
            int        inLength,
            void**     inBuffersPtr,
            int const* inMissingStripesIdx)
        {
            return 0;
        }
        virtual void Release()
            {}
    };
    JEncoder mEncoder;
    JDecoder mDecoder;
};

KFS_REGISTER_EC_METHOD(STRIPED_FILE_TYPE_RS_JERASURE,
    0 // FIXME: QCECMethodJerasure::GetMethod()
);

}} /* namespace client KFS */
