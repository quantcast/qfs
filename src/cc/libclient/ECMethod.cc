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
//ECMethod.h
//----------------------------------------------------------------------------

#include "ECMethodDef.h"

#include "common/kfstypes.h"

#include "qcdio/QCMutex.h"
#include "qcdio/qcstutils.h"

namespace KFS
{
namespace client
{

    static QCMutex&
GetMutex()
{
    static QCMutex sMutex;
    return sMutex;
}

// Force construction prior entering main().
static const QCMutex* const sMutexPtr = &GetMutex();
static ECMethod*            sECMethods[KFS_STRIPED_FILE_TYPE_COUNT] = {0};

    /* static */ ECMethod*
ECMethod::Find(
    int     inMethodType,
    string* outErrMsgPtr)
{
    InitAllSelf();
    if (inMethodType == KFS_STRIPED_FILE_TYPE_UNKNOWN ||
            inMethodType < 0 ||
            KFS_STRIPED_FILE_TYPE_COUNT <= inMethodType) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "invalid erasure coding method type";
        }
        return 0;
    }
    if (! sECMethods[inMethodType]) {
        if (outErrMsgPtr) {
            *outErrMsgPtr = "erasure coding method type is not supported";
        }
    }
    return sECMethods[inMethodType];
}

template<typename T>
static inline T EnsureMsg(
    string*     inOutErrMsgPtr,
    const char* inMsgPtr,
    T           inRetVal)
{
    if (! inRetVal && inOutErrMsgPtr && inOutErrMsgPtr->empty()) {
        *inOutErrMsgPtr = (inMsgPtr && *inMsgPtr) ?
            inMsgPtr : "unspecified error";
    }
    return inRetVal;
}

    /* static */ int
ECMethod::InitAll()
{
    QCStMutexLocker theLocker(GetMutex());
    return InitAllSelf();
}

    /* static */ bool
ECMethod::IsValid(
    int     inMethodType,
    int     inStripeCount,
    int     inRecoveryStripeCount,
    string* outErrMsgPtr)
{
    QCStMutexLocker theLocker(GetMutex());
    ECMethod* const theMethodPtr = Find(inMethodType, outErrMsgPtr);
    if (! theMethodPtr) {
        return 0;
    }
    return EnsureMsg(outErrMsgPtr, "invalid erasure code method parameters",
        theMethodPtr->Validate(
            inMethodType, inStripeCount, inRecoveryStripeCount, outErrMsgPtr));
}

    /* static */ ECMethod::Encoder*
ECMethod::FindEncoder(
    int     inMethodType,
    int     inStripeCount,
    int     inRecoveryStripeCount,
    string* outErrMsgPtr)
{
    QCStMutexLocker theLocker(GetMutex());
    ECMethod* const theMethodPtr = Find(inMethodType, outErrMsgPtr);
    if (! theMethodPtr) {
        return 0;
    }
    return EnsureMsg(outErrMsgPtr, "invalid erasure encoder parameters",
        theMethodPtr->GetEncoder(
            inMethodType, inStripeCount, inRecoveryStripeCount, outErrMsgPtr));
}

    /* static */ ECMethod::Decoder*
ECMethod::FindDecoder(
    int     inMethodType,
    int     inStripeCount,
    int     inRecoveryStripeCount,
    string* outErrMsgPtr)
{
    QCStMutexLocker theLocker(GetMutex());
    ECMethod* const theMethodPtr = Find(inMethodType, outErrMsgPtr);
    if (! theMethodPtr) {
        return 0;
    }
    return EnsureMsg(outErrMsgPtr, "invalid erasure decoder parameters",
        theMethodPtr->GetDecoder(
            inMethodType, inStripeCount, inRecoveryStripeCount, outErrMsgPtr));
}

    /* static */ string
ECMethod::FindDescription(
    int     inMethodType,
    string* outErrMsgPtr)
{
    QCStMutexLocker theLocker(GetMutex());
    ECMethod* const theMethodPtr = Find(inMethodType, outErrMsgPtr);
    if (! theMethodPtr) {
        return string();
    }
    const string theDescription = theMethodPtr->GetDescription();
    return (theDescription.empty() ?
        string("no description provided") : theDescription);
}

ECMethod::ECMethod()
{
    GetMutex(); // Ensure mutex construction.
}

ECMethod::~ECMethod()
{
    QCStMutexLocker theLocker(GetMutex());
    for (int i = 0; i < KFS_STRIPED_FILE_TYPE_COUNT; i++) {
        if (sECMethods[i] == this) {
            sECMethods[i] = 0;
        }
    }
}

    bool
ECMethod::Register(
    int inMethodType)
{
    QCStMutexLocker theLocker(GetMutex());
    if (0 <= inMethodType && inMethodType < KFS_STRIPED_FILE_TYPE_COUNT &&
            inMethodType != KFS_STRIPED_FILE_TYPE_UNKNOWN) {
        if (sECMethods[inMethodType] == this) {
            return true;
        }
        if (Init(inMethodType)) {
            if (sECMethods[inMethodType]) {
                ECMethod& theMethod = *(sECMethods[inMethodType]);
                sECMethods[inMethodType] = 0;
                theMethod.Release(inMethodType);
            }
            if (! sECMethods[inMethodType]) {
                sECMethods[inMethodType] = this;
                return true;
            }
        }
    }
    return false;
}

    void
ECMethod::Unregister(
    int inMethodType)
{
    QCStMutexLocker theLocker(GetMutex());
    if (0 <= inMethodType && inMethodType < KFS_STRIPED_FILE_TYPE_COUNT &&
            sECMethods[inMethodType] == this) {
        sECMethods[inMethodType] = 0;
        Release(inMethodType);
    }
}

#define __KFS_DECLARE_EXTERN_EC_METHOD(inType) \
    extern KFS_DECLARE_EC_METHOD_PTR(inType);
KFS_FOR_EACH_EC_METHOD(__KFS_DECLARE_EXTERN_EC_METHOD)
#undef __KFS_DECLARE_EXTERN_EC_METHOD

    inline /* static */ int
ECMethod::RegisterAllMethods()
{
#define __KFS_REGISTER_EXTERN_EC_METHOD(inType) { \
        ECMethod* const thePtr = KFS_MAKE_REGISTERED_EC_METHOD_NAME(inType); \
        if (thePtr && thePtr->Register(KFS_##inType)) { \
            theRet++; \
        } \
    }

    int             theRet = 0;
    KFS_FOR_EACH_EC_METHOD(__KFS_REGISTER_EXTERN_EC_METHOD)
    return theRet;
#undef __KFS_REGISTER_EXTERN_EC_METHOD
}

    /* static */ int
ECMethod::InitAllSelf()
{
    static const int sMethodCount = RegisterAllMethods();
    return sMethodCount;
}

}} /* namespace client KFS */
