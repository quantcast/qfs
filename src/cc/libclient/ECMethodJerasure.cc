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

#include "ECMethodDef.h"

#ifndef QFS_OMIT_JERASURE
#include "jerasure.h"
#include "jerasure/reed_sol.h"

#include "common/kfstypes.h"
#include "common/kfsatomic.h"
#include "common/StdAllocator.h"
#include "common/IntToString.h"

#include "qcdio/QCUtils.h"
#include "qcdio/QCDLList.h"
#include "qcdio/qcdebug.h"

#include <map>
#include <sstream>

#include <stdlib.h>
#endif

namespace KFS
{
namespace client
{

#ifdef QFS_OMIT_JERASURE
KFS_REGISTER_EC_METHOD(STRIPED_FILE_TYPE_RS_JERASURE, 0);
#else

using std::map;
using std::less;
using std::pair;
using std::make_pair;

class QCECMethodJerasure : public ECMethod
{
public:
    static ECMethod* GetMethod()
    {
        static QCECMethodJerasure sMethod;
        return &sMethod;
    }
protected:
    virtual bool Init(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
        return (inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
    }
    virtual string GetDescription() const
        { return mDescription; }
    void Release(
        int inMethodType)
    {
        QCRTASSERT(inMethodType == KFS_STRIPED_FILE_TYPE_RS_JERASURE);
        Cleanup();
    }
    virtual Encoder* GetEncoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        return GetXCoder(
            inMethodType,
            inStripeCount,
            inRecoveryStripeCount,
            outErrMsgPtr
        );
    }
    virtual Decoder* GetDecoder(
        int     inMethodType,
        int     inStripeCount,
        int     inRecoveryStripeCount,
        string* outErrMsgPtr)
    {
        return GetXCoder(
            inMethodType,
            inStripeCount,
            inRecoveryStripeCount,
            outErrMsgPtr
        );
    }
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
    enum { kMaxCodersCacheCount = 2 << 10 };
    class JXCoder;
    typedef map<
        pair<int, int>,
        JXCoder*,
        less<pair<int, int> >,
        StdFastAllocator<
            pair<const pair<int, int>, JXCoder*> >
    > JXCoders;

    class JXCoder :
        public ECMethod::Encoder,
        public ECMethod::Decoder
    {
    public:
        typedef QCDLList<JXCoder, 0> List;

        JXCoder(
            int* inMatrixPtr,
            int  inW)
            : ECMethod::Encoder(),
              ECMethod::Decoder(),
              mMatrixPtr(inMatrixPtr),
              mW(inW),
              mRefCount(1),
              mIt()
            { List::Init(*this); }
        virtual bool SupportsOneRecoveryStripeRebuild() const
            { return true; }
        virtual int Decode(
            int        inStripeCount,
            int        inRecoveryStripeCount,
            int        inLength,
            void**     inBuffersPtr,
            int const* inMissingStripesIdxPtr)
        {
            return jerasure_matrix_decode(
                inStripeCount,
                inRecoveryStripeCount,
                mW,
                mMatrixPtr,
                1, // row_k_ones
                const_cast<int*>(inMissingStripesIdxPtr),
                reinterpret_cast<char**>(inBuffersPtr),
                reinterpret_cast<char**>(inBuffersPtr + inStripeCount),
                inLength
            );
        }
        virtual int Encode(
            int    inStripeCount,
            int    inRecoveryStripeCount,
            int    inLength,
            void** inBuffersPtr)
        {
            jerasure_matrix_encode(
                inStripeCount,
                inRecoveryStripeCount,
                mW,
                mMatrixPtr,
                reinterpret_cast<char**>(inBuffersPtr),
                reinterpret_cast<char**>(inBuffersPtr + inStripeCount),
                inLength
            );
            return 0;
        }
        virtual void Release()
        {
            const int theRef = SyncAddAndFetch(mRefCount, -1);
            if (0 < theRef) {
                return;
            }
            QCRTASSERT(theRef == 0);
            delete this;
        }
        JXCoder* Ref()
        {
            if (SyncAddAndFetch(mRefCount, 1) <= 1) {
                QCRTASSERT(! "invalid ref. count");
            }
            return this;
        }
        void SetIterator(
            const JXCoders::iterator& inIt)
            {  mIt = inIt; }
        JXCoders::iterator GetIterator() const
            {  return mIt; }
    protected:
        int* const mMatrixPtr;
        int  const mW;

        virtual ~JXCoder()
        {
            free(mMatrixPtr);
            mRefCount = -1000; // To catch double delete.
        }
    private:
        volatile int       mRefCount;
        JXCoders::iterator mIt;
        JXCoder*           mPrevPtr[1];
        JXCoder*           mNextPtr[1];

        friend class QCDLListOp<JXCoder, 0>;
    };
    class JXCoderRaid6 : public JXCoder
    {
    public:
        JXCoderRaid6(
            int* inMatrixPtr,
            int  inW)
            : JXCoder(inMatrixPtr, inW)
            {}
        virtual int Encode(
            int    inStripeCount,
            int    inRecoveryStripeCount,
            int    inLength,
            void** inBuffersPtr)
        {
            return (reed_sol_r6_encode(
                inStripeCount,
                mW,
                reinterpret_cast<char**>(inBuffersPtr),
                reinterpret_cast<char**>(inBuffersPtr + inStripeCount),
                inLength
            ) ? 0 : -1);
        }
    };

    typedef JXCoder::List LruList;
    const string mDescription;
    JXCoders     mJXCoders;
    JXCoder*     mLru[1];
    bool         mInitDoneFlag;

    JXCoder* GetXCoder(
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
        // Create global galios [read-only] structure for "w" 8, 16 and 32.
        // Jerasure library assigns pointers to these structures to the
        // corresponding "w" slot of the jerasure global gfp_array in galios.c
        // The access to ECMethod class methods is serialized  by the ECMethod
        // logic. Therefore the one time initialization of the gfp_array slot
        // from this method should work with multple threads, assuming that the
        // platform mutex performs "memory barrier / fence" or equivalent, if
        // necessary.
        // The memory allocated to hold these structures never released by
        // jerasure library, and might be reported as "memory leak" by memory
        // debuggers. The library api doesn't appear to offer any method to
        // free this memory.
        // The initialization is done here instead of Init method, is to defer
        // the initialization to the last moment. No initialization performed
        // if this method isn't used at all.
        if (! mInitDoneFlag) {
            for (int theW = 8; theW <= 32; theW *= 2) {
                const int theRet = galois_init_default_field(theW);
                if (theRet == 0) {
                    continue;
                }
                if (outErrMsgPtr) {
                    *outErrMsgPtr = "galios init default ";
                    AppendDecIntToString(*outErrMsgPtr, theW) += " error: ";
                    *outErrMsgPtr += QCUtils::SysError(theRet);
                }
                return 0;
            }
            mInitDoneFlag = true;
        }
        JXCoders::iterator const theIt = mJXCoders.find(make_pair(
            inStripeCount, inRecoveryStripeCount));
        if (theIt != mJXCoders.end()) {
            JXCoder& theXCoder = *theIt->second;
            LruList::PushBack(mLru, theXCoder);
            return theXCoder.Ref();
        }
        int theW = inStripeCount + inRecoveryStripeCount;
        if (theW <= (int32_t(1) << 8)) {
            theW = 8;
        } else if (theW <= (int32_t(1) << 16)) {
            theW = 16;
        } else {
            theW = 32;
        }
        int* const theMatrixPtr = inRecoveryStripeCount != 2 ?
            reed_sol_vandermonde_coding_matrix(
                inStripeCount, inRecoveryStripeCount, theW) :
            reed_sol_r6_coding_matrix(inStripeCount, theW);
        if (! theMatrixPtr) {
            if (outErrMsgPtr) {
                *outErrMsgPtr = "failed to create encoding matrix";
            }
            return 0;
        }
        JXCoder& theXCoder = *(inRecoveryStripeCount != 2 ?
            new JXCoder(theMatrixPtr, theW) :
            static_cast<JXCoder*>(new JXCoderRaid6(theMatrixPtr, theW))
        );
        JXCoder* theFrontPtr;
        while ((size_t)kMaxCodersCacheCount <= mJXCoders.size() &&
                (theFrontPtr = LruList::PopFront(mLru))) {
            mJXCoders.erase(theFrontPtr->GetIterator());
            theFrontPtr->Release();
        }
        theXCoder.SetIterator(
            mJXCoders.insert(make_pair(
                make_pair(inStripeCount, inRecoveryStripeCount),
                &theXCoder)).first
        );
        QCASSERT(theXCoder.GetIterator()->second == &theXCoder);
        LruList::PushBack(mLru, theXCoder);
        return theXCoder.Ref();
    }
protected:
    QCECMethodJerasure()
        : ECMethod(),
          mDescription(Describe()),
          mJXCoders(),
          mInitDoneFlag(false)
        { LruList::Init(mLru); }
    virtual ~QCECMethodJerasure()
    {
        QCECMethodJerasure::Unregister(KFS_STRIPED_FILE_TYPE_RS_JERASURE);
        Cleanup();
    }
    void Cleanup()
    {
        size_t   theCount = 0;
        JXCoder* thePtr;
        while ((thePtr = LruList::PopFront(mLru))) {
            thePtr->Release();
            theCount++;
        }
        QCRTASSERT(mJXCoders.size() == theCount);
        mJXCoders.clear();
    }
    static string Describe()
    {
        string theRet;
        theRet += "id: ";
        AppendDecIntToString(theRet, int(KFS_STRIPED_FILE_TYPE_RS_JERASURE)) +=
            "; jerasure"
            "; data stripes range: [1, ";
        AppendDecIntToString(theRet, KFS_MAX_DATA_STRIPE_COUNT) +=
            "]"
            "; recovery stripes range: [0, ";
        AppendDecIntToString(theRet, KFS_MAX_RECOVERY_STRIPE_COUNT) +=
            "]";
        return theRet;
    }
};

KFS_REGISTER_EC_METHOD(STRIPED_FILE_TYPE_RS_JERASURE,
    QCECMethodJerasure::GetMethod()
);

#endif /* QFS_OMIT_JERASURE */
}} /* namespace client KFS */
