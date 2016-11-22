//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2015/09/11
// Author: Mike Ovsiannikov
//
// Copyright 2015,2016 Quantcast Corporation. All rights reserved.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unss required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//----------------------------------------------------------------------------

#include "IOMethodDef.h"

namespace KFS
{

class IOMethodList
{
public:
    typedef IOMethod* (*Registry)(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters);
    class Entry
    {
    public:
        Entry(
            Registry inMethodPtr)
            : mMethodPtr(inMethodPtr),
              mNextPtr(0)
            { IOMethodList::PushBack(*this); }
    private:
        Registry const mMethodPtr;
        Entry*         mNextPtr;
        friend class IOMethodList;
    };
    static IOMethod* Create(
        const char*       inUrlPtr,
        const char*       inLogPrefixPtr,
        const char*       inParamsPrefixPtr,
        const Properties& inParameters)
    {
        const Entry* theNextPtr = sHeadPtr;
        while (theNextPtr) {
            const Entry& theCur = *theNextPtr;
            theNextPtr = theCur.mNextPtr;
            IOMethod* theRetPtr = (*theCur.mMethodPtr)(
                inUrlPtr,
                inLogPrefixPtr,
                inParamsPrefixPtr,
                inParameters
            );
            if (theRetPtr) {
                return theRetPtr;
            }
        }
        return 0;
    }
private:
    static void PushBack(
        Entry& inEntry)
    {
        if (sTailPtr) {
            sTailPtr->mNextPtr = &inEntry;
        } else {
            sHeadPtr = &inEntry;
        }
        sTailPtr = &inEntry;
    }
    friend class Entry;
    static Entry* sHeadPtr;
    static Entry* sTailPtr;
};
IOMethodList::Entry* IOMethodList::sHeadPtr = 0;
IOMethodList::Entry* IOMethodList::sTailPtr = 0;

    /* static */ IOMethod*
IOMethod::Create(
    const char*       inUrlPtr,
    const char*       inLogPrefixPtr,
    const char*       inParamsPrefixPtr,
    const Properties& inParameters)
{
    return IOMethodList::Create(
        inUrlPtr,
        inLogPrefixPtr,
        inParamsPrefixPtr,
        inParameters
    ); 
}

#define __KFS_DECLARE_EXTERN_IO_METHOD(inType) \
    extern KFS_DECLARE_IO_METHOD(inType); \
    static IOMethodList::Entry sListEntry##inType(\
        &KFS_MAKE_REGISTERED_IO_METHOD_NAME(inType))

__KFS_DECLARE_EXTERN_IO_METHOD(KFS_IO_METHOD_NAME_S3ION);

#undef __KFS_DECLARE_EXTERN_IO_METHOD    

} // namespace KFS
